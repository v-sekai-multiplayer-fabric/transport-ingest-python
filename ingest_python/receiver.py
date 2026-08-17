"""Terminate the unreliable half of the fabric's WebTransport contract, on aioquic.

A QUIC datagram carries back-to-back 100-byte records with no framing, and the count is the
length divided by 100. The records go to the ring unchanged, and what the interactor publishes
back goes out as datagrams.

A length that is not a multiple of 100 is malformed rather than short. Trimming it would hand
the interactor entities built from another entity's bytes, so the whole datagram goes and the
drop is counted.

aioquic rather than picoquic is the whole point. Its QUIC and its TLS 1.3 are its own, written
in Python from the RFCs, so where this and `transport-ingest-c` disagree one of them is wrong
about the contract rather than both being wrong together.

    python -m ingest_python.receiver --cert cert.pem --key key.pem --port 4433

SPDX-License-Identifier: MIT
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from aioquic.asyncio import QuicConnectionProtocol, serve
from aioquic.buffer import size_uint_var
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import DatagramReceived, H3Event, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ProtocolNegotiated, QuicEvent

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "gen"))

import xr_grid_entity_packet as wire  # noqa: E402

from ingest_python.bus import Bus  # noqa: E402

# The path the Extended CONNECT names.
WT_PATH = "/ward"

# RFC 9221: "DATAGRAM frames cannot be fragmented; therefore, application protocols need to
# handle cases where the maximum datagram size is limited by other factors." Fragmenting is this
# repository's job, and the size it fragments against is a property of the live connection rather
# than a constant: the RFC notes the limit "can be further reduced by the max_udp_payload_size
# transport parameter and the Maximum Transmission Unit (MTU) of the path".
#
# So `datagram_capacity` derives it per batch. This floor is only what a caller gets before a
# connection exists, and it is the QUIC minimum of 1200 with the same overheads removed.
DATAGRAM_FLOOR_BYTES = 1169

# The AEAD tag on every QUIC packet, and the ONE_RTT short header, which is one byte of flags
# plus the destination connection id plus a two-byte packet number.
AEAD_TAG_BYTES = 16
SHORT_HEADER_FIXED_BYTES = 3

POLL_SECONDS = 0.001

_log = logging.getLogger("transport-ingest-python")


class Counters:
    """What a run saw. A dropped datagram that is not counted reads exactly like one that never
    arrived, which is the difference between a quiet transport and a broken one."""

    def __init__(self) -> None:
        self.datagrams = 0
        self.records = 0
        self.malformed = 0
        self.oversized = 0

    def __str__(self) -> str:
        return (
            f"datagrams={self.datagrams} records={self.records} "
            f"malformed={self.malformed} oversized={self.oversized}"
        )


def split(payload: bytes, counters: Counters) -> list[bytes]:
    """Return the whole records in one datagram, or nothing if its length is not a multiple."""
    counters.datagrams += 1
    if len(payload) == 0 or len(payload) % wire.SIZE != 0:
        counters.malformed += 1
        _log.warning("dropped a datagram of %d bytes, which is not a multiple of %d", len(payload), wire.SIZE)
        return []

    records = [payload[i : i + wire.SIZE] for i in range(0, len(payload), wire.SIZE)]
    counters.records += len(records)
    return records


def batch(records: list[bytes], *, capacity: int) -> list[bytes]:
    """Fragment records into datagram-sized writes, back to back and with no framing.

    This is the fragmentation RFC 9221 requires of the application, and the whole reason it can
    be done here rather than in QUIC is that a slice has no header. Every fragment is a run of
    whole records and decodes on its own by dividing its length, so a lost fragment costs the
    entities it carried and nothing else. There is no reassembly and no sequence to keep.

    A record longer than one datagram would be unfragmentable, and cannot arise: a record is
    `wire.SIZE` bytes and the capacity floor is more than eleven of them.
    """
    per_datagram = max(1, capacity // wire.SIZE)
    return [b"".join(records[i : i + per_datagram]) for i in range(0, len(records), per_datagram)]


class WardProtocol(QuicConnectionProtocol):
    """One QUIC connection, carrying one WebTransport session's datagrams."""

    def __init__(self, *args: object, bus: Bus, counters: Counters, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self._bus = bus
        self._counters = counters
        self._http: H3Connection | None = None
        self._session_id: int | None = None
        self._pump: asyncio.Task[None] | None = None

    def quic_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, ProtocolNegotiated):
            self._http = H3Connection(self._quic, enable_webtransport=True)
        if self._http is None:
            return
        for h3_event in self._http.handle_event(event):
            self._h3_event_received(h3_event)

    def _h3_event_received(self, event: H3Event) -> None:
        assert self._http is not None

        if isinstance(event, HeadersReceived):
            headers = dict(event.headers)
            if headers.get(b":method") == b"CONNECT" and headers.get(b":protocol") == b"webtransport":
                path = headers.get(b":path", b"").decode(errors="replace")
                if path != WT_PATH:
                    self._http.send_headers(stream_id=event.stream_id, headers=[(b":status", b"404")])
                    self.transmit()
                    return
                self._session_id = event.stream_id
                _log.info("session open path=%s id=%d", path, event.stream_id)
                self._http.send_headers(
                    stream_id=event.stream_id,
                    headers=[(b":status", b"200"), (b"sec-webtransport-http3-draft", b"draft02")],
                )
                self.transmit()
                self._pump = asyncio.ensure_future(self._egress(event.stream_id))

        elif isinstance(event, DatagramReceived):
            records = split(bytes(event.data), self._counters)
            if records:
                # One sample per datagram rather than one per record. A ring sample costs a loan
                # and a send whatever it carries, so publishing per record turned an 11-record
                # datagram into 11 sends and overran the subscriber's queue. The ring now carries
                # exactly what the wire carries: back-to-back records, count recovered by division.
                self._bus.publish(b"".join(records))

    def datagram_capacity(self, session_id: int) -> int:
        """The largest WebTransport datagram payload this connection carries right now.

        Derived rather than measured, and recomputed per batch, because both terms move: path
        MTU discovery changes the packet size, and a connection id rotation changes the header.

        A DATAGRAM frame has to fit in one QUIC packet, so the room is the packet minus the
        short header and the AEAD tag, minus the frame type byte, minus the frame's own length
        varint, minus the quarter-stream-id varint HTTP/3 puts in front of the payload.

        aioquic 1.3.0 exposes no public equivalent, so the fallback reads private attributes.
        RFC 9221 makes sizing the application's responsibility and gives it no way to learn the
        size, which quiche answers with `dgram_max_writable_len` and quic-go with the size on
        its too-large error. The first branch is for an aioquic that grows the same thing.
        """
        quic = self._quic
        public = getattr(quic, "max_datagram_frame_payload_size", None)
        if callable(public):
            # The QUIC layer reports its own frame payload; HTTP/3 spends a quarter-stream-id
            # varint of that on every WebTransport datagram.
            return max(0, public() - size_uint_var(session_id // 4))

        packet = getattr(quic, "_max_datagram_size", None)
        peer_cid = getattr(quic, "_peer_cid", None)
        if packet is None or peer_cid is None:
            return DATAGRAM_FLOOR_BYTES

        space = packet - (SHORT_HEADER_FIXED_BYTES + len(peer_cid.cid)) - AEAD_TAG_BYTES
        quarter = size_uint_var(session_id // 4)
        payload = space - 1 - quarter
        payload -= size_uint_var(payload + quarter)

        remote_frame_max = getattr(quic, "_remote_max_datagram_frame_size", None)
        if remote_frame_max is not None:
            payload = min(payload, remote_frame_max - 1 - quarter)

        return max(0, payload)

    def send_datagram(self, session_id: int, payload: bytes) -> None:
        """Refuse an oversized datagram rather than queue it.

        Fragmentation upstream of this should mean nothing ever trips the guard, which is why it
        logs an error rather than splitting: reaching here is a bug in the batching, and silently
        repairing it would hide that.

        The guard is load-bearing all the same. aioquic's `_write_datagram_frame` asks the packet
        builder for room and, when the frame cannot fit, the caller breaks out of the send loop
        *without* popping the queue, so the datagram stays at the head forever and every later
        datagram on that session is lost behind it. One bad send would not lose one message, it
        would end the session's datagram path.
        """
        assert self._http is not None
        capacity = self.datagram_capacity(session_id)
        if len(payload) > capacity:
            self._counters.oversized += 1
            _log.error(
                "refused a %d-byte datagram, capacity is %d; queueing it would jam the session",
                len(payload),
                capacity,
            )
            return
        self._http.send_datagram(stream_id=session_id, data=payload)
        self.transmit()

    async def _egress(self, session_id: int) -> None:
        """Carry what the interactor publishes back out as datagrams."""
        while not self._quic._close_event:  # noqa: SLF001
            outgoing = self._bus.receive()
            if outgoing is None:
                await asyncio.sleep(POLL_SECONDS)
                continue

            records = split(outgoing, Counters())
            for one in batch(records, capacity=self.datagram_capacity(session_id)):
                self.send_datagram(session_id, one)


def build_configuration(*, cert: str, key: str) -> QuicConfiguration:
    """aioquic loads an EC key as readily as an RSA one, which is why this repository runs on it.

    `OPEN_GAPS.md` records the measurement: P-256, P-384 and RSA-2048 all open a listener here,
    where the previous stack accepted only RSA while `contract-wt` records the Godot demo server
    generating P-256.
    """
    configuration = QuicConfiguration(is_client=False, alpn_protocols=H3_ALPN, max_datagram_frame_size=65536)
    configuration.load_cert_chain(cert, key)
    return configuration


async def run(*, cert: str, key: str, host: str, port: int, publish_to: str, subscribe_to: str) -> None:
    bus = Bus(publish_to=publish_to, subscribe_to=subscribe_to)
    counters = Counters()
    await serve(
        host,
        port,
        configuration=build_configuration(cert=cert, key=key),
        create_protocol=lambda *a, **kw: WardProtocol(*a, bus=bus, counters=counters, **kw),
    )
    _log.info("listening on %s:%d%s", host, port, WT_PATH)
    await asyncio.Future()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cert", required=True, help="PEM certificate")
    ap.add_argument("--key", required=True, help="PEM private key. EC or RSA; both work")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=4433)
    ap.add_argument("--publish-to", default="fabric/transport/ingest-python/input")
    ap.add_argument("--subscribe-to", default="fabric/transport/ingest-python/slice")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(
        run(
            cert=args.cert,
            key=args.key,
            host=args.host,
            port=args.port,
            publish_to=args.publish_to,
            subscribe_to=args.subscribe_to,
        )
    )


if __name__ == "__main__":
    main()
