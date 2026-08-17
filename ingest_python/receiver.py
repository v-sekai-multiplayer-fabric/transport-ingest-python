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
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import DatagramReceived, H3Event, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ProtocolNegotiated, QuicEvent

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "gen"))

import xr_grid_entity_packet as wire  # noqa: E402

from ingest_python.bus import Bus  # noqa: E402

# The path the Extended CONNECT names.
WT_PATH = "/ward"

# The largest datagram this stack delivers, measured rather than assumed: 1169 bytes on a
# loopback session, with 1170 the first size that fails. A DATAGRAM frame has to fit inside one
# QUIC packet, and aioquic's packet size is 1200, so the headroom is the packet header, the
# frame header, the session-id varint and the AEAD tag.
#
# transport-fanout caps a slice at 64 records and says 6400 bytes is "comfortably inside one
# message". It is five QUIC packets. The previous stack refused anything over 1161 bytes, so two
# implementations that share no code agree the answer is 11 records. See OPEN_GAPS.md.
DATAGRAM_MAX_BYTES = 1169
RECORDS_PER_DATAGRAM = DATAGRAM_MAX_BYTES // wire.SIZE

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


def batch(records: list[bytes], *, per_datagram: int = RECORDS_PER_DATAGRAM) -> list[bytes]:
    """Pack records into datagram-sized writes, back to back and with no framing.

    The receiver recovers the count by division, so a batch carries whole records only. This is
    where a 64-record slice becomes six datagrams rather than one.
    """
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

    def send_datagram(self, session_id: int, payload: bytes) -> None:
        """Refuse an oversized datagram rather than queue it.

        This guard is load-bearing. aioquic's `_write_datagram_frame` asks the packet builder for
        room and, when the frame cannot fit, the caller breaks out of the send loop *without*
        popping the queue. The oversized datagram therefore stays at the head forever and every
        later datagram on that session is lost behind it -- measured: after one 6400-byte send,
        three subsequent 100-byte datagrams never arrived and the pending queue only grew.

        So one bad send does not lose one message, it ends the session's datagram path. Refusing
        here keeps that impossible.
        """
        assert self._http is not None
        if len(payload) > DATAGRAM_MAX_BYTES:
            self._counters.oversized += 1
            _log.error(
                "refused a %d-byte datagram, cap is %d; queueing it would jam every datagram after it",
                len(payload),
                DATAGRAM_MAX_BYTES,
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
            for one in batch(records):
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
