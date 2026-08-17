"""Terminate the unreliable half of the fabric's WebTransport contract, on pywebtransport.

A QUIC datagram carries back-to-back 100-byte records with no framing, and the count is the
length divided by 100. The records go to the ring unchanged, and what the interactor publishes
back goes out as datagrams.

A length that is not a multiple of 100 is malformed rather than short. Trimming it would hand
the interactor entities built from another entity's bytes, so the whole datagram goes and the
drop is counted. This is the one place where being lenient loses information silently.

    python -m ingest_python.receiver --cert cert.pem --key key.pem --port 4433

SPDX-License-Identifier: MIT
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from pywebtransport import ServerApp, ServerConfig, WebTransportSession
from pywebtransport.events import EventType

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "gen"))

import xr_grid_entity_packet as wire  # noqa: E402

from ingest_python.bus import Bus  # noqa: E402

# The path the Extended CONNECT names.
WT_PATH = "/ward"

# The largest datagram this stack sends, measured rather than assumed: 1161 bytes on a loopback
# session, which is 11 whole records. transport-fanout caps a slice at 64 records and says 6400
# bytes is "comfortably inside one message", which this contradicts. See OPEN_GAPS.md.
DATAGRAM_MAX_BYTES = 1161
RECORDS_PER_DATAGRAM = DATAGRAM_MAX_BYTES // wire.SIZE

_log = logging.getLogger("transport-ingest-python")


class Counters:
    """What a run saw. A dropped datagram that is not counted reads exactly like one that never
    arrived, which is the difference between a quiet transport and a broken one."""

    def __init__(self) -> None:
        self.datagrams = 0
        self.records = 0
        self.malformed = 0

    def __str__(self) -> str:
        return f"datagrams={self.datagrams} records={self.records} malformed={self.malformed}"


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


async def serve_session(session: WebTransportSession, bus: Bus, counters: Counters) -> None:
    """Carry one session's datagrams until it closes."""
    _log.info("session open path=%s id=%s", session.path, session.session_id)

    async def on_datagram(event: object) -> None:
        payload = event.data["data"]  # type: ignore[attr-defined]
        records = split(bytes(payload), counters)
        if records:
            # One sample per datagram rather than one per record. A ring sample costs a loan and
            # a send whatever it carries, so publishing per record turned an 11-record datagram
            # into 11 sends and overran the subscriber's queue. The ring now carries exactly what
            # the wire carries: back-to-back records, count recovered by division.
            bus.publish(b"".join(records))

    session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=on_datagram)

    while not session.is_closed:
        outgoing = bus.receive()
        if outgoing is None:
            await asyncio.sleep(0.001)
            continue

        records = split(outgoing, Counters())
        for one in batch(records):
            await session.send_datagram(data=one)


def build(*, cert: str, key: str, host: str, port: int, publish_to: str, subscribe_to: str) -> ServerApp:
    app = ServerApp(config=ServerConfig(bind_host=host, bind_port=port, certfile=cert, keyfile=key))
    bus = Bus(publish_to=publish_to, subscribe_to=subscribe_to)
    counters = Counters()

    @app.route(path=WT_PATH)
    async def _handler(session: WebTransportSession) -> None:
        try:
            await serve_session(session, bus, counters)
        finally:
            _log.info("session closed, %s", counters)

    return app


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cert", required=True, help="PEM certificate. The key must be RSA; see OPEN_GAPS.md")
    ap.add_argument("--key", required=True, help="PEM private key, RSA")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=4433)
    ap.add_argument("--publish-to", default="fabric/transport/ingest-python/input")
    ap.add_argument("--subscribe-to", default="fabric/transport/ingest-python/slice")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    app = build(
        cert=args.cert,
        key=args.key,
        host=args.host,
        port=args.port,
        publish_to=args.publish_to,
        subscribe_to=args.subscribe_to,
    )
    app.run(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
