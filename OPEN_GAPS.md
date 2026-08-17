# Open gaps

Unfinished work and open problems, each stated as the gap stands now.

## A 64-entity slice does not fit in one datagram

`transport-fanout/src/fanout.h:39` caps a subscriber's slice at 64 entities and says why:
"Cap one subscriber's slice so a single write stays inside a datagram-sized batch. 64 entities
\* 100 bytes = 6400 bytes, comfortably inside one message."

Two implementations that share no code disagree with that comment and agree with each other.

| stack | largest datagram | whole records | how it fails above the limit |
| --- | --- | --- | --- |
| `aioquic` 1.3.0 | 1169 bytes | 11 | queues the frame and jams, see below |
| `pywebtransport` 0.20.1 | 1161 bytes | 11 | refuses at the API |

Measured 2026-08-17, macOS arm64, loopback, one fresh connection per size. The eight-byte
difference is header overhead, and both land on 11 records.

The cause is structural rather than a library limit. A DATAGRAM frame has to fit inside one QUIC
packet, and `aioquic` reports `_max_datagram_size = 1200` against a negotiated
`_remote_max_datagram_frame_size = 65536`, so the packet size binds and the frame limit never
does. 6400 bytes is five QUIC packets. `datasource-queen/src/wt.c:32` sets `WT_MTU_MAX` to 1300,
which is 13 records, so the C side's configuration contradicts the C side's comment before either
Python implementation is considered.

`ingest_python.receiver.batch` splits a slice at 11 records so this repository runs. What stays
open is whether `MAX_SLICE_ENTITIES` is wrong, whether slices belong on streams, or whether
`fanout_one`'s silent truncation at 64 was always the real cap. `transport-fanout` has never run
and `transport-ingest-c` has no `main`, so nothing had exercised the claim.

The number is a negotiated QUIC value, so another path may differ. No measurement on another path
exists.

## One oversized datagram jams every datagram after it

`aioquic`'s `_write_datagram_frame` asks the packet builder for room, and when the frame cannot
fit, the caller breaks out of the send loop **without** popping the queue. The oversized datagram
stays at the head forever.

Measured: after one 6400-byte send, three subsequent 100-byte datagrams never arrived and the
pending queue only grew, from one entry to three. One bad send does not lose one message, it ends
that session's datagram path.

`WardProtocol.send_datagram` refuses anything over the derived capacity rather than queueing it,
so this repository cannot trigger it. Fragmentation upstream of that guard means nothing should
ever reach it, and it logs an error rather than splitting, because arriving there is a bug in the
batching.

A patch is prepared against aioquic and is not submitted: it discards a datagram no packet can
hold instead of blocking the queue, and adds `max_datagram_frame_payload_size()` so an application
can fragment without reading private attributes. Until it lands, `datagram_capacity` derives the
number from private attributes, which is the fragile part of this repository.

This also contaminated an earlier measurement here. A binary search over delivery returned 1050
bytes because one oversized probe jammed the connection and every later size read as lost, which
is why the table above uses a fresh connection per size.

## Not built

The live cross-test against `transport-ingest-c` is not written, so the golden vectors are the
only gate. `transport-ingest-c` has no `main` and no `CMakeLists.txt`, its README says
"State: not started", and its copy of the transport code is a stale fork of
`transport-gateway-c`'s, so there is nothing to cross-test against yet.

Datagram ordering is not checked. H3 and WebTransport give unreliable-unsequenced only, and
nothing here yet catches an endpoint claiming unreliable-ordered and not delivering it.

## A dropped ring sample is undetectable

`iceoryx2` defaults a subscriber's buffer to 2 samples with safe overflow on, so the third of
three sends silently overwrites the first. Measured on 2026-08-17: three records published in a
burst arrived as two, and the missing one was the oldest.

`bus.py` now sets the buffer to 256 explicitly, and safe overflow stays on because RFD 0108
requires that a lagging reader never stall the writer. That makes an overwrite rare rather than
impossible, and it stays silent: `HeaderPublishSubscribe` carries a node id, a publisher id and
an element count, and no sequence number, so nothing here can tell a dropped sample from one that
was never sent.

RFD 0108 says a subscriber whose cursor falls out of the ring "receives a resync signal rather
than a gap". No such signal exists here, and adding one means a sequence number this repository
would have to define, which is a wire decision rather than a transport one.

## The duplicated gate

`conformance.py` is byte-identical in this repository and in `transport-gateway-python`, and
nothing checks that it stays so. That is the same failure `transport-ingest-c`'s stale copy of
`transport-gateway-c`'s transport code already produced once. The generated files under `gen/`
are safe, because both come from one `lake exe packet_emit` run and a drift shows up as a vector
mismatch. The gate itself has no such check.

## Closed

**`pywebtransport` rejected EC server keys.** It refused P-256 and P-384 with "failed to parse
private key as RSA, ECDSA, or EdDSA" and accepted only RSA-2048, while `contract-wt` records the
Godot demo server generating a fresh P-256 certificate every run. `aioquic` loads P-256, P-384
and RSA-2048 alike, and a live session with a P-256 server key answers CONNECT with `:status 200`.
RFD 0123 records the swap.
