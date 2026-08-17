# Open gaps

Unfinished work and open problems, each stated as the gap stands now.

## Two disagreements with the C implementation, found and unresolved

This repository exists to produce these. Both are measured on this stack and neither is fixed.

### A 64-entity slice does not fit in one datagram

`transport-fanout/src/fanout.h:39` caps a subscriber's slice at 64 entities and says why:
"Cap one subscriber's slice so a single write stays inside a datagram-sized batch. 64 entities
\* 100 bytes = 6400 bytes, comfortably inside one message."

`pywebtransport` 0.20.1 refuses to send a datagram over **1161 bytes** on a loopback session,
which is **11** whole 100-byte records. 6400 bytes is 5.5 times that. `datasource-queen`'s own
`src/wt.c:32` sets `WT_MTU_MAX` to 1300, which is 13 records, so the C side's configuration
disagrees with the C side's comment before this implementation is considered at all.

Measured by binary search against a live session on 2026-08-17, macOS arm64, loopback. The
number is a negotiated QUIC limit rather than a `pywebtransport` constant, so it may differ on
another path, and no measurement on another path exists yet.

`ingest_python.receiver.batch` splits a slice across datagrams at 11 records, so this repository
runs. Whether that is the right answer is the open part: `MAX_SLICE_ENTITIES` may be wrong,
slices may be meant to travel on streams, or `fanout_one`'s silent truncation at 64 may always
have been the real cap. `transport-fanout` has never run, so nothing has exercised it.

### pywebtransport rejects EC server keys

`contract-wt/README.md` records that the Godot demo server "builds a fresh self-signed P-256
certificate on every run".

`pywebtransport` 0.20.1 refuses to open a listener with an EC private key, failing with "failed
to parse private key as RSA, ECDSA, or EdDSA". Measured on 2026-08-17 against PKCS#8 keys from
LibreSSL 3.3.6:

| key | result |
| --- | --- |
| EC prime256v1 (P-256) | rejected |
| EC secp384r1 (P-384) | rejected |
| RSA 2048 | accepted |
| Ed25519 | untested, LibreSSL 3.3.6 cannot generate one |

So this server needs an RSA key while the Godot demo server generates P-256. That does not stop
the two talking, because each end's key is its own, and it does stop this repository serving a
role the Godot side serves today. Which end is wrong is unresolved.

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
an element count, and no sequence number, so nothing here can tell a dropped sample from one
that was never sent.

RFD 0108 says a subscriber whose cursor falls out of the ring "receives a resync signal rather
than a gap". No such signal exists here, and adding one means a sequence number this repository
would have to define, which is a wire decision rather than a transport one.

## The duplicated gate

`conformance.py` is byte-identical in this repository and in `transport-gateway-python`, and
nothing checks that it stays so. That is the same failure `transport-ingest-c`'s stale copy of
`transport-gateway-c`'s transport code already produced once. The generated files under `gen/`
are safe, because both come from one `lake exe packet_emit` run and a drift shows up as a
vector mismatch. The gate itself has no such check.
