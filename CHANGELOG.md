# Changelog

Decisions and completed, verified work.

## Conventions

- The wire is never retyped here. `gen/` comes from `lake exe packet_emit` in
  `contract-entity-packet`; regenerate those files rather than editing them.
- A disagreement with `transport-ingest-c` is the output, so it is recorded in `OPEN_GAPS.md`
  rather than smoothed over.
- Every gate has been seen to fail on purpose. `pixi run selftest` is that demonstration.

## 2026-08-17

### Added

- The repository, per RFD 0123: a second implementation of the unreliable half of the fabric's
  WebTransport contract, on `pywebtransport` 0.20.1 and `iceoryx2` 0.9.3.
- `conformance.py`, which holds the emitted Python codec to all 64 golden vectors from
  `contract-entity-packet` and carries a `--self-test` negative control.
- `ingest_python/bus.py`, the one place this repository touches iceoryx2, and
  `ingest_python/receiver.py`, which terminates a session and carries datagrams of back-to-back
  100-byte records, dropping a datagram whose length is not a multiple of the record size.
- A Python emitter in `contract-entity-packet`, so the codec comes out of `Codec.lean` with the
  C header rather than being written by hand.

### Found

- A 64-entity slice is 6400 bytes and does not fit in one datagram. The measured cap is 1161
  bytes, or 11 records. `OPEN_GAPS.md` has the method and the numbers.
- `pywebtransport` 0.20.1 rejects EC P-256 and P-384 server keys and accepts RSA, while
  `contract-wt` records the Godot demo server generating P-256.

### Fixed

- A burst of ring publishes lost its oldest sample. `iceoryx2` defaults a subscriber's buffer to
  2, so three records published back to back arrived as two. The buffer is now set explicitly.
