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

## 2026-08-17, later

### Changed

- The stack is `aioquic` 1.3.0 rather than `pywebtransport` 0.20.1. `aioquic` implements QUIC and
  TLS 1.3 in Python from the RFCs and shares no code with picoquic, so the second-opinion property
  RFD 0123 rests on is unchanged, and it loads EC keys.
- The server accepts an EC or an RSA key. A live session with a P-256 server key answers CONNECT
  with `:status 200`, which the previous stack could not do at all.

### Found

- One oversized datagram jams every datagram after it on `aioquic`. `_write_datagram_frame` breaks
  out of the send loop without popping the queue, so the frame stays at the head forever. Measured:
  after one 6400-byte send, three later 100-byte datagrams never arrived. The send path now refuses
  anything over the cap rather than queueing it.
- The datagram cap on `aioquic` is 1169 bytes, or 11 whole records, against 1161 and 11 on
  `pywebtransport`. Two stacks sharing no code agree that a 64-record slice cannot travel in one
  datagram, and that the binding constraint is one QUIC packet rather than the negotiated frame
  size.

### Fixed

- `pywebtransport` rejected EC P-256 and P-384 server keys while `contract-wt` records the Godot
  demo server generating P-256. `OPEN_GAPS.md` records it closed.

### Changed

- Fragmentation derives the datagram size from the live connection rather than holding the
  measured 1169. RFC 9221 makes fragmenting the application's job and notes the limit moves with
  `max_udp_payload_size` and path MTU, so a constant is wrong on any other path. The derivation
  reproduces the measurement exactly: 1169 derived, 1169 delivered, 1170 lost.
- A 64-record slice now fragments into six datagrams which reassemble byte-identically, verified
  end to end rather than by unit test alone.
