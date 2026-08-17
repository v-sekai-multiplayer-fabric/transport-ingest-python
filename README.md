# transport-ingest-python

Input datagrams, terminated a second time, on a stack that shares nothing with the first.

A **transport layer** is the input that triggers an interactor. This one holds no authority,
runs no simulation, keeps no durable state, and carries no player traffic either. What it
produces is agreement or disagreement with `transport-ingest-c`.

## Why the fabric wants two

`transport-gateway-c`, `transport-ingest-c` and the engine's `modules/http3` all vendor
picoquic and picotls, so both ends of every session run the same code. RFD 0088 chose that, and
it buys interoperability by construction while giving up the check. A wire implemented once
describes the program that implements it, and nothing establishes that the specification is
implementable from the specification. Khronos ratifies against two independent implementations
for this reason.

`aioquic` shares no line of code with picoquic: its QUIC and its TLS 1.3 are written in Python
from the RFCs. Where the two disagree, one of them is wrong about the contract.

## What it terminates

The unreliable half of RFD 0049. A QUIC datagram carries back-to-back 100-byte records with no
framing, and the count is the length divided by 100.

A length that is not a multiple of 100 is malformed rather than short. Trimming it would hand
the interactor entities built from another entity's bytes, so the whole datagram goes.

## The wire is not ours to define

`gen/` is emitted by `lake exe packet_emit` in `contract-entity-packet` and vendored here, the
way `transport-asset` vendors its rebac tables. Regenerate those files, never edit them, and
carry no field offset or packet size anywhere else.

## State

The conformance gate runs: 64 golden vectors decode, re-encode and compare byte for byte, with
`pixi run selftest` showing the gate fail on purpose. The live cross-test against
`transport-ingest-c` is not written.
