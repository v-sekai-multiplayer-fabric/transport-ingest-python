# transport-ingest-python

A second implementation of the unreliable half of the fabric's WebTransport contract, on
`aioquic`. It exists to disagree with `transport-ingest-c` where one of them is wrong, so
anything that makes the two agree by construction destroys the only thing it produces.

`README.md` says what this is. Record decisions in the `multiplayer-fabric-manuals` repository.
`CITATION.cff` says what this repository is built on; add a reference there when you add a
dependency here.

## It is a second opinion, so it copies nothing

This is the rule the repository exists for, and it is the easy one to break under deadline.

- Do NOT copy a constant, a field offset, a packet size, or a framing rule out of
  `transport-ingest-c`, `transport-fanout`, `datasource-queen/src/wt.c`, or the engine's
  `modules/http3`. Two copies of one number agree until one is edited, and an agreement reached
  by copying tests that somebody can copy.
- Do NOT relax a check so a cross-test passes. A disagreement is the output. Record it in
  `OPEN_GAPS.md` and find out which side is wrong.
- Do NOT read the specification off the C implementation. Read RFC 9114, RFC 9297, and the Lean
  sources in `contract-wt`, `contract-entity-packet` and `contract-interest-mgmt`.
- `gen/` is emitted by `lake exe packet_emit` in `contract-entity-packet`. Regenerate those
  files, never edit them.

## A malformed datagram is dropped whole

A slice is back-to-back 100-byte records with no framing and the count is the length divided by
100. A length that is not a multiple of 100 is malformed rather than short.

Do NOT trim it to the nearest record. The trailing bytes are not a partial entity, they are
evidence that the sender and this reader disagree, and trimming hands the interactor an entity
assembled from another entity's bytes. Drop the datagram, count it, and say so.

## It is a transport layer

It holds no authority, runs no simulation, and keeps no durable state.

Do NOT add an interest filter, a priority accumulator, or a ghost expansion here. Deciding which
of a ward's bodies a subscriber hears about is `transport-fanout`'s problem and
`contract-interest-mgmt` proves it. This one moves bytes.

## Two modes, never three

H3 and WebTransport give reliable-sequenced on streams and unreliable-unsequenced on datagrams.
Unreliable-ordered is the endpoint's to build, so a datagram must never be assumed in order.

Do NOT add a sequence number to make datagrams ordered without recording the claim. If a test
needs ordering, it carries its own sequence and says so.

## It carries no player traffic

`transport-gateway-c` measured a scripting runtime at 5.70 M/s against a 15 M/s bar, and
117.8 ns per runtime crossing against a 66.7 ns per-packet budget. Those numbers are why policy
lives in an interactor rather than in the packet path, and they apply here in full.

Do NOT put this on a per-tick path, and do NOT quote a throughput number for it that
`data/measurements/` cannot produce.

## Build

```sh
pixi run check       # the conformance gate: 64 golden vectors, decode, re-encode, compare
pixi run selftest    # the same gate against corrupted input, which must fail
pixi run serve       # terminate WebTransport on localhost
```

The key may be EC or RSA. `aioquic` loads P-256, P-384 and RSA-2048 alike, which is why this
repository runs on it: `contract-wt` records the Godot demo server generating P-256, and the
stack this replaced accepted only RSA. `OPEN_GAPS.md` has the measurement.
