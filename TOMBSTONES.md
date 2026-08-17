# Tombstones

Dead ends, disproven hypotheses, and blocklisted avenues.

## A single struct format string for the codec

`struct.Struct("<I3q3hIII3h42s")` encodes the whole 100-byte packet in one line and is the
obvious way to write this in Python. It states the layout a second time, as positions in a
format string that no longer name the offsets they stand for, so moving a field in
`Codec.lean` leaves the string wrong and passing.

The codec is emitted from the specification instead, a field at a time at a named offset. See
`EntityPacket/EmitPy.lean` in `contract-entity-packet`.

## Copying constants out of the C implementation

Blocklisted rather than merely avoided. A second implementation that reads its constants off
the first tests whether two people can copy a number. `CLAUDE.md` carries the rule.

## webtransportd

RFD 0047 proposed a standalone bridge piping a session's bytes to a child over stdin and
stdout, and abandoned it. `datasource-queen`'s `CLAUDE.md` still says "Do not propose
`webtransportd`. The Queen terminates QUIC in her own process." This repository terminates in
its own process for the same reason.
