#!/usr/bin/env python3
"""Hold this repository's packet codec to the golden vectors the specification emitted.

`gen/xr_grid_entity_packet.py` and `gen/packet_golden.csv` both come out of one
`lake exe packet_emit` run in `contract-entity-packet`. The codec is a claim about provenance;
only the vectors make it a claim about bytes, so this reads all 64 of them, decodes each,
checks the fields the CSV names, and re-encodes to compare byte for byte.

    python3 conformance.py                 # the gate
    python3 conformance.py --self-test     # the negative control, which must fail

The negative control is not decoration. A gate that has never been seen to fail certifies
nothing, so `--self-test` flips one byte of one vector and the run is wrong if that passes.

SPDX-License-Identifier: MIT
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

GEN = Path(__file__).parent / "gen"
sys.path.insert(0, str(GEN))

import xr_grid_entity_packet as wire  # noqa: E402

VECTORS = GEN / "packet_golden.csv"

# The CSV names a subset of the fields, one column each. Position is exact int64 micrometres and
# velocity is an i16 against V_MAX, so every one of these is compared as an integer rather than
# through a tolerance: a wire with no floats in it has no rounding to allow for.
COLUMNS = (
    ("gid", lambda p: p.gid),
    ("pumx", lambda p: p.pos_um_x),
    ("pumy", lambda p: p.pos_um_y),
    ("pumz", lambda p: p.pos_um_z),
    ("velx", lambda p: p.vel_x),
    ("vely", lambda p: p.vel_y),
    ("velz", lambda p: p.vel_z),
    ("pay0", lambda p: p.payload[0]),
    ("pay41", lambda p: p.payload[41]),
)


def check(corrupt: bool = False) -> tuple[int, int, list[str]]:
    """Return how many vectors were read, how many passed, and a line for each fault."""
    total = 0
    passed = 0
    faults: list[str] = []

    with VECTORS.open() as handle:
        for n, row in enumerate(csv.DictReader(handle)):
            total += 1
            raw = bytes.fromhex(row["hex"])
            if corrupt and n == 0:
                raw = bytes([raw[0] ^ 0xFF]) + raw[1:]

            here: list[str] = []
            packet = wire.decode(raw)
            for name, read in COLUMNS:
                if read(packet) != int(row[name]):
                    here.append(f"vector {n}: {name} decoded {read(packet)}, vectors say {row[name]}")

            again = wire.encode(packet)
            if again != raw:
                where = next(i for i, (a, b) in enumerate(zip(again, raw)) if a != b)
                here.append(f"vector {n}: re-encode differs from the vector at byte {where}")

            if here:
                faults.extend(here)
            else:
                passed += 1

    return total, passed, faults


def main(argv: list[str]) -> int:
    corrupt = "--self-test" in argv[1:]
    if not VECTORS.exists():
        print(f"missing {VECTORS}: regenerate it with `lake exe packet_emit` in contract-entity-packet")
        return 2

    total, passed, faults = check(corrupt=corrupt)

    if corrupt:
        if faults:
            print(f"self-test: the gate rejected a corrupted vector, as it must ({len(faults)} fault(s))")
            return 0
        print("self-test: the gate ACCEPTED a corrupted vector, so it certifies nothing")
        return 1

    for fault in faults:
        print(fault)
    print(f"{passed}/{total} golden vectors decode, re-encode and compare byte for byte")
    return 1 if faults else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
