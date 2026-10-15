#!/usr/bin/env python3
"""Compare iai-callgrind instruction counts with benchmark/baselines/instructions.json.

usage: check_instruction_baseline.py [--update] <bench-output.log>

Reads the `Instructions:` line of each `hot_path::...::name` block in the
`cargo bench --bench hot_path` output. Fails when a command exceeds its
baseline by more than THRESHOLD (2%) and more than SLACK instructions
(counts are small now, and a runner differs from another machine by tens of instructions).
"""
import json
import pathlib
import re
import sys

THRESHOLD = 0.02
SLACK = 32
BASELINE = pathlib.Path(__file__).resolve().parent.parent / "benchmark/baselines/instructions.json"


def parse(text):
    counts, name = {}, None
    # CI output is colorized; strip ANSI escapes so the line patterns match.
    text = re.sub(r"\x1b\[[0-9;]*m", "", text)
    for line in text.splitlines():
        header = re.match(r"^\S*hot_path::\S+?::(\w+)\s", line)
        if header:
            name = header.group(1)
        found = re.match(r"^\s+Instructions:\s+(\d+)", line)
        if found and name:
            counts[name] = int(found.group(1))
            name = None
    return counts


def main():
    args = [a for a in sys.argv[1:] if a != "--update"]
    counts = parse(pathlib.Path(args[0]).read_text())
    if not counts:
        sys.exit("no instruction counts found in bench output")
    if "--update" in sys.argv:
        BASELINE.write_text(json.dumps(dict(sorted(counts.items())), indent=2) + "\n")
        return
    base = json.loads(BASELINE.read_text())
    failed = False
    for name, expected in base.items():
        got = counts.get(name)
        if got is None:
            print(f"{name}: missing from bench output")
            failed = True
        elif got > expected * (1 + THRESHOLD) and got - expected > SLACK:
            print(f"{name}: {got} > {expected} (+{(got / expected - 1) * 100:.1f}%)")
            failed = True
        else:
            print(f"{name}: {got} (baseline {expected})")
    sys.exit(1 if failed else 0)


main()
