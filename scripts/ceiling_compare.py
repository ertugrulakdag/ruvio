#!/usr/bin/env python3
"""Ratio saturated single-core ceiling summaries."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

GRID = "pipeline=16,32,64,128;clients=50,100,200,400;requests=1000000;data=64"
COMMANDS = ("SET", "GET", "INCR")


class CompareError(Exception):
    pass


def compare(left: dict, right: dict) -> list[tuple[str, float]]:
    if left.get("grid") != GRID or right.get("grid") != GRID or left.get("grid") != right.get("grid"):
        raise CompareError("grid mismatch")
    ratios: list[tuple[str, float]] = []
    for name in COMMANDS:
        left_command = left["commands"][name]
        right_command = right["commands"][name]
        if left_command["status"] != "saturated" or right_command["status"] != "saturated":
            continue
        ratios.append((name, left_command["median_rps"] / right_command["median_rps"]))
    if not ratios:
        raise CompareError("no command is saturated on both sides")
    return ratios


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("left", type=Path)
    parser.add_argument("right", type=Path)
    args = parser.parse_args()
    left = json.loads(args.left.read_text(encoding="utf-8"))
    right = json.loads(args.right.read_text(encoding="utf-8"))
    try:
        ratios = compare(left, right)
    except CompareError as error:
        print(str(error), file=sys.stderr)
        return 1
    for name, ratio in ratios:
        print(f"{name} {ratio:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
