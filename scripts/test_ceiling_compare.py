#!/usr/bin/env python3
import json
import subprocess
import sys
import unittest
from pathlib import Path

import ceiling_compare


def command(status, rps, clients=200, pipeline=64):
    return {
        "status": status,
        "clients": clients,
        "pipeline": pipeline,
        "median_rps": rps,
        "median_p50_ms": 0.4,
        "median_p99_ms": 0.9,
        "median_server_core_percent": 95 if status == "saturated" else 40,
        "median_client_core_percent": 50,
        "samples": [],
    }


def summary(set_status="saturated", get_status="saturated", incr_status="saturated"):
    return {
        "schema_version": 1,
        "role": "single-core-ceiling",
        "status": "saturated",
        "grid": "pipeline=16,32,64,128;clients=50,100,200,400;requests=1000000;data=64",
        "commands": {
            "SET": command(set_status, 2_000_000),
            "GET": command(get_status, 1_000_000),
            "INCR": command(incr_status, 1_500_000),
        },
    }


class CeilingCompareTests(unittest.TestCase):
    def test_ratios_saturated_commands(self):
        left = summary()
        right = summary()
        right["commands"]["SET"]["median_rps"] = 1_000_000
        right["commands"]["GET"]["median_rps"] = 2_000_000
        right["commands"]["INCR"]["median_rps"] = 1_500_000
        self.assertEqual(
            ceiling_compare.compare(left, right),
            [("SET", 2.0), ("GET", 0.5), ("INCR", 1.0)],
        )

    def test_skips_a_command_that_is_client_bound_on_either_side(self):
        left = summary(get_status="client-bound")
        right = summary()
        self.assertEqual(
            [name for name, _ratio in ceiling_compare.compare(left, right)],
            ["SET", "INCR"],
        )

    def test_rejects_a_different_grid(self):
        left = summary()
        right = summary()
        right["grid"] = "other"
        with self.assertRaises(ceiling_compare.CompareError):
            ceiling_compare.compare(left, right)

    def test_rejects_when_no_command_is_saturated_on_both_sides(self):
        left = summary("client-bound", "client-bound", "client-bound")
        right = summary()
        with self.assertRaises(ceiling_compare.CompareError):
            ceiling_compare.compare(left, right)

    def test_cli_prints_ratios_and_exits_nonzero_on_a_bad_pair(self):
        root = Path(__file__).resolve().parents[1]
        script = root / "scripts" / "ceiling_compare.py"
        import tempfile

        with tempfile.TemporaryDirectory() as temporary:
            left_path = Path(temporary) / "left.json"
            right_path = Path(temporary) / "right.json"
            left_path.write_text(json.dumps(summary()), encoding="utf-8")
            bad = summary()
            bad["grid"] = "other"
            right_path.write_text(json.dumps(bad), encoding="utf-8")
            completed = subprocess.run(
                [sys.executable, str(script), str(left_path), str(right_path)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 1)
            self.assertIn("grid", completed.stderr)


if __name__ == "__main__":
    unittest.main()
