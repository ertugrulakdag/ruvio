#!/usr/bin/env python3

import json
import tempfile
import unittest
from pathlib import Path

import benchmark_regression as regression


CSV_HEADER = (
    '"test","rps","avg_latency_ms","min_latency_ms",'
    '"p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"\n'
)


def write_sample(path: Path, set_rps: float, set_p99: float) -> None:
    path.write_text(
        CSV_HEADER
        + f'"SET","{set_rps}","1.0","0.1","0.8","1.2","{set_p99}","2.0"\n'
        + '"GET","2000","0.8","0.1","0.7","1.0","1.1","1.5"\n',
        encoding="utf-8",
    )


def environment_text(**overrides: str) -> str:
    values = {key: f"value-{key}" for key in regression.FINGERPRINT_KEYS}
    values.update(overrides)
    return "".join(f"{key}={value}\n" for key, value in values.items())


class BenchmarkRegressionTests(unittest.TestCase):
    def test_builds_median_summary_from_repeated_samples(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result_dir = Path(temporary)
            (result_dir / "environment.txt").write_text(
                environment_text(), encoding="utf-8"
            )
            write_sample(result_dir / "redis-01.csv", 2_000, 1.0)
            write_sample(result_dir / "redis-02.csv", 2_200, 1.2)
            write_sample(result_dir / "redis-03.csv", 1_800, 0.8)
            write_sample(result_dir / "ruvio-01.csv", 1_000, 2.0)
            write_sample(result_dir / "ruvio-02.csv", 1_200, 2.2)
            write_sample(result_dir / "ruvio-03.csv", 800, 1.8)

            summary = regression.build_summary(result_dir)

            ruvio_set = summary["servers"]["ruvio"]["commands"]["SET"]
            self.assertEqual(ruvio_set["rps"]["median"], 1_000)
            self.assertEqual(ruvio_set["rps"]["min"], 800)
            self.assertEqual(ruvio_set["rps"]["max"], 1_200)
            self.assertEqual(ruvio_set["p99_ms"]["median"], 2.0)
            self.assertEqual(summary["ruvio_to_redis_median_rps_ratio"]["SET"], 0.5)

    def test_compares_only_matching_environment_fingerprints(self) -> None:
        current = {
            "environment": {
                key: f"value-{key}" for key in regression.FINGERPRINT_KEYS
            },
            "servers": {
                "ruvio": {
                    "commands": {
                        "SET": {
                            "rps": {"median": 900.0},
                            "p99_ms": {"median": 2.2},
                        }
                    }
                }
            },
        }
        baseline = json.loads(json.dumps(current))
        baseline["servers"]["ruvio"]["commands"]["SET"]["rps"]["median"] = 1_000.0
        baseline["servers"]["ruvio"]["commands"]["SET"]["p99_ms"]["median"] = 2.0

        comparison = regression.compare_summaries(current, baseline)
        self.assertTrue(comparison["compatible"])
        self.assertAlmostEqual(
            comparison["commands"]["SET"]["rps_change_percent"], -10.0
        )
        self.assertAlmostEqual(
            comparison["commands"]["SET"]["p99_change_percent"], 10.0
        )
        failures = regression.threshold_failures(comparison, 5.0, 5.0)
        self.assertEqual(len(failures), 2)

        current["environment"]["pipeline"] = "different"
        mismatch = regression.compare_summaries(current, baseline)
        self.assertFalse(mismatch["compatible"])
        self.assertEqual(mismatch["mismatched_fingerprint_keys"], ["pipeline"])
        self.assertEqual(
            regression.threshold_failures(mismatch, 0.0, 0.0),
            [],
        )


if __name__ == "__main__":
    unittest.main()
