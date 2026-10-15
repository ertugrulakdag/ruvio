#!/usr/bin/env python3
"""Summarize repeatable redis-benchmark samples and compare a baseline."""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

METRIC_COLUMNS = {
    "rps": 1,
    "avg_ms": 2,
    "min_ms": 3,
    "p50_ms": 4,
    "p95_ms": 5,
    "p99_ms": 6,
    "max_ms": 7,
}

FINGERPRINT_KEYS = (
    "host_os",
    "host_arch",
    "host_cpu",
    "docker_server",
    "requests",
    "clients",
    "pipeline",
    "data_size",
    "tests",
    "cpu_limit",
    "memory_limit",
    "ruvio_backend",
    "ruvio_shards",
    "ruvio_allocator",
    "redis_image",
)


def load_environment(path: Path) -> dict[str, str]:
    environment: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        environment[key] = value
    return environment


def parse_benchmark_csv(path: Path) -> dict[str, dict[str, float]]:
    commands: dict[str, dict[str, float]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.reader(handle):
            if len(row) < 8:
                continue
            try:
                metrics = {
                    metric: float(row[column])
                    for metric, column in METRIC_COLUMNS.items()
                }
            except ValueError:
                continue
            if not all(math.isfinite(value) for value in metrics.values()):
                raise ValueError(f"{path}: non-finite benchmark value")
            commands[row[0].strip().upper()] = metrics
    if not commands:
        raise ValueError(f"{path}: no redis-benchmark result rows")
    return commands


def sample_paths(result_dir: Path, server: str) -> list[Path]:
    samples = sorted(result_dir.glob(f"{server}-[0-9][0-9].csv"))
    if samples:
        return samples
    legacy = result_dir / f"{server}.csv"
    return [legacy] if legacy.exists() else []


def metric_summary(values: Iterable[float]) -> dict[str, float]:
    samples = list(values)
    return {
        "median": statistics.median(samples),
        "min": min(samples),
        "max": max(samples),
    }


def summarize_server(paths: list[Path]) -> dict[str, object]:
    if not paths:
        raise ValueError("no benchmark samples")
    parsed = [parse_benchmark_csv(path) for path in paths]
    commands = set(parsed[0])
    for sample in parsed[1:]:
        if set(sample) != commands:
            raise ValueError("benchmark samples contain different command sets")

    return {
        "samples": len(parsed),
        "commands": {
            command: {
                metric: metric_summary(sample[command][metric] for sample in parsed)
                for metric in METRIC_COLUMNS
            }
            for command in sorted(commands)
        },
    }


def environment_mismatches(
    current: dict[str, str], baseline: dict[str, str]
) -> list[str]:
    return [
        key
        for key in FINGERPRINT_KEYS
        if current.get(key) != baseline.get(key)
    ]


def percent_change(current: float, baseline: float) -> float:
    if baseline == 0:
        raise ValueError("baseline metric is zero")
    return (current - baseline) / baseline * 100.0


def compare_summaries(
    current: dict[str, object], baseline: dict[str, object]
) -> dict[str, object]:
    mismatches = environment_mismatches(
        current["environment"], baseline["environment"]  # type: ignore[arg-type]
    )
    if mismatches:
        return {
            "compatible": False,
            "mismatched_fingerprint_keys": mismatches,
            "commands": {},
        }

    current_commands = current["servers"]["ruvio"]["commands"]  # type: ignore[index]
    baseline_commands = baseline["servers"]["ruvio"]["commands"]  # type: ignore[index]
    shared = sorted(set(current_commands) & set(baseline_commands))
    commands = {}
    for command in shared:
        current_rps = current_commands[command]["rps"]["median"]
        baseline_rps = baseline_commands[command]["rps"]["median"]
        current_p99 = current_commands[command]["p99_ms"]["median"]
        baseline_p99 = baseline_commands[command]["p99_ms"]["median"]
        commands[command] = {
            "rps_change_percent": percent_change(current_rps, baseline_rps),
            "p99_change_percent": percent_change(current_p99, baseline_p99),
        }
    return {"compatible": True, "mismatched_fingerprint_keys": [], "commands": commands}


def build_summary(result_dir: Path) -> dict[str, object]:
    environment = load_environment(result_dir / "environment.txt")
    servers = {
        server: summarize_server(sample_paths(result_dir, server))
        for server in ("redis", "ruvio")
    }
    ratios = {}
    redis_commands = servers["redis"]["commands"]  # type: ignore[index]
    ruvio_commands = servers["ruvio"]["commands"]  # type: ignore[index]
    for command in sorted(set(redis_commands) & set(ruvio_commands)):
        redis_rps = redis_commands[command]["rps"]["median"]
        ruvio_rps = ruvio_commands[command]["rps"]["median"]
        ratios[command] = ruvio_rps / redis_rps if redis_rps else None

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": environment,
        "servers": servers,
        "ruvio_to_redis_median_rps_ratio": ratios,
    }


def write_csv(path: Path, summary: dict[str, object]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "server",
                "command",
                "samples",
                "median_rps",
                "min_rps",
                "max_rps",
                "median_p50_ms",
                "median_p95_ms",
                "median_p99_ms",
            ]
        )
        for server, server_summary in summary["servers"].items():  # type: ignore[union-attr]
            for command, metrics in server_summary["commands"].items():
                writer.writerow(
                    [
                        server,
                        command,
                        server_summary["samples"],
                        metrics["rps"]["median"],
                        metrics["rps"]["min"],
                        metrics["rps"]["max"],
                        metrics["p50_ms"]["median"],
                        metrics["p95_ms"]["median"],
                        metrics["p99_ms"]["median"],
                    ]
                )


def print_summary(summary: dict[str, object]) -> None:
    for server, server_summary in summary["servers"].items():  # type: ignore[union-attr]
        print(f"{server}:")
        for command, metrics in server_summary["commands"].items():
            print(
                f"  {command}: median {metrics['rps']['median']:.0f} req/s, "
                f"p99 {metrics['p99_ms']['median']:.3f} ms "
                f"({server_summary['samples']} samples)"
            )


def threshold_failures(
    comparison: dict[str, object],
    max_rps_regression: float | None,
    max_p99_regression: float | None,
) -> list[str]:
    if not comparison["compatible"]:
        return []
    failures = []
    for command, metrics in comparison["commands"].items():  # type: ignore[union-attr]
        rps_regression = -metrics["rps_change_percent"]
        p99_regression = metrics["p99_change_percent"]
        if max_rps_regression is not None and rps_regression > max_rps_regression:
            failures.append(
                f"{command} RPS regressed {rps_regression:.2f}% "
                f"(limit {max_rps_regression:.2f}%)"
            )
        if max_p99_regression is not None and p99_regression > max_p99_regression:
            failures.append(
                f"{command} p99 regressed {p99_regression:.2f}% "
                f"(limit {max_p99_regression:.2f}%)"
            )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--write-baseline", type=Path)
    parser.add_argument("--max-rps-regression-percent", type=float)
    parser.add_argument("--max-p99-regression-percent", type=float)
    args = parser.parse_args()

    summary = build_summary(args.result_dir)
    comparison = None
    if args.baseline:
        baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
        comparison = compare_summaries(summary, baseline)
        summary["comparison"] = comparison

    summary_path = args.result_dir / "summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_csv(args.result_dir / "summary.csv", summary)
    if args.write_baseline:
        args.write_baseline.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(summary_path, args.write_baseline)

    print_summary(summary)
    if not comparison:
        return 0
    if not comparison["compatible"]:
        keys = ", ".join(comparison["mismatched_fingerprint_keys"])
        print(
            f"Baseline comparison skipped: environment differs in {keys}",
            file=sys.stderr,
        )
        return 0

    print("baseline comparison:")
    for command, metrics in comparison["commands"].items():
        print(
            f"  {command}: RPS {metrics['rps_change_percent']:+.2f}%, "
            f"p99 {metrics['p99_change_percent']:+.2f}%"
        )
    failures = threshold_failures(
        comparison,
        args.max_rps_regression_percent,
        args.max_p99_regression_percent,
    )
    for failure in failures:
        print(f"regression: {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
