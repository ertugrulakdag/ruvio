#!/usr/bin/env python3
"""Run first-party clients and .NET integrations against an optional live Ruvio."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

import check_compatibility_matrix as matrix


ROOT = Path(__file__).resolve().parents[1]
# One suite command may run this long; a hung test then fails the run instead of blocking it.
SUITE_TIMEOUT_SECONDS = 15 * 60


def run(command: list[str], *, cwd: Path, env: dict[str, str]) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True, timeout=SUITE_TIMEOUT_SECONDS)


def have(executable: str) -> bool:
    return shutil.which(executable) is not None


def suite_env(host: str, port: int) -> dict[str, str]:
    env = os.environ.copy()
    env["RUVIO_HOST"] = host
    env["RUVIO_PORT"] = str(port)
    env["RUVIO_TEST_ADDR"] = f"{host}:{port}"
    return env


def run_rust(host: str, port: int) -> None:
    run(
        [
            "cargo",
            "test",
            "--manifest-path",
            str(ROOT / "clients" / "ruvio-client" / "Cargo.toml"),
        ],
        cwd=ROOT,
        env=suite_env(host, port),
    )


def run_csharp(host: str, port: int) -> None:
    env = suite_env(host, port)
    for project in (
        ROOT / "clients" / "csharp" / "Ruvio.Client.Tests" / "Ruvio.Client.Tests.csproj",
        ROOT
        / "clients"
        / "csharp"
        / "Ruvio.Client.AspNetCore.Tests"
        / "Ruvio.Client.AspNetCore.Tests.csproj",
        *sorted((ROOT / "integrations" / "dotnet").glob("*Tests/*Tests.csproj")),
    ):
        run(
            ["dotnet", "test", str(project), "--nologo", "--blame-hang-timeout", "5m"],
            cwd=ROOT,
            env=env,
        )


RUNNERS = {
    "ruvio-client": (["cargo"], run_rust),
    "ruvio-csharp": (["dotnet"], run_csharp),
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument(
        "--languages",
        default="ruvio-client,ruvio-csharp",
        help="comma-separated client ids from the compatibility matrix",
    )
    parser.add_argument(
        "--require-all",
        action="store_true",
        help="fail if a selected language toolchain is missing",
    )
    args = parser.parse_args()

    payload = matrix.load_matrix()
    known = {client["id"] for client in payload["clients"]}
    selected = [item.strip() for item in args.languages.split(",") if item.strip()]
    unknown = set(selected) - known
    if unknown:
        print(f"unknown client ids: {sorted(unknown)}", file=sys.stderr)
        return 2

    failed = []
    skipped = []
    for language in selected:
        tools, runner = RUNNERS[language]
        missing = [tool for tool in tools if not have(tool)]
        if missing:
            message = f"{language}: missing {', '.join(missing)}"
            if args.require_all:
                failed.append(message)
            else:
                skipped.append(message)
            continue
        try:
            runner(args.host, args.port)
        except subprocess.CalledProcessError as error:
            failed.append(f"{language}: exit {error.returncode}")
        except subprocess.TimeoutExpired as error:
            failed.append(f"{language}: {' '.join(error.cmd)} ran longer than {error.timeout} s")

    for item in skipped:
        print(f"skipped {item}")
    if failed:
        print("client suites failed:", file=sys.stderr)
        for item in failed:
            print(f"  - {item}", file=sys.stderr)
        return 1
    print(f"client suites passed: {', '.join(selected)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
