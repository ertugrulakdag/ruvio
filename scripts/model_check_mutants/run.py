#!/usr/bin/env python3
"""Mutation test for scripts/model_check.py: how many deliberate bugs does it catch?

Each `*.patch` next to this script is a small, realistic bug in the server. For each one this
script resets a scratch worktree to the base commit, applies the patch, builds the release-fast
binary and runs the model checker with fixed seeds over the configs where the bug can show.
The first failure stops the run; the table records whether the checker caught the mutant, how
long it took and what it reported.

    python3 scripts/model_check_mutants/run.py --base HEAD --scratch /tmp/mutants \\
        --target-dir target/mutants --port-base 34000 --duration 120

`--only a,b` runs some mutants; `--seeds 1,2` runs each seed in turn until one catches the
mutant (each seed gets the whole `--duration`). `--base-check` first runs the unmodified base
over each selected mutant's configs with the same seeds, so a "caught" is not a false alarm of
the base. `--keep-bins` keeps the binaries for manual repros. The patches
are never applied to this checkout.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent

# name -> (configs to run, what the patch breaks)
MUTANTS = {
    "always-ack-before-fsync": (
        "always-1,always-2",
        "always: a batch under 4 KiB is acknowledged without its fsync (it rides on the next sync)",
    ),
    "sync-not-sticky": (
        "always-1,everysec-1",
        "a failed WAL fsync is treated like a failed write: the log recovers and accepts writes again",
    ),
    "expired-delete-unlogged": (
        "replica,always-1",
        "keys removed by active expiry get no delete record; replay keeps them and later writes land on them",
    ),
    "checkpoint-capture-list-push": (
        "snapshot-1,always-1",
        "a push onto an existing list skips admission, so a running checkpoint does not capture the key first",
    ),
    "replica-double-apply": (
        "replica,replica-always",
        "a reconnecting replica asks for its last frame again and applies it a second time in memory",
    ),
    "save-ok-on-failure": (
        "snapshot-1",
        "SAVE with one shard replies OK when writing its checkpoint failed",
    ),
    "multi-split-records": (
        "always-1,everysec-1",
        "EXEC logs one record per queued command instead of one transaction record",
    ),
    "rename-two-records": (
        "always-1,everysec-1",
        "RENAME of a string is logged as a delete of the source and a set of the destination",
    ),
    "incr-off-by-one": (
        "memory-1,everysec-1",
        "INCR from 9 returns and stores 11",
    ),
    "sigterm-skips-final-checkpoint": (
        "snapshot-1,snapshot-2",
        "shutdown skips the final checkpoint whenever the WAL flush succeeded, also without a WAL",
    ),
    "oom-applies-hset": (
        "maxmemory",
        "HSET stores the fields before admission, so an OOM refusal leaves them in memory (unlogged)",
    ),
    "hash-field-ttl-lost-on-restore": (
        "snapshot-1,always-1",
        "loading a checkpoint drops hash field deadlines: timed fields live forever after a restart",
    ),
}

FAIL_LINE = re.compile(r"^FAIL (\S+) seed (\d+): ([^:]+): (.*)$")


def run(cmd, **kw):
    return subprocess.run(cmd, check=True, **kw)


def prepare_worktree(worktree: Path, base: str):
    if not (worktree / ".git").exists():
        run(["git", "-C", str(REPO), "worktree", "add", "--detach", str(worktree), base],
            stdout=subprocess.DEVNULL)
    run(["git", "-C", str(worktree), "checkout", "--detach", "--force", base], stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL)
    run(["git", "-C", str(worktree), "reset", "--hard", "--quiet", base])
    run(["git", "-C", str(worktree), "clean", "-fdq", "--", "src"])


def build(worktree: Path, target: Path, log: Path) -> Path | None:
    env = dict(os.environ, CARGO_TARGET_DIR=str(target), CARGO_INCREMENTAL="0")
    # build.rs links hot symbols by path; a fresh mtime keeps a stale path out of the link line.
    (worktree / "build.rs").touch()
    with log.open("w") as out:
        result = subprocess.run(["cargo", "build", "--profile", "release-fast", "--bin", "ruvio"],
                                cwd=worktree, env=env, stdout=out, stderr=subprocess.STDOUT)
    shutil.rmtree(target / "release-fast" / "incremental", ignore_errors=True)
    binary = target / "release-fast" / "ruvio"
    return binary if result.returncode == 0 and binary.exists() else None


CHECKER = REPO / "scripts" / "model_check.py"


def check(binary: Path, configs: str, seed: int, duration: float, port_base: int, results: Path,
          work: Path, log: Path) -> dict:
    """Runs the checker until the first failure or the end of `duration`."""

    cmd = [sys.executable, str(CHECKER), "--seed", str(seed), "--duration",
           str(duration), "--config", configs, "--bin", str(binary), "--results", str(results),
           "--work-dir", str(work), "--port-base", str(port_base)]
    started = time.time()
    found = None

    with log.open("a") as out:
        out.write("$ " + " ".join(cmd) + "\n")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

        for line in proc.stdout:
            out.write(f"[{time.time() - started:7.1f}] {line}")
            match = FAIL_LINE.match(line.rstrip("\n"))

            if match and found is None:
                found = dict(config=match.group(1), seed=int(match.group(2)), kind=match.group(3),
                             message=match.group(4), seconds=round(time.time() - started, 1))
                # SIGINT lets the checker kill its servers in its `finally` blocks.
                proc.send_signal(signal.SIGINT)
            elif line.strip().startswith("repro:") and found is not None and "repro" not in found:
                found["repro"] = line.split("repro:", 1)[1].strip()
        try:
            proc.wait(timeout=120)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    # A server the checker could not stop.
    subprocess.run(["pkill", "-9", "-f", f"^{binary} --config {work}/"], stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)
    shutil.rmtree(work, ignore_errors=True)
    return found or {}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", default="HEAD", help="commit the mutants apply to")
    parser.add_argument("--scratch", required=True, help="scratch worktree, logs and checker results")
    parser.add_argument("--target-dir", default=str(REPO / "target" / "mutants"),
                        help="CARGO_TARGET_DIR for mutant builds (removed at the end)")
    parser.add_argument("--fresh-target", action="store_true", help="remove the target dir after every build")
    parser.add_argument("--port-base", type=int, default=34000)
    parser.add_argument("--duration", type=float, default=120.0, help="checker seconds per seed")
    parser.add_argument("--seeds", default="1", help="comma list; the next seed runs only if the mutant survived")
    parser.add_argument("--only", default=None, help="comma list of mutant names")
    parser.add_argument("--base-check", action="store_true", help="run the unmodified base the same way first")
    parser.add_argument("--base-bin", default=None, help="with --base-check: a binary of the base, not built again")
    parser.add_argument("--base-only", action="store_true", help="with --base-check: run only the control")
    parser.add_argument("--checker", default=None, help="another model_check.py (an older version, say)")
    parser.add_argument("--every-seed", action="store_true",
                        help="run every seed even after a catch; report caught/N and the median time")
    parser.add_argument("--prebuilt", default=None, help="a directory of ruvio-<mutant> binaries to use as they are")
    parser.add_argument("--keep-bins", action="store_true", help="keep each mutant binary under <scratch>/bin")
    args = parser.parse_args(argv)
    global CHECKER

    if args.checker:
        # A copy elsewhere still builds its fault shim from this checkout.
        CHECKER = Path(args.checker).resolve()

    scratch = Path(args.scratch).resolve()
    scratch.mkdir(parents=True, exist_ok=True)
    worktree = scratch / "worktree"
    target = Path(args.target_dir).resolve()
    seeds = [int(s) for s in args.seeds.split(",")]
    names = args.only.split(",") if args.only else list(MUTANTS)
    unknown = [n for n in names if n not in MUTANTS or not (HERE / f"{n}.patch").exists()]

    if unknown:
        parser.error(f"unknown mutants or missing patches: {unknown}")
    table = []
    results_file = scratch / "results.json"

    def run_checks(row, binary, configs, safe, log):
        elapsed = 0.0

        row["runs"] = []

        for seed in seeds:
            found = check(binary, configs, seed, args.duration, args.port_base, scratch / "results" / safe,
                          scratch / "work", log)
            row["seeds"].append(seed)
            row["runs"].append(dict(found) if found else dict(seed=seed, caught=False))

            if found and not row["caught"]:
                first = dict(found, seconds=round(elapsed + found["seconds"], 1))
                row.update(caught=True, **first)

                if not args.every_seed:
                    break
            elapsed += args.duration
        row.setdefault("seconds", elapsed)
        hits = [r["seconds"] for r in row["runs"] if "seconds" in r]
        row["caught_runs"] = f"{len(hits)}/{len(row['runs'])}"
        row["median_seconds"] = statistics.median(hits) if hits else None
        table.append(row)
        label = "failed" if row["mutant"].startswith("(base)") else "CAUGHT"
        print(f"   {label if row['caught'] else 'passed' if label == 'failed' else 'missed'} "
              f"({row['caught_runs']} seeds, median {row['median_seconds']} s)"
              + (f": {row['config']}: {row['kind']}: {row['message'][:160]}" if row["caught"] else ""),
              flush=True)
        results_file.write_text(json.dumps(table, indent=2))

    try:
        for name in (["(base)"] if args.base_check else []) + ([] if args.base_only else names):
            safe = name.strip("()")
            t = time.time()

            prebuilt = Path(args.prebuilt) / f"ruvio-{safe}" if args.prebuilt else None

            if name == "(base)" and args.base_bin:
                binary = Path(args.base_bin).resolve()
            elif prebuilt is not None and prebuilt.exists():
                binary = prebuilt.resolve()
            else:
                prepare_worktree(worktree, args.base)

                if name != "(base)":
                    run(["git", "-C", str(worktree), "apply", str(HERE / f"{name}.patch")])
                print(f"== {name}: build", flush=True)
                binary = build(worktree, target, scratch / f"{safe}.build.log")

            if binary is None:
                table.append(dict(mutant=name, configs=MUTANTS.get(name, ("", ""))[0], caught=None,
                                  message="build failed"))
                print(f"   build failed, see {scratch / f'{safe}.build.log'}", flush=True)
                continue
            if (name == "(base)" and args.base_bin) or (prebuilt is not None and binary == prebuilt.resolve()):
                kept = binary
            else:
                kept = scratch / "bin" / f"ruvio-{safe}"
                kept.parent.mkdir(exist_ok=True)
                shutil.copy(binary, kept)
            print(f"   built in {time.time() - t:.0f} s", flush=True)

            if name == "(base)":
                # The control: the unmodified base over each mutant's configs and seeds.
                for mutant in names:
                    configs = MUTANTS[mutant][0]
                    print(f"   base on {configs} (control for {mutant})", flush=True)
                    log = scratch / f"base-{mutant}.log"
                    log.unlink(missing_ok=True)
                    run_checks(dict(mutant=f"(base) for {mutant}", configs=configs, what="no patch",
                                    caught=False, seeds=[]), kept, configs, f"base-{mutant}", log)
            else:
                configs, what = MUTANTS[name]
                print(f"   checking {configs}", flush=True)
                log = scratch / f"{safe}.log"
                log.unlink(missing_ok=True)
                run_checks(dict(mutant=name, configs=configs, what=what, caught=False, seeds=[]), kept,
                           configs, safe, log)
            if not args.keep_bins and kept != binary:
                kept.unlink(missing_ok=True)
            if args.fresh_target:
                shutil.rmtree(target, ignore_errors=True)
    finally:
        results_file.write_text(json.dumps(table, indent=2))
        subprocess.run(["git", "-C", str(REPO), "worktree", "remove", "--force", str(worktree)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        shutil.rmtree(target, ignore_errors=True)

    mutants = [r for r in table if not r["mutant"].startswith("(base)") and r["caught"] is not None]
    caught = sum(1 for r in mutants if r["caught"])
    print("\n| mutant | configs | caught | seeds caught | median s | message |\n"
          "| --- | --- | --- | --- | --- | --- |")

    for r in table:
        message = f"{r.get('config', '')}: {r.get('kind', '')}: {r.get('message', '')}" if r.get("caught") \
            else r.get("message", "")
        print(f"| {r['mutant']} | {r['configs']} | {'yes' if r['caught'] else 'no'} | {r.get('caught_runs', '')} | "
              f"{r.get('median_seconds', '')} | {message[:200]} |")
    if mutants:
        print(f"\ndetection: {caught}/{len(mutants)} ({100 * caught / len(mutants):.0f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
