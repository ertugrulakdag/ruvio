# Manual Docker Resilience Tests

For the wider automated test surface, published measurements, and remaining
production evidence gaps, see [Testing and Evidence](TESTING.md).

`scripts/docker_resilience.py` injects bounded filesystem and process failures
into disposable containers built from the current checkout. It currently tests:

- `disk-full`: pre-fills a size-limited `/data` tmpfs, writes until the server
  rejects a mutation, releases the reserved space, restarts Ruvio, and verifies
  every acknowledged key.
- `permission-denied`: removes write permission from the live WAL shard
  directory, writes until WAL rotation fails, restores permission, restarts,
  and verifies every acknowledged key.
- `process-crash`: acknowledges a small `always`-durability workload, sends
  `SIGKILL` to the server process (not the container), restarts, and verifies
  every acknowledged key.
- `replica-promotion`: runs an authenticated replication link on a private
  Docker network, disconnects the replica while the primary continues to
  acknowledge writes, reconnects and verifies catch-up, then kills the primary,
  promotes the replica, and checks its data and ability to accept writes.
- `soak`: runs a configurable `SET`/`GET`/`INCR` workload and samples server
  persistence, WAL sequence, memory, and key counts.

Each scenario creates a uniquely named container with a private tmpfs, a
loopback-only published port, and a label identifying its owner. Cleanup
removes only the container ID created by that scenario. JSON reports are
written under the ignored `target/docker-resilience/` directory.

## Run

From the repository root, build a fresh image and run all scenarios:

```bash
docker build --tag ruvio:resilience-test .
python3 scripts/docker_resilience.py
```

Run only one case or raise the write safety limit:

```bash
python3 scripts/docker_resilience.py --scenario disk-full
python3 scripts/docker_resilience.py --scenario permission-denied --max-writes 30000
python3 scripts/docker_resilience.py --scenario process-crash
python3 scripts/docker_resilience.py --scenario replica-promotion
```

The runner requires a working Docker Engine and the Python standard library.
The Docker daemon must support `tmpfs` mounts. No host data directory, named
volume, or pre-existing container is modified.

## Observed local run

On 2026-10-03, Docker Engine 29.8.0 (`linux/aarch64`) passed all five cases
against image `ruvio:resilience-test` (`sha256:56dcb352...`): 9,216/9,216
acknowledged writes recovered after `ENOSPC`, 3/3 after permission-denied WAL
rotation, and 100/100 after process `SIGKILL`. A separate replication drill
disconnected the replica while the primary accepted 100 more writes; all 200
keys caught up after reconnect in 0.371 seconds, then survived manual
promotion. The 30-second mixed workload completed 48,413 iterations (145,239
commands). Mutations retried while each filesystem fault remained active
returned `MISCONF persistence error`. The full report is
`target/docker-resilience/20261003T155213Z-90102.json` (local, ignored build
output).

## Evidence boundary

These scenarios validate Ruvio's process-level recovery and filesystem error
handling inside a Linux container. Docker Desktop may run that container in a
Linux VM. A container `SIGKILL`, tmpfs `ENOSPC`, or permission failure does not
simulate host power loss, prove physical-device flush semantics, or replace
bare-metal filesystem and hardware testing. The `always` profile's behavior on
real storage remains a release-evidence gate.
