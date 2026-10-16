[Ruvio](../../README.md) / [Tests](../README.md) / Scenario lab

# Ruvio Scenario Lab

An isolated two-host integration/performance lab for exercising the .NET
Output Cache, HTTP Idempotency, and distributed Rate Limiting integrations
against a real Ruvio process. This is a test fixture, not a production sample.
It deliberately uses a synthetic caller identity and in-memory order side
effect counter; do not expose it as a customer API.

## Topology

| Host | Services |
| --- | --- |
| SUT (system under test) | Ruvio, the ASP.NET Core scenario API, and Node Exporter |
| Observer / load generator | k6, OpenTelemetry Collector, Prometheus, Grafana, and Tempo |

The API connects to Ruvio over a private Compose bridge. For the operator UI,
the authenticated Ruvio RESP port can be published on the SUT host; Grafana
can likewise be published on the observer. Prefer private interfaces for all
published listeners. If only public addresses are available, bind to the
peer-reachable IPv4 address, restrict UFW, and install the Docker
`DOCKER-USER` filter described below. Docker-published ports can bypass
ordinary UFW input rules. The filter limits API (8080), Toxiproxy (8474), and
Ruvio (6379) access on the SUT, and OTLP (4317) and Grafana (3000) access on
the observer to their configured peer/client IPs. Ruvio RESP is not encrypted
in transit; prefer an SSH tunnel on untrusted networks. Never expose
Toxiproxy's unauthenticated control API to untrusted sources.

## Prerequisites and one-time setup

Install Docker Engine with the Compose plugin on both Linux hosts and Python 3
on the observer host.
The k6,
OpenTelemetry Collector, Prometheus, Grafana, Tempo, and Node Exporter binaries
run as pinned containers; no JMeter or host-installed k6 is needed. Each host
needs Git access to this repository, sufficient disk space for its Docker
images and retained data, and private-network connectivity between the two
machines.

On both hosts:

```bash
git clone https://github.com/salihcantekin/ruvio.git
cd ruvio/tests/scenario-lab
cp .env.example .env
```

On the **observer host**, make the result directory writable by the pinned k6
container user (`uid/gid 12345`):

```bash
mkdir -p load/results
sudo chown 12345:12345 load/results
```

Use long, unique secrets generated locally, for example
`openssl rand -hex 32`. Do not check `.env` into source control or paste its
contents into logs.

On the **SUT host**, set `API_BIND_IP`, `FAULT_CONTROL_BIND_IP`, and
`RUVIO_BIND_IP` to its peer-reachable interface address, set
`OTEL_EXPORTER_OTLP_ENDPOINT` to `http://<observer-ip>:4317`, and set unique
`RUVIO_PASSWORD` and `REDIS_PASSWORD` values.

On the **observer host**, set `SUT_ADDRESS`, `TARGET_URL` to
`http://<sut-ip>:8080`, `FAULT_CONTROL_URL` to
`http://<sut-ip>:8474`, and `COLLECTOR_BIND_IP` and `GRAFANA_BIND_IP` to the
observer's peer-reachable interface address. Set `GRAFANA_ADMIN_PASSWORD` to a
new unique secret.

When using public IPs, install the peer filter on each host from the copied lab
directory before starting either Compose stack:

```bash
# SUT host; only the observer can reach the test API, fault control, and host metrics.
sudo ufw allow from <observer-ip> to any port 8080 proto tcp
sudo ufw allow from <observer-ip> to any port 8474 proto tcp
sudo ufw allow from <observer-ip> to any port 9100 proto tcp
sudo ufw allow from <your-client-ip> to any port 6379 proto tcp
sudo scripts/install-docker-ingress-filter.sh sut <observer-ip> <your-client-ip>

# Observer host; the SUT sends telemetry, and your client can open Grafana.
sudo ufw allow from <sut-ip> to any port 4317 proto tcp
sudo ufw allow from <your-client-ip> to any port 3000 proto tcp
sudo scripts/install-docker-ingress-filter.sh observer <sut-ip> <your-client-ip>
```

## Start and run

On the SUT host:

```bash
scripts/scenario-lab.sh sut-up
```

On the observer host, run the autonomous suite:

```bash
scripts/scenario-lab.sh suite
```

The suite starts telemetry, waits for the API and Ruvio, runs the smoke
scenario, then starts the sustained workload. The sustained workload
automatically disables the API-to-Ruvio Toxiproxy link once after `FAULT_AFTER`
(default 5 minutes), holds the partition for `FAULT_DURATION` (default 30
seconds), restores the link, and checks Ruvio reconnection. A shell cleanup
trap also restores the proxy if the k6 command exits or receives INT/TERM.
Because the Toxiproxy admin API is unauthenticated, limit it to the private
network and allow only the observer host in the firewall. If the observer host
is forcibly terminated, verify `/proxies/ruvio` reports `enabled: true` before
resuming tests.

The smoke script checks readiness against Ruvio, an output-cache replay,
duplicate order replay and changed-request conflict, and a 20-per-minute
shared rate-limit budget. Its ephemeral rate-limit partition expires
automatically. Its only business side effect is an in-process counter.

Run the sustained mixed workload after the smoke scenario passes:

```bash
DURATION=30m CATALOG_RPS=80 ORDER_RPS=5 RATE_PROBE_RPS=5 \
  docker compose -f compose.observer.yml --profile load run --rm k6 \
  run --out experimental-prometheus-rw \
  --summary-export=/results/sustained.json /scripts/sustained.js
```

The duration and rates are configurable through environment variables. Results
are written to the ignored `load/results/` directory on the observer host.
The `suite` command publishes k6 time series to Prometheus remote-write as well
as writing a JSON summary. Prometheus scrapes the API's Ruvio `/INFO
prometheus` passthrough, .NET runtime/ASP.NET Core metrics, Toxiproxy, and Linux
host metrics. Prometheus retains 30 days by default; Grafana and Tempo data
use named volumes. Prometheus binds to observer loopback; Grafana binds to
`GRAFANA_BIND_IP` and the firewall filter admits only its configured client
address. Its browser session uses HTTP; do not use an untrusted network.
Alternatively, keep Grafana bound to loopback and use an SSH tunnel:

```bash
ssh -L 3000:127.0.0.1:3000 -L 9090:127.0.0.1:9090 <observer-host>
```

The Grafana UI is then available at `http://127.0.0.1:3000`. Explore Ruvio
Prometheus metrics, .NET runtime/ASP.NET Core metrics, Linux host metrics, and
traces sent through OTLP to Tempo.

### Bounded stress profile

After deploying the current tree to both hosts and confirming that telemetry is
available, run the separate stress profile on the observer:

```bash
scripts/scenario-lab.sh stress
```

The default run lasts 28 minutes in 14 two-minute constant-arrival-rate steps:
50, 100, 200, 400, 600, 800, 1,200, 1,600, 2,000, 2,400, and 3,000
iterations/second, followed by a drop to 300, recovery to 1,600, and a second
3,000 peak. Each iteration exercises one of output cache, idempotency (a create plus replay),
or distributed rate limiting. It allows 250 preallocated and up to 4,000 k6
VUs; those are load-generator settings, not a Ruvio or server capacity limit.
Change the 14 rates with `STRESS_RATES`, stage duration with
`STRESS_STAGE_SECONDS` (30-600 seconds), and the VU ceiling with
`STRESS_MAX_VUS` (up to 5,000). Longer stages increase test duration linearly.

The stress command records k6 stdout/stderr in `load/results/<run>-stress.log`
and its summary JSON beside it. It stops the k6 container if the API/Ruvio
readiness endpoint fails three consecutive checks or Prometheus reports SUT
available memory below `STRESS_MIN_MEMORY_AVAILABLE_PERCENT` (10% by default).
The SUT memory guard requires the Linux Node Exporter target to be scraped; if
it is absent, that check is skipped. On Linux, it also stops if the observer
falls below the same available-memory threshold or its one-minute load average
exceeds twice its CPU count for three checks. A guarded stop is recorded in a
sibling `*-stress-stop-reason.txt` file. k6's dropped iterations and thresholds
distinguish target arrival rate from achieved throughput; do not interpret
configured rates or VUs as measured Ruvio capacity.

The integration libraries publish low-cardinality counters and duration
histograms for their Ruvio storage calls:

- `ruvio.output_cache.storage_calls`,
  `ruvio.output_cache.storage_commands`, and
  `ruvio.output_cache.storage_duration` (operation: entry/tag read/write/evict).
- `ruvio.idempotency.storage_calls`,
  `ruvio.idempotency.storage_commands`, and
  `ruvio.idempotency.storage_duration` (BEGIN/COMPLETE), plus
  `ruvio.idempotency.states` (acquired/completed/pending/conflict).
- `ruvio.rate_limit.storage_calls`,
  `ruvio.rate_limit.storage_commands`, and
  `ruvio.rate_limit.storage_duration`, alongside accepted/rejected/error counts.

The storage duration histograms measure time in the respective client call;
commands count individual Redis-compatible commands, including commands in
batched calls. The scenario API additionally exports its process working set
and cumulative CPU time (`api_process_working_set_bytes`,
`api_process_cpu_time_seconds` before the collector namespace prefix). Ruvio
key count, command counters, memory accounting,
host CPU/memory/disk, .NET GC, thread-pool, HTTP, and k6 metrics remain separate
signals and should be correlated by `test_run` and `load_stage`. On Linux,
`ruvio_process_resident_memory_bytes` exposes Ruvio's process RSS; the same
process value is repeated for each shard label, so use `max` rather than `sum`
when graphing it.

## Current scope and safety limits

For a short paired native-grouping check through the scenario API:

```bash
docker compose -f compose.observer.yml --profile load run --rm \
  -e TEST_RUN=paired-group-check -e COMPARE_BACKEND=both \
  -e COMPARE_RATE_RPS=200 -e COMPARE_STAGE_SECONDS=120 \
  -e COMPARE_GAP_SECONDS=30 k6 run \
  --out experimental-prometheus-rw \
  --summary-export=/results/paired-group-check.json /scripts/compare.js
```

Use a unique `TEST_RUN` for each measurement. This covers GET, SET PX, native
IDEM BEGIN/COMPLETE and LIMIT (Redis semantic equivalents use Lua), not every
supported command. The comparison exports backend-specific HTTP duration
summaries, requires all functional checks to pass, and counts transport failures
as unsuccessful requests. Backend p95 must remain below one second. Fixed
arrival rate measures latency at that load, not maximum throughput; the combined
iteration rate includes the gap. Keep both persistence configurations explicit
and unchanged between stages.

- The SUT Ruvio and Redis instances are configured for synchronous persistence
  (`always` / Redis AOF `appendfsync always`) during the paired comparison.
  This intentionally includes persistence cost in both backend results.
  For an explicitly non-persistent comparison, set `RUVIO_DURABILITY=memory`
  and `REDIS_APPENDONLY=no` in the SUT `.env`. Restore `RUVIO_DURABILITY=always`,
  `REDIS_APPENDONLY=yes`, and `REDIS_APPENDFSYNC=always`, then recreate the SUT
  containers afterward. The benchmark runner does not change persistence
  modes automatically.
- The order side-effect counter is process-local; it demonstrates HTTP
  idempotency on one API instance, not transactional exactly-once behavior
  across a database or multiple API replicas.
- The automated fault is a bounded application-to-Ruvio network partition. It
  does not fill host disks, terminate the host, or simulate physical power loss.
- The `suite` profile opts into exactly one 30-second network partition. The
  standalone `sustained` command does not inject faults unless explicitly
  configured with `ENABLE_RUVIO_PARTITION=true`.
- No 24-hour soak is started automatically. Begin with a 30-minute profile,
  inspect resource and error trends, then increase duration deliberately.
- Docker Desktop skips Node Exporter because its Linux container cannot
  provide meaningful macOS/Windows host metrics. On macOS, local k6 runs can
  use `SUT_ADDRESS=host.docker.internal`, the published `API_PORT`, and
  `TARGET_URL=http://host.docker.internal:<API_PORT>`. For partition tests,
  set host-side `FAULT_CONTROL_URL=http://127.0.0.1:8474` and
  `K6_FAULT_CONTROL_URL=http://host.docker.internal:8474`. Linux VPS results
  are recorded separately with host and image details in
  [`docs/evidence/2026-10-03-dotnet-scenario-lab.md`](../../docs/evidence/2026-10-03-dotnet-scenario-lab.md).

### Paired Ruvio / Redis trace comparison

The `compare` command runs the same 40% cache-read, 30% idempotency
create-and-replay, and 30% rate-limit workload first against Ruvio, then
against Redis. Each backend receives a five-minute constant-arrival stage at
200 iterations/s (about 260 HTTP requests/s at the configured workload mix),
with a one-minute quiet gap between stages. A paired run therefore takes about
11 minutes, plus container startup and summary output; it is not a five-minute
total run. The default 2,000 preallocated VUs avoids creating load-generator
workers during the measured stage. Configure the rate, stage length, gap, and
VU limits with the `COMPARE_*` variables. This is a comparison of
the same scenario semantics using the Ruvio .NET client/native IDEM/LIMIT
commands and StackExchange.Redis/Lua implementations; it does not route
Ruvio-only ASP.NET integration packages through Redis.

```bash
scripts/scenario-lab.sh sut-up
# On the observer, after checking that the Redis and Ruvio targets are up:
scripts/scenario-lab.sh compare
```

To run only one five-minute backend stage against a historical baseline:

```bash
docker compose -f compose.observer.yml --profile load run --rm \
  -e COMPARE_BACKEND=ruvio -e COMPARE_STAGE_SECONDS=300 \
  -e TEST_RUN=unique-run-id k6 run \
  --out experimental-prometheus-rw \
  --summary-export=/results/unique-run-id-ruvio.json /scripts/compare.js
```

`COMPARE_BACKEND` accepts `both` (default), `ruvio`, or `redis`. A historical
comparison is not a contemporaneous controlled pair; retain the same durability,
offered rate and telemetry settings, and state the time/order limitation.

Every benchmark HTTP request has an `X-Lab-Request-ID` and a sampled-at-100%
OpenTelemetry server trace. The root HTTP span carries the backend, test run,
stage, and request ID; child `cache.*`, `idempotency.*`, or `rate_limit` spans measure the
scenario storage layer, and nested `db.client` spans measure the awaited
client operation. The Ruvio client adds child spans for socket semaphore
queueing, socket writes, and waiting for the RESP reply. Slow requests (at
least one second) and server failures are printed as JSON with their request
and trace IDs, so an individual outlier can be opened in Tempo. ASP.NET
request spans and .NET runtime/GC metrics provide
the outer request and runtime context. k6's backend/operation tags provide
arrival rate, dropped iterations, checks, and HTTP latency distributions.
The `scenario_lab_ruvio_client_socket_phase_duration_milliseconds` histogram
also aggregates Ruvio socket queue, write, and reply-wait durations by `phase`,
so the client-side split can be compared without manually aggregating traces.

The `db.client` duration is an end-to-end await, not pure network time. Ruvio
exports per-shard command counts and cumulative command-processing duration
(excluding socket I/O) split into GET, SET, increment, and other commands;
Redis INFO-derived API metrics expose Redis server command, CPU, memory, and
persistence metrics. Ruvio's `ruvio_wal_syncs_total` and
`ruvio_wal_sync_duration_seconds_sum` measure dirty WAL syncs separately from
command dispatch. Redis AOF sync spikes can be inspected with its latency
monitor (`aof-fsync-always`) when enabled. These server measurements are
aggregate series and cannot be subtracted from an individual client span to
claim an exact per-request network duration. Use them alongside the request
trace: HTTP span minus child storage spans indicates time in API/framework
and other request work, while the client span and server command distributions
show storage wait and server work at their respective layers.

The k6 HTTP `name` tag is normalized to the operation name so URLs with
different catalog SKUs do not create separate percentile series. Without this
normalization, taking the maximum of per-URL p99 values is not a valid
backend-wide or operation-wide p99.

Ruvio command-processing counters are `ruvio_command_processing_seconds_sum`,
`ruvio_get_command_processing_seconds_sum`,
`ruvio_set_command_processing_seconds_sum`,
`ruvio_increment_command_processing_seconds_sum`, and
`ruvio_other_command_processing_seconds_sum`, paired with their corresponding
`ruvio_*_commands_total` counters. Divide the matching duration delta by the
command-count delta for the observed average server-side processing time.
Redis command deltas come from `INFO commandstats`; `total_commands_processed`
also includes commands executed inside Lua scripts.

The k6 JSON summary and console log are saved as
`load/results/<run-id>-compare.{json,log}`. Prometheus uses the same `test_run`
label for both stages; the `backend` and `operation` tags separate the result
series, and adapter metrics include `test_run` and `load_stage` labels. Tempo
retains individual traces for drill-down.
