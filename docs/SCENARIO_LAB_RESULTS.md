# .NET Scenario Lab Results

## 2026-10-03 Ruvio / Redis layer-by-layer comparison

Accepted evidence (raw k6 summaries are not included in this repository):

- Synchronous persistence enabled
- Synchronous persistence disabled

The paired comparison is **five minutes per backend**, plus a one-minute gap:
about 11 minutes of test time, not five minutes total. Both runs sent traffic
to Ruvio first and Redis second.

### Conditions and correctness

Both tests used the same two-vCPU Ubuntu 24.04 SUT and ASP.NET Core scenario
API. k6 0.54.0 offered 200 iterations/s per backend with a 40% cache-read,
30% idempotency create-and-replay, and 30% rate-limit mix. Traces were sampled
at 100%.

| Run | Ruvio | Redis | Iterations | HTTP requests | Dropped iterations | Failed requests |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Persistence on | `always` | AOF `yes`, `appendfsync always` | 60,001 each | 155,886 total | 0 | 0 |
| Persistence off | `memory` | AOF `no` | 60,001 each | 155,973 total | 0 | 0 |

All checks passed across 120,002 iterations in each accepted run. The second run used
2,000 preallocated VUs after a warm-up smoke test; only five VUs were active at
once because requests completed quickly. An earlier persistence-off attempt
dropped 1,418 Ruvio iterations, so it is excluded from the comparison and its
results are not mixed into the table below.

This is a semantic comparison of the scenario adapters: the Ruvio .NET client
with native IDEM/LIMIT commands versus StackExchange.Redis with Lua scripts.
It does **not** exercise the production ASP.NET Output Cache, Idempotency, or
Rate Limiting middleware packages for both backends.

### Awaited storage-call duration

These measurements are the API adapter's end-to-end awaited storage calls.
They include client queuing, socket I/O, server work, and reply handling; they
are not pure network time. Percentiles are estimated from the newly refined
Prometheus histogram buckets and remain bucket-interpolated estimates.

| Persistence | Backend | Operation | Mean | p50 | p95 | p99 |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| On | Ruvio | Cache GET | 9.55 ms | 0.40 ms | 1.76 ms | 365.80 ms |
| On | Ruvio | Cache SET | 11.17 ms | 1.24 ms | 47.66 ms | 352.60 ms |
| On | Ruvio | Idempotency BEGIN | 8.54 ms | 1.04 ms | 3.94 ms | 276.98 ms |
| On | Ruvio | Idempotency COMPLETE | 8.70 ms | 1.12 ms | 4.29 ms | 299.27 ms |
| On | Ruvio | Rate limit | 10.54 ms | 1.38 ms | 4.61 ms | 361.43 ms |
| On | Redis | Cache GET | 0.78 ms | 0.37 ms | 1.49 ms | 13.74 ms |
| On | Redis | Cache SET | 1.44 ms | 0.96 ms | 3.17 ms | 13.06 ms |
| On | Redis | Idempotency BEGIN | 1.23 ms | 0.92 ms | 2.78 ms | 13.44 ms |
| On | Redis | Idempotency COMPLETE | 1.54 ms | 0.98 ms | 2.93 ms | 16.46 ms |
| On | Redis | Rate limit | 1.70 ms | 1.26 ms | 3.13 ms | 14.74 ms |
| Off | Ruvio | Cache GET | 0.52 ms | 0.42 ms | 1.12 ms | 2.63 ms |
| Off | Ruvio | Cache SET | 0.36 ms | 0.30 ms | 0.80 ms | 2.21 ms |
| Off | Ruvio | Idempotency BEGIN | 0.51 ms | 0.41 ms | 1.13 ms | 2.63 ms |
| Off | Ruvio | Idempotency COMPLETE | 0.38 ms | 0.33 ms | 0.84 ms | 1.98 ms |
| Off | Ruvio | Rate limit | 0.54 ms | 0.44 ms | 1.13 ms | 2.49 ms |
| Off | Redis | Cache GET | 0.48 ms | 0.39 ms | 0.99 ms | 2.39 ms |
| Off | Redis | Cache SET | 0.29 ms | 0.22 ms | 0.69 ms | 1.48 ms |
| Off | Redis | Idempotency BEGIN | 0.49 ms | 0.39 ms | 1.05 ms | 2.49 ms |
| Off | Redis | Idempotency COMPLETE | 0.37 ms | 0.28 ms | 0.83 ms | 1.98 ms |
| Off | Redis | Rate limit | 0.51 ms | 0.42 ms | 1.10 ms | 2.28 ms |

The medians are much closer than the means in the persistent Ruvio run. The
large mean and p99 therefore reflect a real long tail, not every request
costing 10 ms. With persistence disabled, both adapters are near 0.5 ms mean
and have similar tails. The Ruvio/Redis difference in the persistent run is
therefore strongly associated with the persistence-enabled Ruvio path.

### Ruvio client phase measurements

The client now emits `socket.queue`, `socket.write`, and `socket.reply` trace
spans and exports
`scenario_lab_ruvio_client_socket_phase_duration_milliseconds` by phase.
These remote runs predate the multiplexing change described below.
In that client version, `queue` measures waiting for the per-socket semaphore;
the client holds that semaphore while writing **and waiting for the reply**. `write`
measures socket write/flush. `reply` includes waiting for the server response,
the return path, and RESP parsing, so it is not a pure network measurement.

| Persistence | Phase | Mean | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| On | Queue | 6.38 ms | 0.006 ms | 1.24 ms | 230.40 ms |
| On | Write | 0.055 ms | 0.038 ms | 0.210 ms | 0.399 ms |
| On | Reply wait | 0.896 ms | 0.836 ms | 2.01 ms | 4.36 ms |
| Off | Queue | 0.032 ms | 0.005 ms | 0.013 ms | 0.836 ms |
| Off | Write | 0.066 ms | 0.043 ms | 0.223 ms | 0.418 ms |
| Off | Reply wait | 0.355 ms | 0.312 ms | 0.833 ms | 1.76 ms |

This is the first direct evidence for the earlier latency question: in the
persistent run, socket semaphore queueing averaged about 6.4 ms per Ruvio
client operation, versus about 0.03 ms with persistence off. The queue p99
also rose sharply. This is consistent with a serialized client socket
amplifying slower replies into wait time for subsequent commands. The client
queue metric localizes the latency; it does not by itself prove whether the
remaining cause is WAL write/sync scheduling, command processing, or both.
Phase-duration sums add up concurrent operations, so their sums can exceed
the five-minute wall-clock stage and must not be interpreted as elapsed test
time.

### Server-side processing and durability

| Persistence | Ruvio commands | Command-processing duration | Dirty WAL syncs | WAL sync duration |
| --- | ---: | ---: | ---: | ---: |
| On | ~99,700 | 58.34 s | ~1,965 | 1.99 s total (~1.01 ms/sync) |
| Off | ~102,600 | 2.60 s | 0 | 0 s |

Ruvio command-processing duration excludes socket I/O and is reported
separately from dirty WAL synchronization. The `other` command bucket includes
metrics/INFO traffic. The WAL timer measures sync attempts with dirty bytes;
its 1.99 seconds is accumulated across the run, not a per-request duration.
Redis was configured with latency monitoring at a 1 ms threshold. During the
persistent run, the retained `aof-fsync-always` history included events up to
17 ms; Redis retains only a bounded recent history, so this is not a full-run
percentile. No AOF fsync events appeared with AOF disabled.

Raw Redis command counts are not directly comparable to Ruvio command counts:
Redis counts Lua script invocations and the individual commands executed
inside them, while native IDEM/LIMIT operations are each one Ruvio command.

### What the HTTP and server metrics do—and do not—tell us

The combined k6 HTTP distribution was 1.90 ms mean / 3.58 ms p95 with
persistence off, versus 7.85 ms mean / 6.19 ms p95 with persistence on.
These are combined two-backend values and cannot be attributed to either
backend; their means are tail-sensitive. Per-request Tempo traces now show
the HTTP server span, adapter span, `db.client` span, and Ruvio socket phases.
Ruvio's server metrics separately show command processing and WAL sync.

We still cannot claim an exact per-request “network-only” duration by
subtracting aggregate server timings from client timings. The `reply` span
contains server response time as well as the network return path, and the
server metrics are aggregate series. A paired server receive/send timestamp
instrumentation point would be needed to isolate one-way network time.

The previous result's coarse-bucket p50/p95/p99 values remain withdrawn. The
new histograms expose the tail much better, but percentile values are still
interpolated within buckets. Raw k6 console logs are intentionally not kept
in the repository; the JSON summaries above are retained.

## 2026-10-03 local C# client multiplexing comparison

The client now has a single writer and a single FIFO reply reader per command
socket. It can send subsequent commands before earlier replies arrive. Bounded
channels and an admission semaphore limit admitted command groups to 256 by
default; this is not a lock-free design or a strict byte limit. The socket is
not locked across request/reply round trips. There is no per-command linked CTS
or timer, and cancellation does not interrupt the shared reader.

### Method

Same existing Release Ruvio binary on macOS, bound to `127.0.0.1:16389`,
`--durability memory`, same dedicated server process and client connection
settings (`DiscoverCluster=false`, `Reconnect=false`, no tracing/metric listener).
Server/WAL code was not changed for this comparison. Client and server were
separate processes on the same machine.

The client benchmark used one connection per case, a one-second warmup and
three seconds of measured closed-loop traffic, with 1, 16 or 128 concurrent
callers. GET read a 256-byte value; SET wrote a 256-byte value to each worker's
key; LIMIT used each worker's key, a 1,000,000,000 limit and a 60,000-ms window.
The command order was GET, SET, LIMIT, each at increasing concurrency.
The pre-change baseline was recorded before editing the client. The table
uses the final isolated post-change run, which also validated reply shapes.
Additional post-change runs confirmed the concurrent-load improvement, but
single-caller performance varied; this is a short exploratory benchmark,
not a statistical confidence interval or a production capacity estimate.

Run the retained benchmark with:

```bash
dotnet run -c Release --project clients/csharp/Ruvio.Client.Benchmarks -- --socket-load 16389 3
```

### Results

| Command | Concurrent callers | Before ops/s | After ops/s | Throughput ratio | Before p99 | After p99 | Allocated bytes/op before / after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 1 | 39,043 | 32,850 | 0.84x | 0.035 ms | 0.045 ms | 1,744 / 1,992 |
| GET | 16 | 35,808 | 177,745 | 4.96x | 0.515 ms | 0.229 ms | 1,832 / 1,792 |
| GET | 128 | 34,746 | 238,390 | 6.86x | 4.442 ms | 0.989 ms | 1,832 / 1,785 |
| SET | 1 | 37,510 | 21,067 | 0.56x | 0.046 ms | 0.082 ms | 1,424 / 1,672 |
| SET | 16 | 34,792 | 174,925 | 5.03x | 0.716 ms | 0.231 ms | 1,512 / 1,474 |
| SET | 128 | 35,382 | 231,131 | 6.53x | 4.087 ms | 1.029 ms | 1,513 / 1,467 |
| LIMIT | 1 | 39,558 | 32,948 | 0.83x | 0.032 ms | 0.053 ms | 1,392 / 1,640 |
| LIMIT | 16 | 35,980 | 177,144 | 4.92x | 0.527 ms | 0.226 ms | 1,480 / 1,443 |
| LIMIT | 128 | 36,212 | 256,188 | 7.07x | 3.857 ms | 0.928 ms | 1,480 / 1,432 |

Latency histograms here are local 1-microsecond buckets, not Prometheus
histogram interpolation. Samples at or above 100 ms go into an overflow
bucket. The final 128-caller GET/SET/LIMIT cases had 3/5/6 overflow samples;
their p99 values are below that bucket, but the reported distribution must
not be interpreted as a maximum-latency bound.

### Memory and interpretation

Under concurrent load, allocations per operation decreased by approximately
2-3%. This includes the benchmark harness, not just client allocations.
End-of-case client working set ranged from 60.3-63.9 MiB before versus
61.0-65.4 MiB after. It did not fall: the queues, completion objects and more
outstanding work have a small footprint. These are process samples, not
peak or exclusively client-owned memory.

GC counts per three-second case generally increased because the new client
completed many more operations. For example, at 128 concurrent GET callers,
Gen0 collections were 23 before and 154 after while throughput increased
6.86x. Raw GC counts cannot be read as a per-operation allocation regression.
Single-caller allocations increased by about 248 bytes/op, and throughput
regressed in that path. Queue/completion scheduling still has overhead, and
there is no claim that this first implementation is faster or uses less
memory in every workload.

The concurrent-load improvement supports removing client round-trip
serialization as the first optimization. This local memory-mode result
does **not** establish the improvement with synchronous persistence, compare
against Redis, or exercise the full ASP.NET integration stack. Those earlier
remote results remain historical evidence, not measurements of this new client.

### Correctness coverage

Client tests passed: 117. Added coverage verifies commands sent before any
reply, FIFO matching, pipeline error replies, bounded admission, cancellation
before dispatch and during writes, binary buffer ownership after cancellation,
draining abandoned replies, per-request read retry eligibility,
disconnect/dispose fan-out without reopening disposed clients, and invalid capacities.
Existing reconnect, cluster, database, binary and Pub/Sub tests remain passing.
The local live idempotency suite, including HTTP middleware, passed 68 tests.

## Local adaptive-writer follow-up

The writer now starts on the caller's path when no writes are scheduled. An
atomic reservation counter prevents a new caller bypassing queued work during
ownership handoff. Busy callers enter the bounded queue; the current owner drains
it without waiting for replies. The single reader, admission bound and cancellation
contract are unchanged. No polling loop, reader handoff or per-call task worker
was added. The normal fast path bypasses the outgoing channel, but it is not
allocation-free: request/completion objects and reply parsing still remain.

Disabled tracing now avoids constructing phase-name strings, and disabled
histograms skip recording. These are included in the following results, so
allocation improvements must not be attributed solely to direct writing.

Same local memory-mode server and method as above, with a fresh pre-change
queued-multiplexer baseline at 1/2/4/16/128 callers. Two isolated final client
runs used three measured seconds per case after one second of warmup.
The post-change range is the two observed results, not a confidence interval.

| Command | Callers | Queued multiplexer ops/s | Adaptive writer ops/s (two runs) | Allocated bytes/op before / after |
| --- | ---: | ---: | ---: | ---: |
| GET | 1 | 33,562 | 32,659-34,045 | 1,992 / 1,832 |
| GET | 2 | 64,917 | 64,385-66,215 | 1,884 / 1,725 |
| GET | 4 | 74,511 | 76,555-88,066 | 1,847 / 1,685-1,687 |
| GET | 16 | 179,045 | 181,706-183,544 | 1,793 / 1,633 |
| GET | 128 | 249,338 | 243,247-258,962 | 1,785 / 1,625 |
| SET | 1 | 20,371 | 21,806-23,533 | 1,672 / 1,512 |
| SET | 2 | 40,886 | 43,093-44,275 | 1,582 / 1,421 |
| SET | 4 | 74,631 | 76,890-78,359 | 1,526 / 1,368-1,369 |
| SET | 16 | 173,810 | 172,285-178,286 | 1,475 / 1,315 |
| SET | 128 | 244,392 | 245,921-248,540 | 1,466 / 1,306 |
| LIMIT | 1 | 34,743 | 20,742-21,871 | 1,640 / 1,480 |
| LIMIT | 2 | 64,290 | 39,586-39,697 | 1,535 / 1,384-1,385 |
| LIMIT | 4 | 84,338 | 76,046-76,149 | 1,494 / 1,333-1,337 |
| LIMIT | 16 | 179,502 | 174,962-175,792 | 1,440 / 1,282-1,284 |
| LIMIT | 128 | 251,933 | 263,296-267,631 | 1,433 / 1,271 |

The repeatable result is lower allocation, about 160 bytes/op in most cases,
while preserving the large concurrent-load throughput of multiplexing.
Single-caller GET was effectively unchanged, SET improved modestly, and LIMIT
was slower than this baseline. Single-caller LIMIT had already varied between
roughly 20k and 35k ops/s in prior queued-multiplexer runs. This test does not
resolve that variability or establish recovery of the original serialized
client's single-caller performance.

Single-caller p99 was GET 0.044 ms before versus 0.050-0.056 ms after,
SET 0.082 ms versus 0.073-0.082 ms, and LIMIT 0.045 ms versus 0.077-0.082 ms.
At 128 callers it was GET 0.918 ms versus 0.927-1.072 ms,
SET 0.976 ms versus 0.950-1.000 ms, and LIMIT 0.935 ms versus 0.880-0.909 ms.
End-of-case process working set was approximately 61-65 MiB before and
49-64 MiB after across the two runs; GC timing and prior cases affect this
sample, so it is not evidence of a fixed retained-memory reduction.

Added tests exercise direct initiation, 1,024 calls from concurrent producers
through repeated ownership handoffs with an admission limit of eight, and
shutdown while a direct write, queued writes and capacity waiters coexist.
Final validation passed 119 client tests and 68 local live idempotency/HTTP tests;
the scenario API build also succeeded.

## 2026-10-03 Linux adaptive client: five-minute persistence-on run

Evidence: k6 summary.
The updated C# client was deployed to the existing SUT
`<SUT-IP>`; only the API image was rebuilt/recreated. The Ruvio server
image and data volume were unchanged, with durability still `always`.
The load ran from `<observer-IP>`, starting at 22:17:30 UTC, with a single
300-second Ruvio stage, 200 iterations/s, 2,000 preallocated VUs, and the same
40/30/30 scenario mix and enabled telemetry as the earlier remote comparison.
The warmup smoke passed 11/11 checks. Redis was not rerun; comparison below
uses the accepted earlier persistence-on Redis stage.

The new run completed **60,001 iterations and 78,115 HTTP requests**, all
60,001 functional checks passed, with no failed HTTP requests, dropped or
interrupted iterations. Peak active VUs reached 443. Offered-rate completion
does not establish maximum capacity: this test was capped at 200 iterations/s.

### Storage latency: historical Redis versus old/new Ruvio

All durations are milliseconds. Means and p99 are adapter histogram
measurements; p99 is bucket-interpolated, not an exact sample percentile.

| Operation | Old Ruvio mean | New Ruvio mean | Earlier Redis mean | Old Ruvio p99 | New Ruvio p99 | Earlier Redis p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cache GET | 9.55 | 22.59 | 0.78 | 365.80 | 1,106.05 | 13.74 |
| Cache SET | 11.17 | 15.71 | 1.44 | 352.60 | 582.00 | 13.06 |
| Idempotency BEGIN | 8.54 | 17.99 | 1.23 | 276.98 | 584.94 | 13.44 |
| Idempotency COMPLETE | 8.70 | 16.66 | 1.54 | 299.27 | 653.41 | 16.46 |
| Rate limit | 10.54 | 24.90 | 1.70 | 361.43 | 1,072.28 | 14.74 |

**This run did not reproduce the local latency improvement.** New Ruvio
adapter means/tails were worse than the historical Ruvio measurement and
still substantially higher than historical Redis. Medians remained small:
GET 0.424 ms, SET 1.323 ms, BEGIN 1.128 ms, COMPLETE 1.239 ms and LIMIT
1.722 ms. The high averages therefore reflect a long tail, not uniformly
slow commands.

HTTP mean was 25.86 ms, median 2.54 ms, p95 8.38 ms, maximum 3.71 seconds.
This is Ruvio-only HTTP traffic, so it must not be compared directly with
the earlier two-backend combined HTTP mean.

### Client and server observations

| Phase | New mean | New p95 | New p99 |
| --- | ---: | ---: | ---: |
| Queue/admission/encoding | 5.56 ms | 0.034 ms | 145.43 ms |
| Write | 0.051 ms | 0.198 ms | 0.386 ms |
| Reply completion | 14.72 ms | 4.70 ms | 405.93 ms |

Old queue mean/p99 were 6.38/230.40 ms. Queue waiting decreased, but it
did not disappear. The new reply span starts when the request enters the
pending-response queue and ends when its reply is parsed, so it includes
waiting behind previous pipelined replies and overlaps socket writing.
It is **not equivalent to the old reader-only reply timing**, nor is it
network-only time. Do not add phase percentiles or interpret this as proof
of an isolated server/WAL/client cause.

Prometheus window estimates reported approximately 102k Ruvio commands,
71.37 accumulated command-processing seconds, 1,916 dirty WAL syncs and
2.08 accumulated WAL sync seconds (about 1.08 ms/sync). The old persistent
run reported 58.34 processing seconds and 1.99 WAL sync seconds.
The measured WAL sync total alone does not explain the new seconds-scale
HTTP tail.

API runtime estimates over the load and trailing scrape window:
approximately 1.25 GB cumulative allocation, 0.215 seconds GC duration,
123 seconds process CPU time, sampled maximum working set 330 MiB and
GC committed memory 220 MiB. Sampled Ruvio RSS peaked at 29 MiB.
The sampled ThreadPool queue length stayed at zero; sampling does not rule
out short scheduling stalls. Cumulative allocations are not retained RAM.

### Interpretation and limitations

The local memory-mode concurrency gain remains valid for those local
conditions, but this persistent, fully instrumented Linux run shows that
removing client round-trip serialization is **not sufficient** to resolve
the observed production-like tail. This does not prove the adaptive writer
caused the regression: Redis and old Ruvio baselines were measured earlier,
data/warmup state and VPS scheduling can differ, and the server was not
restarted. The next investigation needs correlated slow traces and server
timelines before attributing the tail or changing WAL behavior.

Storage series are selected by the unique run label using cumulative
histograms at the trailing scrape. Their exported counts do not fully
reconcile with the k6 workload; they must not be reported as exact call
counts or a complete per-request accounting. Socket and server deltas use
a 332-second window including startup/trailing scraping, and Prometheus
`increase`/`delta` extrapolate. Resource figures are therefore approximate.
No raw console logs were added to the repository. The API remained ready
after the test, with persistence settings unchanged.

## 2026-10-03 Linux adaptive client: five-minute memory-mode run

Evidence: k6 summary.
The same updated API/client and Linux hosts were used. Ruvio was recreated
with `RUVIO_DURABILITY=memory`, preserving its named data volume; the API
was restarted and the functional smoke passed before load. The 300-second
Ruvio-only stage started at 22:30:38 UTC, with the same 200 iterations/s,
2,000 preallocated VUs, scenario mix and telemetry. Redis was not rerun;
the baseline below is its earlier accepted AOF-disabled stage.

Completed 60,001 iterations and 78,195 HTTP requests. All 60,001 checks passed;
there were no failed requests, dropped or interrupted iterations. Only four
VUs were active at peak. HTTP mean was 1.89 ms, median 1.61 ms, p95 3.41 ms
and maximum 208 ms, compared with the new client's persistence-on mean of
25.86 ms and maximum 3.71 seconds. The old combined memory-mode HTTP mean
was 1.90 ms, but that combined number must not be treated as Redis-only.

### Awaited storage durations, milliseconds

| Operation | Old Ruvio memory mean | New Ruvio memory mean | Earlier Redis AOF-off mean | Old Ruvio p99 | New Ruvio p99 | Earlier Redis p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cache GET | 0.52 | 0.443 | 0.48 | 2.63 | 1.527 | 2.39 |
| Cache SET | 0.36 | 0.332 | 0.29 | 2.21 | 1.409 | 1.48 |
| Idempotency BEGIN | 0.51 | 0.441 | 0.49 | 2.63 | 1.496 | 2.49 |
| Idempotency COMPLETE | 0.38 | 0.333 | 0.37 | 1.98 | 1.225 | 1.98 |
| Rate limit | 0.54 | 0.467 | 0.51 | 2.49 | 1.500 | 2.28 |

New memory-mode means are approximately 8-14% below the historical Ruvio
memory-mode means; estimated p99 values are approximately 36-43% lower.
They are comparable to the earlier Redis AOF-off measurements: Redis SET
mean is lower, while the other new Ruvio means and all estimated p99 values
are lower. This is a historical comparison, not a simultaneous A/B or proof
of a backend's maximum throughput. At this offered rate both complete the
load, so capacity differences are not measured.

### Layer/resource observations

New client queue mean/p99 were 0.0073/0.0684 ms, write 0.054/0.320 ms,
and reply-completion 0.376/1.391 ms. The earlier Ruvio memory queue
mean/p99 were 0.032/0.836 ms. Reply timing has the new overlapping
pipeline semantics described above and is not directly comparable to
the old reader-only timing.

WAL sync count/duration were zero, confirming this run did not perform
dirty WAL syncs. Approximately 99k server commands accumulated 2.45 seconds
of processing time, versus 71.37 seconds in the new persistence-on run.
That processing metric is aggregate and does not alone identify which
persistence-path operation caused the tail.

Runtime/window estimates: 1.25 GB cumulative API allocation, 0.104 seconds
GC duration, 119 seconds API CPU time, sampled peak API working set 377 MiB,
GC committed memory 209 MiB and Ruvio RSS 28 MiB. Working set was higher
than in the preceding persistent run despite lower latency; retained
process memory/GC timing cannot be inferred from cumulative allocation.

The memory-mode result shows that the new client can deliver sub-millisecond
storage means in this Linux environment. The severe tail is strongly
associated with the persistence-enabled test conditions; it is not evidence
that the new client is uniformly slow. Exact causality still requires
correlated server/client investigation, because recreation, data state,
warmup and VPS scheduling also differ. All earlier histogram-count and
scrape-window limitations apply here; p99 values are interpolated estimates.

After telemetry capture, Ruvio was restored to `always`, the API restarted,
and readiness returned `ready`/`PONG`. Named data volumes were not removed.

### Middleware comparison scope

The `/bench/*` routes used by these comparisons have shared cache, order
idempotency and rate-limit scenario logic. Their storage adapter chooses
native Ruvio IDEM/LIMIT or Redis Lua, with GET/SET for caching. They do not
attach the production cache/idempotency/rate-limit endpoint policies.
The real Ruvio middleware registrations exist in the app, but their
functional policies are attached to the separate `/api/*` routes.
Redis does not have an equivalent production middleware stack in this lab.

These semantic-adapter tests should be complemented by direct matched
GET/SET/client tests without HTTP or middleware to isolate transport and
server costs, and by separate `/api/*` tests for the real integration
packages. No single test answers all three questions.

## Independent RESP durability probe on the Linux SUT

Evidence: summary.
This probe excluded the Ruvio C#/Rust clients, HTTP, middleware and Toxiproxy.
The API was stopped for the measurements. A standalone Python 3 RESP driver
ran on the SUT and connected directly to each container's bridge address.
It sent AUTH once, warmed up 100 GETs per connection, then alternated GET
and SET against per-worker test keys. Values were 256 bytes; each SET
changed a counter suffix to avoid identical-value write optimization.
Every reply was validated. There was no pipelining.

Four sequential conditions each measured approximately one minute:
Ruvio memory, Ruvio always, Redis AOF-off, Redis AOF-on/appendfsync-always.
Each condition comprised 30 seconds with one connection and 30 seconds with
16 connections/threads, in that order. The relevant container was recreated
for each condition, preserving its data volume; a three-second startup wait
preceded warmup. Redis was version 7.4.2. Both services were restored to
synchronous persistence and the API was restarted/verified ready afterwards.

| Backend / durability | Connections | Mixed GET+SET ops/s | GET mean | SET mean | GET p99 | SET p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Ruvio memory | 1 | 14,750 | 0.066 ms | 0.066 ms | 0.23 ms | 0.23 ms |
| Ruvio always | 1 | 2,301 | 0.109 ms | 0.751 ms | 0.40 ms | 1.78 ms |
| Redis AOF-off | 1 | 13,930 | 0.069 ms | 0.070 ms | 0.26 ms | 0.27 ms |
| Redis AOF always | 1 | 2,218 | 0.110 ms | 0.783 ms | 0.50 ms | 3.10 ms |
| Ruvio memory | 16 | 14,063 | 1.131 ms | 1.137 ms | 4.76 ms | 4.75 ms |
| Ruvio always | 16 | 6,030 | 2.616 ms | 2.684 ms | 9.21 ms | 9.52 ms |
| Redis AOF-off | 16 | 13,602 | 1.169 ms | 1.176 ms | 4.80 ms | 4.79 ms |
| Redis AOF always | 16 | 10,006 | 1.594 ms | 1.598 ms | 3.59 ms | 3.60 ms |

Percentile buckets have 10-microsecond resolution and are reported at their
upper edge. No samples reached the 100-ms overflow bucket in these runs.
Mean durations cover driver send, reply read/parse and thread scheduling,
not internal fsync alone.

**Interpretation:** synchronous persistence adds a clear cost without our
clients: one-connection SET mean increases roughly 11x for both backends.
Under 16 connections, Redis handles this workload better with persistence
on (about 10k versus 6k ops/s, and lower p99). Durability therefore matters
independently of the C# client, and Ruvio's concurrent durable execution
deserves investigation. However, neither backend exhibited seconds-scale
tails here. This short, direct GET/SET probe does not explain the earlier
IDEM/LIMIT/HTTP long tail or establish that client behavior is irrelevant.

**Limitations:** the Python driver parses replies in Python and uses threads
in one process; its CPU/GIL overhead likely limits the memory-mode results,
which do not improve with 16 connections. These numbers are not maximum
server throughput or comparable to the .NET local load-generator capacity.
The driver shares the SUT's CPU, targets Docker bridge rather than true
loopback, and has no separate generator CPU profile. Conditions were not
order-reversed or repeated, data state differs, and sampled tails cover only
30 seconds per concurrency profile. This measures the observed effect of
durability on a matched request/reply path, not an isolated WAL implementation
benchmark. Redis AOF and Ruvio WAL have different write/sync scheduling.

The native-feature comparison already uses Ruvio IDEM/LIMIT directly rather
than translating them to Lua. Redis Lua remains its semantic equivalent.
Testing the real Ruvio integration packages on `/api/*`, with a matching
Redis implementation, remains separate work from this GET/SET probe.

## Native-command durability dispatch fix and two-minute Linux verification

### Confirmed defect

The evented server had two durability paths. Hot GET/SET batches called
`WalSink::begin_group/end_group`, yielded to let other ready clients publish
their mutations, and called `sync_pending` before flushing responses.
Fallback native IDEM/LIMIT commands did not enter this group. Their mutation
appends could call `sync_open_segment` directly, performing `sync_data` while
executing on the shard's event-loop thread. Each mutation could therefore
block other connections without the existing cooperative group-sync point.

**Earlier WAL metric interpretation was incomplete:** the runtime's dirty
sync counter/timer covered `sync_pending`, not inline `sync_open_segment`
calls. Thus the previously reported roughly two WAL-sync seconds did not
measure the complete native scenario's sync cost. Those observations must
not be used to rule out persistence as a contributor to the long tail.
Fallback command-processing timers included that inline sync time.

The fix places standalone native LIMIT, IDEM BEGIN and IDEM COMPLETE in
the same group/barrier as hot writes. It does not change the file sync API,
durability profile, WAL encoding or retry semantics. Successful responses
still wait for the durable barrier. Session/transaction/blocking and other
fallback commands retain their previous boundary; groups are not carried
across their execution. This introduces no per-command heap allocation,
thread, timer or sleep.

### Independent native before/after

Evidence: before,
after.
Same independent Python RESP driver on the SUT, API stopped, `always`,
30 seconds each at one and 16 connections. The test alternated IDEM BEGIN
and LIMIT; BEGIN acquired the per-worker key once then returned pending,
while LIMIT mutated each time. This is not a full create/complete/replay
idempotency workload. The value/GET warmup remained as in the earlier driver.
The pre-fix baseline was captured before deploying the server fix.

| Connections | Mixed ops/s before / after | BEGIN mean before / after | LIMIT mean before / after | LIMIT p99 before / after |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1,420 / 2,176 | 0.175 / 0.120 ms | 1.211 / 0.782 ms | 4.49 / 1.97 ms |
| 16 | 1,816 / 6,544 | 8.294 / 2.415 ms | 9.304 / 2.463 ms | 32.49 / 10.30 ms |

The 16-connection native improvement is approximately 3.6x throughput.
It confirms a server-side concurrency issue independently of our client.
The earlier Python/GIL, shared CPU, sequential order and short-run limitations
still apply; these are not maximum-capacity figures.

### New client + durable server, 120-second scenario

Evidence: k6 summary.
Only the server image changed for this verification; the adaptive C# client
was unchanged. Same hosts, 200 iterations/s, 2,000 preallocated VUs,
40/30/30 scenario mix and full telemetry, with persistence `always`.
The run started at 23:06:37 UTC and lasted two minutes.

24,001 iterations and 31,150 HTTP requests completed; all checks passed,
no failed HTTP requests, dropped or interrupted iterations, peak four active
VUs. HTTP mean fell from the earlier new-client persistent run's 25.86 ms
to **2.78 ms**, median 2.12 ms, p95 5.59 ms, maximum 211 ms.
Different duration, data state and run order prevent a controlled attribution
of every percentile, but the previous seconds-scale tail was not observed.

| Operation | Pre-fix new-client mean | Fixed mean | Earlier Redis AOF-always mean | Fixed p99 | Earlier Redis p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cache GET | 22.59 ms | 0.654 ms | 0.78 ms | 7.47 ms | 13.74 ms |
| Cache SET | 15.71 ms | 1.484 ms | 1.44 ms | 8.06 ms | 13.06 ms |
| IDEM BEGIN | 17.99 ms | 1.235 ms | 1.23 ms | 9.96 ms | 13.44 ms |
| IDEM COMPLETE | 16.66 ms | 1.507 ms | 1.54 ms | 11.20 ms | 16.46 ms |
| LIMIT | 24.90 ms | 1.732 ms | 1.70 ms | 12.37 ms | 14.74 ms |

Redis is still a historical baseline, not a fresh paired run. Means are now
close in this workload, but neither backend's saturation throughput was tested.
Storage histogram export-count and interpolation caveats remain unchanged.

Client queue mean/p99 fell to 0.0082/0.0859 ms; reply-completion mean/p99
to 1.179/9.467 ms. The now-grouped native syncs became visible to the existing
sync metric: approximately 22.6k sync attempts and 22.5 accumulated sync
seconds across the load/trailing scrape window, versus the undercounted
pre-fix native path. This increase is improved coverage, not proof of higher
sync overhead. Server processing accumulated about 0.90 seconds; sync time
now falls outside the native command timer.

### Remaining GET/SET-only concurrency gap

The original 16-connection durable GET/SET result (Ruvio about 6k versus Redis
10k ops/s) is not claimed fixed by this native-only change. GET/SET already
used grouping. A separate traced pre-fix probe showed 18,307 `fdatasync`
calls at one connection versus 8,404 at 16 connections, with 18,613 versus
38,578 `write` calls. This confirms group sharing was occurring; the
concurrent path was not simply performing one sync per SET.
`strace` perturbs execution and its syscall CPU-time summary is not full
storage wait latency. Ruvio still synchronously calls `sync_data` on the
shard runtime thread; changing that safely requires preserving sequence
barriers, segment rotation, error fan-out and bounded pending acknowledgements.
This remaining architectural/performance question needs its own controlled
measurement and is not resolved by the successful native-path fix.

Validation: 412 Rust library tests passed, including a new durable native
pipeline test that verifies one shared sync, durable sequence before
acknowledgement, and completed-result recovery after restart. The fixed
server is deployed, `always` remains enabled, and the API is ready.

## Central grouping policy: fresh paired Linux API comparison

Evidence: k6 summary
and storage/resource metrics.
The central eligibility-rule build was deployed to the SUT; its source SHA-256
matched the local `evented.rs`. Live RESP inspection confirmed the evented
backend, Ruvio `always`, and Redis 7.4.2 AOF enabled with `appendfsync always`.
The API/client image was unchanged. Functional smoke passed all 11 checks.

Run `20261003T232805Z-central-group` used the existing `/bench/*` API workload:
200 iterations/s, 120 seconds per backend, Ruvio first, a 30-second gap, then
Redis; 2,000 preallocated and 2,500 maximum VUs, full telemetry. The random
40/30/30 cache/idempotency/rate-limit mix was unchanged. This covers five
storage operations: GET, SET PX, IDEM BEGIN, IDEM COMPLETE and LIMIT; Redis
uses GET/SET and the existing semantic Lua equivalents. It is not an exhaustive
command catalogue, production middleware parity test, or saturation benchmark.
No extra commands were added because this workload directly exercises the
native grouping change. INCR/DEL and additional reviewed fallback commands
remain necessary for the broader persistence-worker benchmark matrix.

Both stages completed 24,001 iterations at the offered rate. All 48,002
functional checks passed; 62,372 HTTP requests completed without failed
requests, dropped or interrupted iterations. Ruvio issued 31,097 HTTP requests,
Redis 31,275; their random workload mixes therefore differ slightly. Peak
active VUs across the combined run were 14, not the preallocated capacity.
The aggregate 177.8 iterations/s includes the idle gap and must not be
interpreted as either backend's achieved stage throughput.

| HTTP latency | Ruvio | Redis |
| --- | ---: | ---: |
| Mean | 3.221 ms | 2.608 ms |
| Median | 1.982 ms | 1.956 ms |
| p95 | 6.206 ms | 5.180 ms |
| Maximum | 186.420 ms | 79.988 ms |

### Awaited storage latency

| Operation | Ruvio mean | Redis mean | Ruvio estimated p99 | Redis estimated p99 |
| --- | ---: | ---: | ---: | ---: |
| Cache GET | 0.941 ms | 0.678 ms | 19.94 ms | 10.39 ms |
| Cache SET | 2.048 ms | 1.440 ms | 33.40 ms | 16.70 ms |
| IDEM BEGIN / Redis equivalent | 1.499 ms | 1.177 ms | 22.22 ms | 10.70 ms |
| IDEM COMPLETE / Redis equivalent | 1.883 ms | 1.464 ms | 28.01 ms | 13.34 ms |
| LIMIT / Redis equivalent | 2.055 ms | 1.664 ms | 24.97 ms | 12.84 ms |

These are adapter/client await durations, not isolated server-command times.
The run-labelled histogram samples still do not fully reconcile with k6:
Ruvio BEGIN/COMPLETE counts were 14,192/7,096, while Redis exported
12,462/6,231 despite more idempotency HTTP requests. The telemetry export
coverage issue remains open; treat means/percentiles as exported-sample
observations, not complete request accounting. Estimated p99 is interpolated
from buckets and is distinct from the exact k6 HTTP p95.

### Resource observations and interpretation

Approximate 120-second per-stage scrape windows showed API peak working set
400.0 MiB during Ruvio and 393.7 MiB during Redis; cumulative managed allocation
increases were approximately 427.5/311.9 MiB and GC duration 0.098/0.035 seconds.
API CPU deltas were 43.34/48.68 seconds. Both stages share a long-lived API
process, retained heap, background exporters and previous test data; these
values are not isolated backend memory costs or proof of a memory regression.
Redis server RSS was not captured. Ruvio sampled RSS during its stage was
20.7 MiB. Window offsets are approximate relative to k6 startup and `increase`
extrapolates counters between scrapes.

During the Ruvio stage approximately 21.8k dirty syncs accumulated 21.58 seconds
of sync duration. Native syncs now pass through the measured group barrier;
this must not be compared with the earlier undercounted inline-sync metric.

The severe pre-fix tail did not return: HTTP mean is 3.22 ms versus the
historical pre-grouping new-client 25.86 ms, with no seconds-scale samples.
The central rule itself preserves the previously deployed native grouping
scope; this run does not establish an additional speedup from centralizing it.
The preceding grouped-only run's mean was 2.78 ms, so this run is not a further
latency improvement. Sequential run order, cold/warm state, accumulated data,
random mix and VPS scheduling prevent causal attribution of that difference.
Fresh Redis was faster in this run: Ruvio's mean was **23.5% higher than
Redis's**; storage tails
also favored Redis. Neither service's maximum throughput was measured.

The bounded persistence-worker/sequence-barrier prototype is now an unchecked
item in [the roadmap](ROADMAP.md#durable-log), with ownership, failure,
observed-read durability, memory bounds and acceptance gates. It was not
implemented here. Both durable services remain running and API readiness
returned `ready`/`PONG`; named data volumes were preserved.

## Fresh paired memory-mode API comparison

Evidence: k6 summary
and storage/resource metrics.
The same central-rule server and adaptive client were used. Ruvio was recreated
with `memory`, Redis with AOF disabled; Redis periodic RDB saves were also
disabled using `CONFIG SET save ""`. Live RESP verification confirmed these
settings and no active Redis background save/rewrite before load. The API was
restarted, and the functional smoke passed. No data volumes were deleted.

Run `20261003T233848Z-central-memory` kept the previous comparison's
200 iterations/s, 120 seconds per backend, Ruvio-first order, 30-second gap,
2,000 preallocated/2,500 maximum VUs, random scenario mix and full telemetry.
Each backend completed 24,001 iterations. All 48,002 checks passed; 62,454
HTTP requests completed (31,218 Ruvio, 31,236 Redis), with no failed requests,
dropped or interrupted iterations. Peak active VUs across the run were four.

| HTTP latency | Ruvio memory | Redis without persistence |
| --- | ---: | ---: |
| Mean | 1.812 ms | 1.581 ms |
| Median | 1.432 ms | 1.389 ms |
| p95 | 3.414 ms | 2.676 ms |
| Maximum | 50.725 ms | 29.326 ms |

| Awaited storage operation | Ruvio mean | Redis mean | Ruvio estimated p99 | Redis estimated p99 |
| --- | ---: | ---: | ---: | ---: |
| Cache GET | 0.463 ms | 0.363 ms | 2.168 ms | 1.342 ms |
| Cache SET | 0.326 ms | 0.252 ms | 1.202 ms | 1.342 ms |
| IDEM BEGIN / Redis equivalent | 0.463 ms | 0.379 ms | 2.417 ms | 1.378 ms |
| IDEM COMPLETE / Redis equivalent | 0.364 ms | 0.285 ms | 1.861 ms | 1.131 ms |
| LIMIT / Redis equivalent | 0.506 ms | 0.405 ms | 2.547 ms | 1.375 ms |

Ruvio performed zero measured dirty WAL syncs in both stage windows. Its HTTP
mean was 14.6% higher than Redis's (an absolute difference of 0.231 ms).
Relative to the preceding durable comparison, mean HTTP latency fell about
43.7% for Ruvio and 39.4% for Redis. These are sequential short-run observations,
not controlled attribution of every difference to persistence alone.

Both backends' storage means are sub-millisecond, but Redis remains faster on
all exported mean durations. Removing durability does not eliminate the gap,
so a future WAL optimization cannot be assumed to resolve all remaining
differences. Client/adapter encoding, scheduling, transport and server execution
remain potential contributors; this HTTP test cannot distinguish them.
Ruvio native IDEM/LIMIT versus Redis Lua remains the comparison contract.
This is not a maximum-throughput test or a pure server benchmark.

Approximate stage scrape windows showed API peak working set 325.0 MiB during
Ruvio and 360.2 MiB during Redis; allocation deltas were approximately
432.0/311.2 MiB, GC duration 0.085/0.014 seconds, API CPU 43.58/46.68 seconds.
Ruvio sampled RSS during its active stage was 25.1 MiB; Redis server RSS was
not captured. Shared process retained heap and exporter overhead prevent
interpreting API working set as isolated backend memory usage. Counter
extrapolation, approximate stage windows, histogram interpolation and
incomplete telemetry sample accounting still apply.

After measurement both servers were recreated with synchronous durability:
live verification confirmed Ruvio `always`, Redis AOF enabled with
`appendfsync always`, and Redis's default snapshot schedule restored. The API
was restarted and returned `ready`/`PONG`. Memory-only writes from this fixture
have no persistence guarantee and were not retained as durable test data.

## Experimental sync worker: local repeated durability probe

Evidence: raw results.
The opt-in sequence-barrier worker is a local, uncommitted prototype; default
behavior remains unchanged. This experiment compared the same release binary
with the option disabled/enabled, not different client versions.

A standalone Python RESP driver connected over macOS loopback without any
Ruvio SDK, HTTP, middleware, proxy or pipeline. Four fresh server/data-directory
runs used disabled/enabled/enabled/disabled order. Each case measured eight
seconds after 100 GET warmups per connection, at one and 16 connections:
alternating GET/SET with a changing 256-byte value, then native BEGIN/LIMIT.
BEGIN acquired once per worker then returned pending; LIMIT mutated every
time. Every reply was validated. This native workload is not full idempotency
create/complete/replay. Temporary servers were stopped and their test-only
directories removed; the user's checkout config was not used.

| Workload / connections | Default mixed ops/s, repeat range | Worker mixed ops/s, repeat range |
| --- | ---: | ---: |
| GET/SET, 1 | 498-503 | 496-499 |
| GET/SET, 16 | 1,364-1,399 | 1,547-1,564 |
| BEGIN/LIMIT, 1 | 496-496 | 491-496 |
| BEGIN/LIMIT, 16 | 1,352-1,352 | 1,482-1,508 |

Average per-run throughput increased about 12.6% for 16-connection GET/SET
and 10.6% for 16-connection native work, with no clear single-connection gain.
**Concurrent tails regressed:** SET p99 was 13.80-14.16 ms by default versus
19.97-20.14 ms with the worker; LIMIT p99 was 13.95-15.09 versus
19.91-20.17 ms. These are per-run percentile ranges, not pooled percentiles.
No samples overflowed the driver's 100-ms bucket. Sampled final server RSS
was 9,968 KiB by default versus 10,016 KiB with the worker, not a peak-memory
or allocation measurement.

The local result is a tradeoff, **not a successful performance gate**. More
throughput is insufficient when p99 worsens materially. macOS filesystem/sync
semantics differ from Linux; Python thread/GIL and histogram overhead, shared
host scheduling, short windows and the small workload limit interpretation.
No Redis baseline was included. The option stays disabled while an isolated
Linux comparison checks whether this tail behavior reproduces; the live
scenario API and its durable backend configuration are not changed for that
experiment.

## Experimental sync worker: isolated repeated Linux probe

Evidence: raw results.
The prototype was built as a separate `ruvio-sync-prototype` Docker image on
the Linux SUT. The running scenario API, Ruvio and Redis containers were not
recreated or reconfigured. Four temporary containers used fresh bind-mounted
test-only data directories, no published ports, and disabled/enabled/enabled/
disabled order. Each exited after measurement; its temporary data was removed.
API readiness remained `ready`/`PONG` afterwards.

The same Python driver and eight-second cases were used over the Docker bridge,
without SDKs, HTTP, middleware or pipelining. Live INFO/CONFIG checks confirmed
`always` and the intended worker option for every container. The image's
periodic snapshot interval was set to one hour. Every reply was validated;
there were no 100-ms histogram overflow samples.

| Workload / connections | Default mixed ops/s, repeat range | Worker mixed ops/s, repeat range |
| --- | ---: | ---: |
| GET/SET, 1 | 1,253-1,784 | 1,451-2,226 |
| GET/SET, 16 | 5,456-5,800 | 10,048-10,804 |
| BEGIN/LIMIT, 1 | 1,926-1,996 | 2,161-2,210 |
| BEGIN/LIMIT, 16 | 5,971-6,652 | 9,686-12,677 |

| Concurrent mutating operation, 16 connections | Default mean range | Worker mean range | Default p99 range | Worker p99 range |
| --- | ---: | ---: | ---: | ---: |
| SET | 2.786-2.955 ms | 1.523-1.638 ms | 6.20-7.16 ms | 3.54-4.25 ms |
| LIMIT | 2.431-2.702 ms | 1.289-1.697 ms | 4.95-5.92 ms | 2.94-4.17 ms |

Average per-run mixed throughput increased approximately 85% for concurrent
GET/SET and 77% for concurrent native operations. Unlike the macOS probe, the
Linux concurrent p99 improved in both repeats. Single-connection GET/SET
varied widely even across default repeats, so no precise single-client
improvement claim is justified. The native single-connection samples favored
the worker but still need longer repeated runs.

Sampled final server RSS ranged 13,012-13,324 KiB by default and
12,928-13,080 KiB with the worker; this does not establish a memory saving or
measure peak RSS/allocations. No driver CPU, server CPU or syscall profile was
captured. The driver shared SUT CPU with idle lab services, and Python/GIL
overhead can cap concurrent throughput. Eight-second windows, VPS scheduling,
fresh data, bridge networking and test order limit generalization.
No fresh Redis or real API/client comparison was included: these numbers must
not be substituted for the earlier paired HTTP results or used as a capacity
claim.

**Interpretation:** the isolated Linux result supports continuing the
sequence-barrier prototype and suggests that offloading sync can reduce the
remaining concurrent GET/SET cost. It does not complete the broader
persistence-worker work: append, fallback, rotation/checkpoint and final drain
still execute synchronously, and replication/hot-store use is rejected.
The macOS tail regression remains a cross-platform release concern rather
than being dismissed by the Linux result. Full API comparisons, longer matched
Linux trials, stalled-disk and concurrency-model gates remain open.
The worker option stays disabled by default and the live lab's original
durable images remain in use. Prototype code and these new results have not
been committed or pushed.

## Worker-enabled Ruvio versus Redis: direct and remote-client comparisons

Evidence: direct SUT results
and remote .NET client results.
Two separate authenticated test containers ran on the same SUT: the current
worker-enabled prototype in `always` and Redis 7.4.2 with AOF enabled,
`appendfsync always`, periodic snapshots disabled. The prototype snapshot
interval was one hour. Live INFO/CONFIG checks verified the settings.
These are isolated test services, not the live API's existing containers.

Temporary published ports were password-protected and admitted only the
observer's IP through scoped ingress rules. The direct phase ran first and
completed before the remote phase; they did not compete with each other.
Both used Ruvio/Redis/Redis/Ruvio order and 15-second measured cases, one and
16 concurrent workers, alternating GET/SET or BEGIN/LIMIT. SET changed its
256-byte value on every operation. Native BEGIN used a fixed per-worker
fingerprint/owner and mostly returned pending after its first acquisition;
LIMIT mutated each time. Redis Lua provided equivalent atomic state checks
and limit transitions. This is not full idempotency create/complete/replay.
Every response was validated; no samples reached the 100-ms overflow bucket.

### Direct on the SUT, excluding SDKs and network between hosts

A standalone Python RESP driver connected to each container's Docker bridge
address. Each worker had its own socket, with AUTH once and 100 GET warmups.
There was no SDK, HTTP, middleware, proxy or explicit pipeline. Redis native
equivalents used EVAL with the script source per call; this includes Lua/frame
overhead and is a semantic-feature comparison, not equal command encoding.

| Mixed workload / connections | Ruvio worker ops/s, repeat range | Redis ops/s, repeat range |
| --- | ---: | ---: |
| GET/SET, 1 | 1,684-2,030 | 1,829-2,111 |
| GET/SET, 16 | 7,345-7,874 | 6,033-6,843 |
| BEGIN/LIMIT, 1 | 1,863-1,957 | 1,221-1,778 |
| BEGIN/LIMIT, 16 | 10,031-10,531 | 7,114-7,643 |

| Mutating operation, 16 connections | Ruvio mean range | Redis mean range | Ruvio p99 range | Redis p99 range |
| --- | ---: | ---: | ---: | ---: |
| SET | 2.094-2.253 ms | 2.339-2.658 ms | 8.76-10.89 ms | 11.23-20.74 ms |
| LIMIT | 1.562-1.642 ms | 2.092-2.243 ms | 3.80-3.94 ms | 4.95-5.56 ms |

Ruvio led both 16-socket direct workloads in both repeats; Redis led
single-socket GET/SET. Sampled server RSS after each backend's cases was
13,172-13,228 KiB for Ruvio and 9,124-9,180 KiB for Redis. This is not peak
memory, equal retained data size, or allocation accounting: backend-native
records, AOF/WAL buffers and temporary keys differ.

### Observer to SUT through actual .NET client libraries

The new benchmark mode `--paired-socket-load` ran on the separate observer
<observer-IP>, directly against test ports on SUT <SUT-IP>. It used the
current adaptive Ruvio client and StackExchange.Redis 2.8.31, one shared
multiplexed connection per backend/case with one or 16 callers. Both permitted
automatic multiplexing; neither explicitly pipelined a command batch.
This differs deliberately from the direct test's 16 separate sockets.

Each case warmed the same keys/workload for two seconds, then measured
15 seconds. Backend connection/authentication and forced GC were outside the
measurement. There was no HTTP, middleware, Toxiproxy or tracing collector in
the path. Ruvio sent native IDEM/LIMIT; Redis used ScriptEvaluateAsync. The
benchmark targets net8.0 but the observer had only .NET 10 installed, so it
ran with explicit major-version roll-forward on .NET 10.0.12 for both clients.
Do not directly compare its allocation/runtime behavior with earlier .NET 8
API runs.

| Mixed workload / callers | Ruvio worker ops/s, repeat range | Redis ops/s, repeat range |
| --- | ---: | ---: |
| GET/SET, 1 | 1,148-1,186 | 881-938 |
| GET/SET, 16 | 7,437-9,919 | 9,752-10,989 |
| BEGIN/LIMIT, 1 | 1,171-1,275 | 1,124-1,181 |
| BEGIN/LIMIT, 16 | 7,563-10,289 | 7,914-9,730 |

| Mutating operation, 16 callers | Ruvio mean range | Redis mean range | Ruvio p99 range | Redis p99 range |
| --- | ---: | ---: | ---: | ---: |
| SET | 1.612-2.156 ms | 1.463-1.645 ms | 3.559-8.883 ms | 3.053-3.952 ms |
| LIMIT | 1.560-2.124 ms | 1.648-2.027 ms | 3.302-9.985 ms | 3.487-12.999 ms |

Redis led concurrent remote GET/SET in the average of repeats, while native
remote throughput was close and variable: the ranges overlap substantially.
Ruvio led single-caller remote GET/SET in this run, but the opposite direct
single-socket result illustrates that SDK/network/scheduling matters.
These short observations are not a universal backend ranking.

Managed allocation per mixed operation at 16 callers was approximately
1,991-1,992 bytes for Ruvio GET/SET versus 966 for Redis; native operations
were 1,511-1,512 versus 875 bytes. This includes the benchmark, Task/response
objects, command encoding and client internals; it is not server memory.
The measured network path therefore still has a meaningful Ruvio
allocation gap. Client process working set is retained across cases and
cannot be assigned exclusively to a backend.

### Interpretation and limitations

The worker now has a fresh Redis reference on both paths: direct concurrent
execution favored Ruvio, but remote shared-socket GET/SET still favored Redis.
This does not prove the remaining gap is exclusively in our client:
connection topology, network round trips, server batch formation, response
handling and driver scheduling differ. Future isolation should match
connection/pipeline topology and repeat longer client tests with CPU/allocation
profiles. No real API or full middleware comparison was performed here.

Python/GIL overhead can cap direct throughput; both drivers share their
respective hosts with idle lab services. Fifteen-second windows, VPS disk/
CPU scheduling, accumulated temporary keys and sequential order create
variation. Percentiles are per-run ranges, not pooled values. No isolated
fsync-cost or saturation-capacity claim is made.

The test containers were stopped/removed, temporary ingress rules and test
credentials removed, and live lab readiness verified. The original live
Ruvio remained in `always` with its default worker-disabled image; Redis
remained AOF-always. The prototype is still opt-in, local/uncommitted, and
not promoted based on these results.

## C# client allocation reduction: local memory isolation (2026-10-04)

An isolated loopback Ruvio memory server on port 16403 removed disk and
remote-network costs. The existing `--socket-load 16403 3` runner measured
GET, SET and native LIMIT at 1/2/4/16/128 callers, with one-second warmup
and three-second measurement per case. Both builds used the same local
.NET 8 runtime, Release configuration and generic `ExecuteAsync` harness.
The rebuilt baseline used the old parser/single-command routes with the
same Request field layout; the optimized build was measured twice.
No live lab server or durability configuration was changed.

Avoidable allocations identified and removed:

- Buffered scalar replies now return an internal `ValueTask` directly.
  Fragmented replies and arrays retain the asynchronous reader path, whose
  generic helpers also use `ValueTask` rather than allocating completed Tasks.
- Single string commands no longer allocate an outer one-command array.
  Cancellation still snapshots argument data before publication, and replies
  from abandoned callers still drain in FIFO order.
- Public Task APIs and owned bulk-response byte arrays remain unchanged.
  Request/TCS objects are deliberately not pooled: cancellation and reader/
  writer completion make premature reuse unsafe.

The fragmented-reader regression also exposed an existing CR/LF boundary
bug: when the current buffer ended in CR, the reader requested two more bytes
instead of only the missing LF. That could wait past the end of a valid reply.
The reader now requests exactly one more byte.

| Operation / 16 callers | Before bytes/op | After bytes/op, repeat range | Reduction | Before ops/s | After ops/s, repeat range |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 1,641 | 1,385-1,386 | 15.5-15.6% | 185,745 | 178,190-181,167 |
| SET | 1,324 | 1,139-1,144 | 13.6-14.0% | 171,134 | 173,116-175,378 |
| LIMIT | 1,291 | 1,105-1,110 | 14.0-14.4% | 185,096 | 175,950-178,860 |

At 128 callers the allocation reduction was approximately 15-17%. At one
caller GET fell from 1,840 to 1,768 bytes/op, while SET/LIMIT were effectively
unchanged: asynchronous socket suspension still needs completion/state
objects. A buffered-reader allocation regression checks a 256-byte bulk
reply stays below 400 bytes including its owned payload/RespValue, in both
Debug and Release. Fragmented nested arrays, malformed/truncated replies,
retry classification and response-buffer ownership are covered alongside
the existing FIFO/cancellation suite; all 127 client tests passed.

**Allocation improved reproducibly; throughput did not improve uniformly.**
Concurrent GET/LIMIT were slightly slower in these short runs, SET slightly
faster, and single-caller SET varied substantially. This is not evidence
that the allocation gap caused the earlier remote WAL latency gap, nor a
new Redis comparison. Process allocation includes harness and SDK objects.
Longer matched remote durable comparisons and allocation/CPU profiles are
still required before making a performance or causality claim.

Raw evidence:
`20261004-client-allocation-before.json`,
`20261004-client-allocation-after.json`,
`20261004-client-allocation-after-repeat.json`.

## Linux durable client allocation retest (2026-10-04)

The optimized C# client was rebuilt and run on Linux observer <observer-IP>
against isolated services on SUT <SUT-IP>: unchanged Ruvio sync-worker
image, `always`, and Redis 7.4.2 AOF/`appendfsync always`. Both clients ran
on .NET 10.0.12 with explicit net8 major-version roll-forward. The preserved
pre-optimization source was also rebuilt on the same observer and tested
against the same services. No server code was changed for this comparison.

Three complete paired runs were performed: optimized, baseline, optimized
repeat. Each run used Ruvio/Redis/Redis/Ruvio ordering, two-second warmup,
15-second cases, one shared connection with 1/16 callers, native Ruvio
IDEM/LIMIT versus Redis Lua, changing 256-byte SET values, and validated
replies. This is a remote Linux SDK/socket comparison, not an HTTP middleware
test or a new direct server-only benchmark.

| Mixed workload / 16 callers | Baseline Ruvio bytes/op | Optimized Ruvio bytes/op | Reduction | Redis bytes/op |
| --- | ---: | ---: | ---: | ---: |
| GET/SET | 1,992 | 1,774-1,775 | 10.9% | 966 |
| BEGIN/LIMIT | 1,511 | 1,148-1,150 | 23.9-24.0% | 875 |

The allocation reduction is stable across repeats on Linux, but a significant
gap to Redis remains. Managed bytes include the benchmark, SDK Tasks,
response conversion and client objects; they are not network bytes.

| Mixed workload / 16 callers | Baseline Ruvio ops/s | First optimized ops/s | Optimized repeat ops/s | Redis range across all runs |
| --- | ---: | ---: | ---: | ---: |
| GET/SET | 8,879-9,954 | 4,359-9,762 | 9,894-10,262 | 8,113-10,806 |
| BEGIN/LIMIT | 9,716-9,792 | 7,862-9,310 | 6,120-10,667 | 7,121-10,221 |

In the optimized repeat's 16-caller GET/SET cases, Ruvio SET mean was
1.559-1.618 ms/p99 3.480-3.562 ms versus Redis mean 1.487-1.571 ms/p99
3.142-3.363 ms. However the first optimized Ruvio GET/SET case had SET
p99 21.906 ms; another optimized native case had LIMIT p99 18.564 ms.
Redis also varied significantly, including native throughput 7,121-10,221
ops/s in the repeat. One >=100-ms sample occurred across the repeat run;
the first optimized and baseline runs had none.

**Linux confirms less allocation, not a proven throughput or tail-latency
improvement.** Short sequential VPS cases have substantial time-dependent
variation; the baseline was run between optimized runs rather than each
version interleaved per case. Neither averaging away slow cases nor selecting
only the fast repeat establishes a causal gain. The remaining latency gap
still needs longer matched profiling and client/server scheduling isolation.

Isolated containers, test credentials, disposable datasets and temporary
observer-only ingress rules were removed. Live lab Ruvio/Redis retained their
original always/AOF-always settings. Raw JSON remains locally under
`20261004-linux-client-allocation-{before,after,after-repeat}.json`;
`docs/results/` is now ignored and previously tracked raw outputs are removed
from the Git index without deleting local files. This report is the
version-controlled summary.

## C# client pooled ValueTask migration: local memory isolation (2026-10-04)

The client was changed in three measured steps: a per-type allocation
profile, a simpler single-command path, and then pooled `ValueTask` results.
In the last step, the pending command is a reusable `IValueTaskSource`
returned to a pool after both the caller and the pipeline release it.
Async helpers use `PoolingAsyncValueTaskMethodBuilder`. Fixed argument lists
are plain arrays, because a collection literal typed as `IReadOnlyList<T>`
allocates an extra wrapper.

Setup: one MacBook, a release Ruvio memory-mode server on loopback, and
`Ruvio.Client.Benchmarks --allocation-profile 16403 5 16` with 16 callers.
The table shows allocated bytes per operation.

| Workload | Before | Simpler path | Pooled ValueTask |
|---|---:|---:|---:|
| get | 1397 | 1184 | **339** |
| get (typed) | 1303 | 1368 | **379** |
| set | 1151 | 939 | **89** |
| set (typed) | 1399 | 1131 | **137** |
| limit | 1116 | 901 | **57** |

`--socket-load 16403 3`, first run, compared with the earlier
allocation-optimized client (ops/s, B/op):

| Case | Earlier | Pooled ValueTask |
|---|---|---|
| get c=16 | 181,167 / 1385 | 177,150 / 336 |
| get c=128 | 254,906 / 1361 | 284,394 / 336 |
| set c=16 | 173,116 / 1144 | 173,604 / 88 |
| set c=128 | 235,938 / 1118 | 261,565 / 88 |
| limit c=16 | 178,860 / 1105 | 179,904 / 56 |
| limit c=128 | 241,100 / 1082 | 292,047 / 56 |

Gen0 collections fell by 4–19×, and p99 at c=128 fell from about 0.92–1.0 ms
to 0.70–0.74 ms. Throughput at c=1–4 varied widely from run to run on this
host. For example, set at c=1 gave 23k, 37k and 38k ops/s across three
repeats, against an earlier 34k. Low-concurrency differences are therefore
not taken as a change. With no Redis server running locally, this is a
client-cost isolation run, not a Redis comparison.

What remains per `GET` is mostly the reply `byte[]`, plus the `RespValue`
class instance. Typed wrappers also allocate a `string[]` per call. Making
`RespValue` a struct and removing the wrapper arrays are the next options.
Both would change the API.

Follow-up, same setup: null, `+OK`, `+PONG`, `+QUEUED`, the empty bulk
string, and integers from -2 to 255 are now shared immutable `RespValue`
instances. The other values are unchanged.

| Workload | Pooled ValueTask | Shared replies |
|---|---:|---:|
| set | 89 | **8** |
| set (typed) | 137 | **56** |
| get / limit | 339 / 57 | 339 / 57 |

The `limit` probe stays at 56 B because its counter, limited to 10^9, goes
past 255. Rate limits with small values get the shared integers.

Tests: the client suite (130, including saturation-time cancellation,
double-await and `WhenAll` via `AsTask` tests) and every .NET integration
suite against a live server: AspNetCore 11, DataProtection 10,
Idempotency 68, OutputCaching 12, RateLimiting 11, Caching 19.

Follow-up, same setup: typed commands store up to five arguments inline in
a `RespCommand` struct, with no `string[]`. Integers are encoded straight
into the send buffer, and `+OK`-style replies are matched from bytes even
on the slow read path.

| Workload | Shared replies | Inline commands |
|---|---:|---:|
| set | 8 | **0** |
| set (typed) | 56 | **0** |
| get (typed) | 379 | **339** |
| get / limit | 339 / 57 | 338 / 57 |

`GET` keeps the 336 B reply `byte[]` (256-byte value plus array header).
Removing it would need a pooled, caller-released buffer API.

Tests: client 139, AspNetCore 11, DataProtection 10, Idempotency 68,
OutputCaching 12, RateLimiting 11, Caching 19.

Follow-up: the remaining per-request allocations found in the audit were
removed without changing any stored format or key. Equivalence tests pin
each hashed key and fingerprint to the old algorithm.

| Area | Change |
|---|---|
| Client commands | LINQ, closures, and `IntegerFrame` replaced by exact-size arrays |
| Cluster router | single-key fast path from the inline command; per-node socket cache per topology |
| Rate limiting | partition key hashed from a stack buffer; only the key string is allocated |
| Idempotency | key, fingerprint, and owner from stack buffers; no per-part UTF-8 arrays, LINQ, or delegates |
| Distributed cache | key and record header encoded straight into their final arrays |
| Output cache | keys built once, no rehash or substrings in the record, no delegates |
| Data protection | storage key name cached at construction |

Client profile is unchanged (set 0 B, get 338 B, limit 57 B); these paths
sit above it. Tests: client 142, AspNetCore 11, DataProtection 10,
Idempotency 71, OutputCaching 14, RateLimiting 15, Caching 19.
