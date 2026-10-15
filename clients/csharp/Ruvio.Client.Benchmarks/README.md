[Ruvio](../../../README.md) / [C# clients](../README.md) / Benchmarks

# Client benchmarks

The normal entry point runs BenchmarkDotNet. `--socket-load port seconds`
provides the existing single-backend load mode.

## Paired durable socket comparison

```bash
export BENCH_PASSWORD='test-only-storage-password'
dotnet run -c Release --project clients/csharp/Ruvio.Client.Benchmarks -- \
  --paired-socket-load server.example 16391 16392 15 results.json
unset BENCH_PASSWORD
```

Use authenticated, isolated test servers with peer-restricted ingress.
The first port is Ruvio, the second Redis. The benchmark does not change
durability settings: verify Ruvio `always`/worker setting and Redis
AOF/`appendfsync always` before running. Do not target customer data.

The runner alternates Ruvio/Redis/Redis/Ruvio, GET/SET and native BEGIN/LIMIT
workloads, with one and 16 callers sharing a multiplexed connection per case.
Redis uses equivalent Lua through StackExchange.Redis. There is a two-second
warmup and a configurable 5-120 second measurement per case; 15 seconds means
roughly four and a half minutes overall plus setup. SET values change on every
write. BEGIN mostly returns pending; this is not full idempotency lifecycle
coverage. Temporary test keys are unique per run; IDEM/LIMIT keys expire,
but ordinary GET/SET keys remain in the disposable test dataset.

Responses are validated and any unexpected reply aborts the run. JSON is
written after each successful case with throughput, operation latency,
100-ms histogram overflow counts, process allocation/CPU and working set.
The top latency bucket means at least 100 ms, not a bounded maximum.
Connection setup/authentication and forced GC are outside measured intervals.
Allocation includes the harness and both libraries; working set is retained
between cases. Comparisons must use the same runtime and host conditions.
No HTTP, middleware or explicit pipeline is used. Concurrent shared-socket
results are not equivalent to a direct probe with one socket per worker.
