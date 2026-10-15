using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Ruvio.AspNetCore.Idempotency;
using Ruvio.AspNetCore.OutputCaching;
using Ruvio.AspNetCore.RateLimiting;
using Ruvio.Client;
using Ruvio.AspNetCore.DistributedLock;
using Ruvio.Extensions.Caching;

// Live benchmarks against a local server: RUVIO_TEST_ADDR=127.0.0.1:6580 dotnet run -c Release -- --filter '*'
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly)
    .Run(args, DefaultConfig.Instance.AddJob(Job.ShortRun).WithOptions(ConfigOptions.DisableOptimizationsValidator));

[MemoryDiagnoser]
public class IntegrationBenchmarks
{
    private RuvioConnection connection = null!;
    private IRuvioClient client = null!;
    private RuvioDistributedCache cache = null!;
    private RuvioOutputCacheStore store = null!;
    private RuvioLockProvider locks = null!;
    private readonly byte[] small = new byte[64];
    private readonly byte[] large = new byte[4096];
    private readonly DistributedCacheEntryOptions ttl = new() { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5) };
    private long counter;

    [GlobalSetup]
    public async Task Setup()
    {
        string[] address = (Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR") ?? "127.0.0.1:6580").Split(':');
        connection = await RuvioConnection.ConnectAsync(address[0], int.Parse(address[1]));
        client = await connection.GetDefaultDatabaseAsync();
        cache = new RuvioDistributedCache(client, Options.Create(new RuvioCacheOptions()));
        store = new RuvioOutputCacheStore(client, Options.Create(new RuvioOutputCacheOptions()));
        locks = new RuvioLockProvider(client);
        await cache.SetAsync("bench:small", small, ttl);
        await cache.SetAsync("bench:large", large, ttl);
        await store.SetAsync("bench:oc", large, null, TimeSpan.FromMinutes(5), default);
    }

    [GlobalCleanup]
    public async Task Cleanup() => await connection.DisposeAsync();

    [Benchmark] public Task<byte[]?> Cache_Get_64B() => cache.GetAsync("bench:small");
    [Benchmark] public Task<byte[]?> Cache_Get_4KB() => cache.GetAsync("bench:large");
    private static readonly Func<CancellationToken, Task<byte[]>> Factory = _ => Task.FromResult(new byte[64]);
    [Benchmark] public Task<byte[]> Cache_GetOrCreate_Hit() => cache.GetOrCreateAsync("bench:small", Factory, ttl);
    [Benchmark] public async Task<byte[]> Cache_GetOrCreate_Miss() => await cache.GetOrCreateAsync("bench:miss:" + Interlocked.Increment(ref counter), Factory, ttl);

    [Benchmark] public Task Cache_Set_64B() => cache.SetAsync("bench:small", small, ttl);
    [Benchmark] public Task Cache_Set_4KB() => cache.SetAsync("bench:large", large, ttl);
    [Benchmark] public ValueTask<byte[]?> OutputCache_Get_4KB() => store.GetAsync("bench:oc", default);
    [Benchmark] public ValueTask OutputCache_Set_4KB() => store.SetAsync("bench:oc", large, null, TimeSpan.FromMinutes(5), default);
    [Benchmark] public ValueTask OutputCache_Set_Tagged() => store.SetAsync("bench:oct", small, ["tag1"], TimeSpan.FromMinutes(5), default);

    [Benchmark]
    public async Task Lock_AcquireRelease()
    {
        await using IRuvioLock? l = await locks.TryAcquireAsync("bench-resource", TimeSpan.FromSeconds(30));
    }

    [Benchmark] public string RateLimit_KeyOnly() => RuvioRateLimitingMiddleware.StorageKey("rl:", "policy", "198.51.100.7");
    [Benchmark] public ValueTask<long?> RateLimit_Accepted() => client.LimitAsync("bench:rl", 1_000_000_000, 60_000, default);

    [Benchmark]
    public async Task Idempotency_New()
    {
        string key = "bench:idem:" + Interlocked.Increment(ref counter);
        await IdempotencyStore.AcquireAsync(client, key, "fp", "owner", TimeSpan.FromSeconds(30), default);
        await IdempotencyStore.CompleteAsync(client, key, "fp", "owner", "result", default);
    }

    [Benchmark] public Task<(string State, string? Result)> Idempotency_Replay() =>
        IdempotencyStore.AcquireAsync(client, "bench:idem:replay", "fp", "owner", TimeSpan.FromSeconds(30), default);
}
