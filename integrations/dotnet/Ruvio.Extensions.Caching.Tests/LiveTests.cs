using System.Globalization;
using System.Text;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Caching.Tests;

public class LiveTests
{
    [LiveFact]
    public async Task Binary_empty_namespace_isolation_and_sync_contract()
    {
        await using var scope = await Scope.Open();
        byte[] allBytes = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();

        scope.Cache.Set("", allBytes, new());
        Assert.Equal(allBytes, scope.Cache.Get(""));
        scope.Cache.Refresh("");
        Assert.Equal(-1, await scope.Ttl(""));
        await scope.Cache.SetAsync("empty", [], new());
        Assert.Empty((await scope.Cache.GetAsync("empty"))!);
        Assert.Null(await scope.Cache.GetAsync("missing"));
        var other = new RuvioDistributedCache(scope.Client,
            Options.Create(new RuvioCacheOptions { InstanceName = scope.Namespace + "-other" }));

        Assert.Null(await other.GetAsync(""));
        scope.Cache.Remove("");
        Assert.Null(scope.Cache.Get(""));
    }

    [LiveFact]
    public async Task GetOrCreate_runs_the_factory_once_for_concurrent_callers_and_serves_the_stored_value()
    {
        await using var scope = await Scope.Open();
        int builds = 0;
        var options = new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(30) };

        async Task<byte[]> Build(CancellationToken token)
        {
            Interlocked.Increment(ref builds);
            await Task.Delay(300, token);

            return [7, 8, 9];
        }

        byte[][] results = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ =>
            scope.Cache.GetOrCreateAsync("fill", Build, options)));

        Assert.Equal(1, builds);
        Assert.All(results, value => Assert.Equal(new byte[] { 7, 8, 9 }, value));
        Assert.Equal(new byte[] { 7, 8, 9 }, await scope.Cache.GetAsync("fill"));
        Assert.InRange(await scope.Ttl("fill"), 20_000, 30_000);

        // A second call is a plain hit: the factory does not run again.
        await scope.Cache.GetOrCreateAsync("fill", Build, options);
        Assert.Equal(1, builds);

        // Without any expiry the value is stored and then persisted.
        await scope.Cache.GetOrCreateAsync("fill-forever", _ => Task.FromResult(new byte[] { 1 }), new());
        Assert.Equal(-1, await scope.Ttl("fill-forever"));
        Assert.Equal(new byte[] { 1 }, await scope.Cache.GetAsync("fill-forever"));
    }

    [LiveFact]
    public async Task GetOrCreate_waiters_build_for_themselves_when_the_builder_never_finishes()
    {
        await using var scope = await Scope.Open();
        var cache = new RuvioDistributedCache(scope.Client, Options.Create(new RuvioCacheOptions
        {
            InstanceName = scope.Namespace,
            FillTimeout = TimeSpan.FromSeconds(5),
            FillWait = TimeSpan.FromMilliseconds(300),
        }));
        using var stuck = new CancellationTokenSource();
        Task<byte[]> builder = cache.GetOrCreateAsync("fill-wait", async token =>
        {
            await Task.Delay(Timeout.Infinite, token);

            return [];
        }, new(), stuck.Token);

        await Task.Delay(100);
        byte[] value = await cache.GetOrCreateAsync("fill-wait", _ => Task.FromResult(new byte[] { 4 }),
            new() { AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(30) });

        Assert.Equal(new byte[] { 4 }, value);
        stuck.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => builder);
    }

    [LiveFact]
    public async Task Sliding_refresh_and_get_renew_but_absolute_deadline_is_never_extended()
    {
        await using var scope = await Scope.Open();
        await scope.Cache.SetAsync("slide", [1], new() { SlidingExpiration = TimeSpan.FromMilliseconds(600) });

        await Task.Delay(350);
        await scope.Cache.RefreshAsync("slide");
        Assert.InRange(await scope.Ttl("slide"), 400, 600);
        await Task.Delay(350);
        Assert.Equal(new byte[] { 1 }, await scope.Cache.GetAsync("slide"));

        Assert.InRange(await scope.Ttl("slide"), 400, 600);
        await Task.Delay(750);
        Assert.Null(await scope.Cache.GetAsync("slide"));

        long before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        await scope.Cache.SetAsync("cap", [2], new()
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromMilliseconds(900),
            SlidingExpiration = TimeSpan.FromMilliseconds(400),
        });

        long after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        for (int i = 0; i < 6; i++)
        {
            await Task.Delay(100);
            await scope.Cache.RefreshAsync("cap");
            long measuredAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            long ttl = await scope.Ttl("cap");

            Assert.InRange(ttl, 0, 400);
            Assert.True(measuredAt + ttl <= after + 900, "Refresh exceeded the original absolute deadline.");
        }

        await Task.Delay(TimeSpan.FromMilliseconds(Math.Max(1, after + 1000 - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())));
        Assert.Null(await scope.Cache.GetAsync("cap"));
        Assert.True(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > before + 900);
    }

    [LiveFact]
    public async Task Relative_deadline_is_client_based_and_earlier_absolute_option_wins()
    {
        await using var scope = await Scope.Open();
        await scope.Cache.SetAsync("relative", [1], new() { AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(10) });

        string envelope = (await scope.Client.ExecuteAsync("GET", scope.Key("relative"))).AsString()!;
        long deadline = long.Parse(envelope.Split('\n')[1], CultureInfo.InvariantCulture);

        Assert.InRange(deadline - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 9000, 10000);
        var earlier = DateTimeOffset.UtcNow.AddSeconds(2);

        await scope.Cache.SetAsync("earlier", [1], new()
        {
            AbsoluteExpiration = earlier,
            AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(30),
            SlidingExpiration = TimeSpan.FromSeconds(10),
        });

        await scope.Cache.RefreshAsync("earlier");
        Assert.InRange(await scope.Ttl("earlier"), 1, 2000);
        string stored = (await scope.Client.ExecuteAsync("GET", scope.Key("earlier"))).AsString()!;

        Assert.Equal(earlier.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture), stored.Split('\n')[1]);
    }

    [LiveFact]
    public async Task Deadline_boundary_and_missing_refresh_never_resurrect()
    {
        await using var scope = await Scope.Open();

        foreach (long absolute in new[] { 1L, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() })
        {
            byte[] record = Encoding.ASCII.GetBytes($"RDC1\n{absolute.ToString(CultureInfo.InvariantCulture)}\n60000\n")
                .Concat(new byte[] { 255 }).ToArray();

            await scope.Client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[]
            {
                "SET"u8.ToArray(), Encoding.UTF8.GetBytes(scope.Key("boundary")), record,
            });

            await scope.Cache.RefreshAsync("boundary");
            Assert.Null(await scope.Cache.GetAsync("boundary"));
            Assert.Equal(-2, await scope.Ttl("boundary"));
        }

        await scope.Cache.RefreshAsync("missing");
        Assert.Equal(-2, await scope.Ttl("missing"));
    }

    [LiveFact]
    public async Task Concurrent_replacement_and_remove_cannot_receive_stale_sliding_renewals()
    {
        await using var scope = await Scope.Open();
        await using RuvioConnection secondConnection = await Scope.Connect();
        RuvioClient second = await secondConnection.GetDefaultDatabaseAsync();
        var other = new RuvioDistributedCache(second, Options.Create(new RuvioCacheOptions { InstanceName = scope.Namespace }));

        for (int i = 0; i < 40; i++)
        {
            await scope.Cache.SetAsync("race", [0], new()
            {
                SlidingExpiration = TimeSpan.FromSeconds(1),
                AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(2),
            });

            await Task.WhenAll(scope.Cache.GetAsync("race"), other.SetAsync("race", [255, (byte)i], new()));
            Assert.Equal(new byte[] { 255, (byte)i }, await other.GetAsync("race"));

            Assert.Equal(-1, await scope.Ttl("race"));
            await scope.Cache.SetAsync("race", [0], new() { SlidingExpiration = TimeSpan.FromSeconds(1) });

            await Task.WhenAll(scope.Cache.RefreshAsync("race"), other.RemoveAsync("race"));
            Assert.Null(await scope.Cache.GetAsync("race"));
        }
    }

    [LiveFact]
    public async Task Wrong_type_and_corrupt_envelope_fail_loudly()
    {
        await using var scope = await Scope.Open();
        await scope.Client.ExecuteAsync("SET", scope.Key("corrupt"), "not-a-cache-entry");
        await Assert.ThrowsAsync<RuvioException>(() => scope.Cache.GetAsync("corrupt"));
        await Assert.ThrowsAsync<RuvioException>(() => scope.Cache.RefreshAsync("corrupt"));
        await scope.Client.ExecuteAsync("HSET", scope.Key("wrongtype"), "f", "v");
        await Assert.ThrowsAsync<RuvioException>(() => scope.Cache.GetAsync("wrongtype"));
    }

    [LiveFact]
    public async Task Binary_client_routes_non_utf8_keys_without_transcoding()
    {
        await using RuvioConnection connection = await Scope.Connect();
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        byte[] key = Encoding.ASCII.GetBytes("cache-binary-" + Guid.NewGuid().ToString("N"))
            .Concat(new byte[] { 255, 128, 0 }).ToArray();

        byte[] value = [255, 0, 128, 10, 13];

        try
        {
            await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[] { "SET"u8.ToArray(), key, value });

            RespValue result = await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[] { "GET"u8.ToArray(), key });

            Assert.Equal(value, result.Bytes);
        }

        finally
        {
            await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[] { "DEL"u8.ToArray(), key });
        }
    }

    private sealed class Scope : IAsyncDisposable
    {
        private readonly HashSet<string> _keys = [];

        public string Namespace { get; } = "cache-test-" + Guid.NewGuid().ToString("N");

        public RuvioClient Client { get; }
        public RuvioConnection Connection { get; }
        public RuvioDistributedCache Cache { get; }
        private Scope(RuvioConnection connection, RuvioClient client)
        {
            Connection = connection;
            Client = client;
            Cache = new(client, Options.Create(new RuvioCacheOptions { InstanceName = Namespace }));

            foreach (string key in new[] { "", "empty", "slide", "cap", "relative", "earlier", "boundary", "race", "corrupt", "wrongtype", "fill", "fill-forever", "fill-wait" })
                Key(key);
        }

        public string Key(string key)
        {
            string physical = Namespace + RuvioCacheOptions.VersionPrefix + key;

            _keys.Add(physical);

            return physical;
        }

        public async Task<long> Ttl(string key) => (await Client.ExecuteAsync("PTTL", Key(key))).Integer;
        public static async Task<Scope> Open()
        {
            RuvioConnection connection = await Connect();
            return new Scope(connection, await connection.GetDefaultDatabaseAsync());
        }
        public static Task<RuvioConnection> Connect()
        {
            string[] address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")!.Split(':');

            return RuvioConnection.ConnectAsync(address[0], int.Parse(address[1], CultureInfo.InvariantCulture));
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                foreach (string key in _keys)
                    await Client.DeleteAsync(key);
            }

            finally { await Connection.DisposeAsync(); }
        }
    }
}

public sealed class LiveFactAttribute : FactAttribute
{
    public LiveFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")))
            Skip = "Set RUVIO_TEST_ADDR to run against an isolated live Ruvio server.";
        Timeout = 30000;
    }
}
