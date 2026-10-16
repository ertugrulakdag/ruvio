using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Ruvio.Client;
using StackExchange.Redis;

internal static class PairedSocketLoad
{
    private const string BeginScript = """
        local f = redis.call('HGET', KEYS[1], 'fingerprint')
        if not f then
          redis.call('HSET', KEYS[1], 'fingerprint', ARGV[1], 'owner', ARGV[2], 'state', 'acquired')
          redis.call('PEXPIRE', KEYS[1], ARGV[3])
          return {'acquired'}
        end
        if f ~= ARGV[1] then return {'conflict'} end
        if redis.call('HGET', KEYS[1], 'state') == 'completed' then
          return {'completed', redis.call('HGET', KEYS[1], 'result')}
        end
        return {'pending'}
        """;
    private const string LimitScript = """
        local n = tonumber(redis.call('GET', KEYS[1]) or '0')
        local maximum = tonumber(ARGV[1])
        if n >= maximum then return -1 end
        if n == 0 then
          n = 1
          redis.call('SET', KEYS[1], n, 'PX', ARGV[2])
        else n = redis.call('INCR', KEYS[1]) end
        return maximum - n
        """;

    internal static async Task RunAsync(string[] args)
    {
        if (args.Length is not (5 or 6) || (args.Length == 6 && args[5] != "--full"))
            throw new ArgumentException("Expected host ruvio-port redis-port seconds output.json [--full]; BENCH_PASSWORD is required.");

        // Quick runs one ruvio -> redis pass; --full runs the ruvio/redis/redis/ruvio order.
        bool full = args.Length == 6;
        int repeats = full ? 4 : 2;
        string host = args[0];
        int ruvioPort = int.Parse(args[1], CultureInfo.InvariantCulture);
        int redisPort = int.Parse(args[2], CultureInfo.InvariantCulture);
        int seconds = int.Parse(args[3], CultureInfo.InvariantCulture);

        if (seconds is < 5 or > 120)
            throw new ArgumentOutOfRangeException(nameof(args), "Case duration must be 5-120 seconds.");

        string password = Environment.GetEnvironmentVariable("BENCH_PASSWORD")
            ?? throw new InvalidOperationException("BENCH_PASSWORD is required.");
        string run = Guid.NewGuid().ToString("N");
        var results = new List<object>();
        using var process = Process.GetCurrentProcess();

        for (int repeat = 0; repeat < repeats; repeat++)
        {
            string backend = repeat == 0 || repeat == 3 ? "ruvio" : "redis";

            foreach (string profile in new[] { "hot", "native" })
            foreach (int concurrency in new[] { 1, 16 })
            {
                using RuvioConnection? connection = backend == "ruvio"
                    ? await RuvioConnection.ConnectAsync(new RuvioClientOptions
                    {
                        Host = host, Port = ruvioPort, Password = password,
                        DiscoverCluster = false, Reconnect = false,
                    }) : null;
                var redisOptions = new ConfigurationOptions
                {
                    Password = password, AbortOnConnectFail = true,
                    ConnectTimeout = 10000, AsyncTimeout = 10000,
                };
                redisOptions.EndPoints.Add(host, redisPort);
                using ConnectionMultiplexer? redis = backend == "redis"
                    ? await ConnectionMultiplexer.ConnectAsync(redisOptions) : null;
                RuvioClient? client = connection is null ? null : await connection.GetDefaultDatabaseAsync();
                IDatabase? database = redis?.GetDatabase();
                long[][] buckets = [new long[100_001], new long[100_001]];
                long[] ticks = new long[2], counts = new long[2];

                async Task Worker(int worker, long deadline, bool measure)
                {
                    string key = $"paired:{run}:{repeat}:{profile}:{concurrency}:{worker}";
                    string value = new('x', 256);

                    if (!measure)
                    {
                        if (client is not null)
                            await client.SetAsync(key, value);
                        else if (!await database!.StringSetAsync(key, value))
                            throw new InvalidOperationException("Redis setup SET failed.");
                    }

                    string[] get = ["GET", key];
                    string[] set = ["SET", key, value];
                    string[] begin = ["IDEM", key + ":idem", "BEGIN", "fp", "owner", "60000"];
                    string[] limit = ["LIMIT", key + ":limit", "1000000000", "60000"];
                    long counter = 0;

                    while (Stopwatch.GetTimestamp() < deadline)
                    {
                        int op = (int)(counter++ & 1);

                        // Change every SET so identical-value optimizations cannot bias the comparison.
                        if (profile == "hot" && op == 1)
                            set[2] = value[..240] + counter.ToString("D16", CultureInfo.InvariantCulture);

                        long start = Stopwatch.GetTimestamp();

                        if (client is not null)
                        {
                            RespValue reply = await client.ExecuteAsync(
                                profile == "hot" ? op == 0 ? get : set : op == 0 ? begin : limit);
                            bool valid = profile == "hot"
                                ? op == 0 ? reply.Kind == RespKind.Bulk && reply.Bytes?.Length == 256
                                          : reply.AsString() == "OK"
                                : op == 0 ? reply.Items is { Count: > 0 } items &&
                                    items[0].AsString() is "acquired" or "pending"
                                          : reply.Kind == RespKind.Integer && reply.Integer >= 0;

                            if (!valid)
                                throw new InvalidOperationException("Unexpected Ruvio reply.");
                        }
                        else if (profile == "hot")
                        {
                            if (op == 0)
                            {
                                RedisValue reply = await database!.StringGetAsync(key);

                                if (reply.IsNull || ((byte[]?)reply)?.Length != 256)
                                    throw new InvalidOperationException("Unexpected Redis GET.");
                            }
                            else if (!await database!.StringSetAsync(key, set[2]))
                                throw new InvalidOperationException("Unexpected Redis SET.");
                        }
                        else if (op == 0)
                        {
                            RedisResult result = await database!.ScriptEvaluateAsync(
                                BeginScript, [key + ":idem"], ["fp", "owner", 60000]);
                            RedisResult[] items = (RedisResult[])result!;

                            if (items.Length == 0 || items[0].ToString() is not ("acquired" or "pending"))
                                throw new InvalidOperationException("Unexpected Redis BEGIN equivalent.");
                        }
                        else if ((long)await database!.ScriptEvaluateAsync(
                            LimitScript, [key + ":limit"], [1000000000, 60000]) < 0)
                            throw new InvalidOperationException("Unexpected Redis LIMIT equivalent.");

                        if (measure)
                        {
                            long duration = Stopwatch.GetTimestamp() - start;
                            Interlocked.Add(ref ticks[op], duration);
                            Interlocked.Increment(ref counts[op]);
                            int bucket = (int)Math.Min(100_000, duration * 1_000_000 / Stopwatch.Frequency);
                            Interlocked.Increment(ref buckets[op][bucket]);
                        }
                    }
                }

                async Task Workers(int duration, bool measure)
                {
                    long deadline = Stopwatch.GetTimestamp() + duration * Stopwatch.Frequency;
                    await Task.WhenAll(Enumerable.Range(0, concurrency)
                        .Select(worker => Worker(worker, deadline, measure)));
                }

                await Workers(2, false);
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                process.Refresh();
                long allocated = GC.GetTotalAllocatedBytes(true);
                TimeSpan cpu = process.TotalProcessorTime;
                long started = Stopwatch.GetTimestamp();
                await Workers(seconds, true);
                double elapsed = Stopwatch.GetElapsedTime(started).TotalSeconds;
                long allocations = GC.GetTotalAllocatedBytes(true) - allocated;
                process.Refresh();
                double Percentile(int op, double fraction)
                {
                    long target = (long)Math.Ceiling(counts[op] * fraction), sum = 0;

                    for (int i = 0; i < buckets[op].Length; i++)
                    {
                        sum += buckets[op][i];

                        if (sum >= target)
                            return (i + 1) / 1000.0;
                    }

                    throw new InvalidOperationException("Empty latency histogram.");
                }

                string[] operations = profile == "hot" ? ["GET", "SET"] : ["BEGIN", "LIMIT"];
                results.Add(new
                {
                    repeat, backend, profile, concurrency, elapsedSeconds = elapsed,
                    operationsPerSecond = counts.Sum() / elapsed,
                    operations = Enumerable.Range(0, 2).Select(op => new
                    {
                        operation = operations[op], count = counts[op],
                        meanMs = ticks[op] * 1000.0 / Stopwatch.Frequency / counts[op],
                        p50Ms = Percentile(op, .5), p95Ms = Percentile(op, .95),
                        p99Ms = Percentile(op, .99), overflow100ms = buckets[op][^1],
                    }),
                    allocatedBytesPerOperation = allocations / (double)counts.Sum(),
                    clientCpuSeconds = (process.TotalProcessorTime - cpu).TotalSeconds,
                    clientWorkingSetBytes = process.WorkingSet64,
                });
                await File.WriteAllTextAsync(args[4], JsonSerializer.Serialize(new
                {
                    host, seconds, run, runtime = Environment.Version.ToString(),
                    transport = "one shared multiplexed connection per backend; no explicit pipeline",
                    results,
                }, new JsonSerializerOptions { WriteIndented = true }));
                Console.WriteLine($"{repeat} {backend} {profile} {concurrency}: {counts.Sum() / elapsed:F0} ops/s");
            }
        }
    }
}
