using System.Diagnostics;
using System.Text.Json;
using Ruvio.Client;

internal static class SocketLoad
{
    internal static async Task RunAsync(string[] args)
    {
        int port = args.Length > 0 ? int.Parse(args[0]) : 16389;
        int seconds = args.Length > 1 ? int.Parse(args[1]) : 3;
        bool inline = args.Length > 2 && args[2] == "--inline";
        string? selectedOperation = null;
        int? selectedConcurrency = null;

        if (args.Length == 4)
        {
            string[] selected = args[3].Split(':');

            if (selected.Length != 2 ||
                selected[0] is not ("get" or "set" or "limit") ||
                !int.TryParse(selected[1], out int concurrency) ||
                concurrency is not (1 or 2 or 4 or 16 or 128))
                throw new ArgumentException("Case must be get/set/limit:1/2/4/16/128.");

            selectedOperation = selected[0];
            selectedConcurrency = concurrency;
        }

        if (args.Length > 4 || (args.Length > 2 && !inline) || seconds <= 0)
            throw new ArgumentException("Expected [port] [positive seconds] [--inline [operation:concurrency]].");

        string run = Guid.NewGuid().ToString("N");
        Console.Error.WriteLine($"socket-load runtime={Environment.Version} run={run} port={port} seconds={seconds}");
        var results = new List<object>();
        using var process = Process.GetCurrentProcess();

        foreach (string operation in new[] { "get", "set", "limit" })
        {
            if (selectedOperation is not null && operation != selectedOperation)
                continue;

            foreach (int concurrency in new[] { 1, 2, 4, 16, 128 })
            {
                if (selectedConcurrency is not null && concurrency != selectedConcurrency)
                    continue;

                Console.Error.WriteLine($"socket-load {operation} concurrency={concurrency} inline={inline}");
                using var connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
                {
                    Port = port, DiscoverCluster = false, Reconnect = false,
                });
                RuvioClient client = await connection.GetDefaultDatabaseAsync();
                string getKey = $"socket-load:{run}:get";
                await client.SetAsync(getKey, new string('x', 256));
                long completed = 0;
                long[] buckets = new long[100_001];
                long ticks = 0;
                async Task Workers(long deadline, bool measure)
                {
                    await Task.WhenAll(Enumerable.Range(0, concurrency).Select(async worker =>
                    {
                        string[] command = operation switch
                        {
                            "get" => ["GET", getKey],
                            "set" => ["SET", $"socket-load:{run}:set:{worker}", new string('x', 256)],
                            _ => ["LIMIT", $"socket-load:{run}:limit:{worker}", "1000000000", "60000"],
                        };
                        RespCommand typed = operation switch
                        {
                            "get" => new RespCommand("GET", command[1]),
                            "set" => new RespCommand("SET", command[1], command[2]),
                            _ => new RespCommand("LIMIT", command[1], 1000000000L, 60000L),
                        };

                        while (Stopwatch.GetTimestamp() < deadline)
                        {
                            long started = Stopwatch.GetTimestamp();
                            RespValue reply = inline
                                ? await client.ExecuteCommandAsync(typed, CancellationToken.None).ConfigureAwait(false)
                                : await client.ExecuteAsync(command).ConfigureAwait(false);
                            bool valid = operation switch
                            {
                                "get" => reply.Kind == RespKind.Bulk && reply.Bytes?.Length == 256,
                                "set" => reply.AsString() == "OK",
                                _ => reply.Kind == RespKind.Integer,
                            };

                            if (!valid)
                                throw new InvalidOperationException($"Unexpected {operation} reply: {reply.Kind}");

                            if (measure)
                            {
                                long duration = Stopwatch.GetTimestamp() - started;
                                Interlocked.Add(ref ticks, duration);
                                int bucket = (int)Math.Min(100_000, duration * 1_000_000 / Stopwatch.Frequency);
                                Interlocked.Increment(ref buckets[bucket]);
                                Interlocked.Increment(ref completed);
                            }
                        }
                    }));
                }

                Console.Error.WriteLine("phase=warmup");
                await Workers(Stopwatch.GetTimestamp() + Stopwatch.Frequency, false);
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                long allocated = GC.GetTotalAllocatedBytes(true);
                int[] gc = [GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2)];
                process.Refresh();
                TimeSpan cpu = process.TotalProcessorTime;
                Console.Error.WriteLine("phase=measurement");
                long start = Stopwatch.GetTimestamp();
                await Workers(start + seconds * Stopwatch.Frequency, true);
                double elapsed = Stopwatch.GetElapsedTime(start).TotalSeconds;
                long bytes = GC.GetTotalAllocatedBytes(true) - allocated;
                process.Refresh();
                double Percentile(double fraction)
                {
                    long target = (long)Math.Ceiling(completed * fraction), sum = 0;

                    for (int i = 0; i < buckets.Length; i++)
                    {
                        sum += buckets[i];

                        if (sum >= target)
                            return i / 1000.0;
                    }

                    return 100;
                }

                results.Add(new
                {
                    operation, concurrency, inline, completed, elapsedSeconds = elapsed,
                    operationsPerSecond = completed / elapsed,
                    meanMs = ticks * 1000.0 / Stopwatch.Frequency / completed,
                    p50Ms = Percentile(.50), p95Ms = Percentile(.95), p99Ms = Percentile(.99),
                    latencyOverflowSamples = buckets[^1],
                    allocatedBytesPerOperation = bytes / (double)completed,
                    gen0 = GC.CollectionCount(0) - gc[0],
                    gen1 = GC.CollectionCount(1) - gc[1],
                    gen2 = GC.CollectionCount(2) - gc[2],
                    cpuSeconds = (process.TotalProcessorTime - cpu).TotalSeconds,
                    workingSetBytes = process.WorkingSet64,
                    managedHeapBytes = GC.GetGCMemoryInfo().HeapSizeBytes,
                });
                var keys = new List<string> { getKey };

                for (int worker = 0; worker < concurrency; worker++)
                    keys.Add($"socket-load:{run}:{(operation == "set" ? "set" : "limit")}:{worker}");

                foreach (string[] chunk in keys.Chunk(32))
                    await client.ExecuteAsync(new[] { "DEL" }.Concat(chunk).ToArray());

                Console.Error.WriteLine($"completed={completed} opsPerSecond={completed / elapsed:F0}");
            }
        }

        Console.WriteLine(JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true }));
    }
}
