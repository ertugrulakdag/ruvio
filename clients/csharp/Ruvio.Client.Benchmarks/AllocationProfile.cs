using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using Ruvio.Client;

/// <summary>
/// Samples GC AllocationTick events (one per ~100 KB) and attributes allocated bytes
/// to types. Use enough operations that each workload produces thousands of samples.
/// </summary>
internal static class AllocationProfile
{
    internal static async Task RunAsync(string[] args)
    {
        int port = args.Length > 0 ? int.Parse(args[0]) : 16403;
        int seconds = args.Length > 1 ? int.Parse(args[1]) : 5;
        int concurrency = args.Length > 2 ? int.Parse(args[2]) : 16;

        using var connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Port = port, DiscoverCluster = false, Reconnect = false,
        });
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        await client.SetAsync("alloc-profile:key", new string('x', 256));

        foreach (string operation in new[] { "get", "get-typed", "set", "set-typed", "limit" })
        {
            await RunWorkersAsync(client, operation, concurrency, Stopwatch.GetTimestamp() + Stopwatch.Frequency);
            using var listener = new AllocationListener();
            long allocated = GC.GetTotalAllocatedBytes(true);
            long completed = await RunWorkersAsync(
                client, operation, concurrency, Stopwatch.GetTimestamp() + seconds * Stopwatch.Frequency);
            long bytes = GC.GetTotalAllocatedBytes(true) - allocated;
            listener.Dispose();

            long sampled = listener.Types.Values.Sum();
            Console.WriteLine($"== {operation} c={concurrency} ops={completed} bytes/op={bytes / (double)completed:F0}");

            foreach ((string type, long amount) in listener.Types.OrderByDescending(pair => pair.Value).Take(14))
            {
                double share = sampled == 0 ? 0 : amount / (double)sampled;
                Console.WriteLine($"  {share * bytes / completed,7:F0} B/op {share,6:P1}  {type}");
            }
        }
    }

    private static async Task<long> RunWorkersAsync(RuvioClient client, string operation, int concurrency, long deadline)
    {
        long completed = 0;
        string value = new('x', 256);
        await Task.WhenAll(Enumerable.Range(0, concurrency).Select(async worker =>
        {
            string key = $"alloc-profile:{worker}";
            string[] command = operation switch
            {
                "get" => ["GET", "alloc-profile:key"],
                "set" => ["SET", key, value],
                _ => ["LIMIT", $"alloc-profile:limit:{worker}", "1000000000", "60000"],
            };
            long local = 0;

            while (Stopwatch.GetTimestamp() < deadline)
            {
                switch (operation)
                {
                    case "get-typed":
                        await client.GetAsync("alloc-profile:key").ConfigureAwait(false);
                        break;
                    case "set-typed":
                        await client.SetAsync(key, value).ConfigureAwait(false);
                        break;
                    default:
                        await client.ExecuteAsync(command).ConfigureAwait(false);
                        break;
                }

                local++;
            }

            Interlocked.Add(ref completed, local);
        }));

        return completed;
    }

    private sealed class AllocationListener : EventListener
    {
        internal readonly ConcurrentDictionary<string, long> Types = new();

        protected override void OnEventSourceCreated(EventSource source)
        {
            if (source.Name == "Microsoft-Windows-DotNETRuntime")
                EnableEvents(source, EventLevel.Verbose, (EventKeywords)0x1);
        }

        protected override void OnEventWritten(EventWrittenEventArgs data)
        {
            if (data.EventName is null || !data.EventName.StartsWith("GCAllocationTick", StringComparison.Ordinal) ||
                data.Payload is null || data.PayloadNames is null)
                return;

            int typeIndex = data.PayloadNames.IndexOf("TypeName");
            int amountIndex = data.PayloadNames.IndexOf("AllocationAmount64");

            if (typeIndex < 0)
                return;

            string type = data.Payload[typeIndex] as string ?? "?";
            long amount = amountIndex >= 0 ? Convert.ToInt64(data.Payload[amountIndex]) : 100_000;
            Types.AddOrUpdate(type, amount, (_, current) => current + amount);
        }
    }
}
