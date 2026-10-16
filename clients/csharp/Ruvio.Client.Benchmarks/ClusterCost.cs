using System.Diagnostics;
using Ruvio.Client;

/// <summary>
/// CPU and allocation per request: the plain single-connection client, the cluster client with
/// keys spread over the nodes, and the cluster client with every key in one hash tag (the routing
/// code's own cost). Plain depth 4 is the per-socket batch a depth-16 pipeline leaves on each of
/// four nodes. About a minute with the defaults. Run with
/// DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0, or thread-pool spinning dominates the CPU numbers.
/// </summary>
internal static class ClusterCost
{
    private const int KeyCount = 4096;
    private const double Warmup = 0.5;

    // RUVIO_DEDICATED_READER=1 measures RuvioClientOptions.DedicatedReaderThread.
    private static readonly bool DedicatedReader = Environment.GetEnvironmentVariable("RUVIO_DEDICATED_READER") == "1";

    public static async Task RunAsync(string[] args)
    {
        int port = int.Parse(args[0]);
        int workers = args.Length > 1 ? int.Parse(args[1]) : 8;
        int seconds = args.Length > 2 ? int.Parse(args[2]) : 2;
        string value = new('x', 32);

        foreach (string op in new[] { "GET", "SET" })
        {
            foreach ((string mode, int depth) in new[]
            {
                ("plain", 4), ("plain", 16), ("cluster", 16), ("one-node", 16),
                ("plain", 64), ("cluster", 64), ("one-node", 64),
            })
            {
                // RUVIO_COST_ONLY=plain:4 runs one row (GET only), for quick experiments.
                if (Environment.GetEnvironmentVariable("RUVIO_COST_ONLY") is { } only && (only != $"{mode}:{depth}" || op != "GET"))
                    continue;

                string tag = mode == "one-node" ? "{t}" : "";
                string[] keys = Enumerable.Range(0, KeyCount).Select(i => $"{tag}key:{i}").ToArray();
                IReadOnlyList<string>[] commands = keys
                    .Select(key => op == "SET" ? (IReadOnlyList<string>)new[] { "SET", key, value } : new[] { "GET", key })
                    .ToArray();

                await RowAsync(port, workers, seconds, op, depth, mode, keys, value, commands);
            }
        }
    }

    private static async Task RowAsync(
        int port, int workers, int seconds, string op, int depth, string mode,
        string[] keys, string value, IReadOnlyList<string>[] commands)
    {
        var connections = new List<RuvioConnection>();
        var clients = new IRuvioClient[workers];

        for (int worker = 0; worker < workers; worker++)
        {
            RuvioConnection connection = await RuvioConnection.ConnectAsync(
                new RuvioClientOptions
                {
                    Host = "127.0.0.1",
                    Port = port,
                    DiscoverCluster = mode != "plain",
                    DedicatedReaderThread = DedicatedReader,
                });

            connections.Add(connection);
            clients[worker] = await connection.GetDefaultDatabaseAsync();
        }

        await Task.WhenAll(clients.Select((client, worker) =>
            Task.Run(() => Loop(client, worker, DateTime.UtcNow.AddSeconds(Warmup), op, depth, keys, value, commands))));

        Process process = Process.GetCurrentProcess();
        process.Refresh();
        TimeSpan user = process.UserProcessorTime;
        TimeSpan system = process.PrivilegedProcessorTime;
        long allocated = GC.GetTotalAllocatedBytes(true);
        var clock = Stopwatch.StartNew();
        DateTime until = DateTime.UtcNow.AddSeconds(seconds);

        long[] counts = await Task.WhenAll(clients.Select((client, worker) =>
            Task.Run(() => Loop(client, worker, until, op, depth, keys, value, commands))));

        double elapsed = clock.Elapsed.TotalSeconds;

        process.Refresh();
        double total = counts.Sum();

        Console.WriteLine(
            $"{mode,-8} {op} P={depth} ops/s={total / elapsed:F0} " +
            $"user_ns/op={(process.UserProcessorTime - user).TotalNanoseconds / total:F0} " +
            $"sys_ns/op={(process.PrivilegedProcessorTime - system).TotalNanoseconds / total:F0} " +
            $"bytes/op={(GC.GetTotalAllocatedBytes(true) - allocated) / total:F0}");

        foreach (RuvioConnection connection in connections)
        {
            await connection.DisposeAsync();
        }
    }

    private static async Task<long> Loop(
        IRuvioClient client, int worker, DateTime until, string op, int depth,
        string[] keys, string value, IReadOnlyList<string>[] commands)
    {
        long operations = 0;
        int index = worker * 997 % (commands.Length - depth);

        while (DateTime.UtcNow < until)
        {
            if (depth == 1)
            {
                if (op == "SET")
                {
                    await client.SetAsync(keys[index], value);
                }
                else
                {
                    await client.GetAsync(keys[index]);
                }
            }
            else
            {
                var replies = await client.ExecuteManyAsync(new ArraySegment<IReadOnlyList<string>>(commands, index, depth));

                if (replies.Count != depth)
                {
                    throw new InvalidOperationException("reply count");
                }
            }

            operations += depth;
            index = (index + depth) % (commands.Length - depth);
        }

        return operations;
    }
}
