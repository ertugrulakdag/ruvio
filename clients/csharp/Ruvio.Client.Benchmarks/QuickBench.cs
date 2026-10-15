using System.Diagnostics;
using System.Text;
using Ruvio.Client;
using Ruvio.Client.Benchmarks;

/// <summary>
/// A few-second smoke benchmark: frame encode and reply parse cost (ns and bytes per op, measured with a
/// Stopwatch and GC.GetAllocatedBytesForCurrentThread, not BenchmarkDotNet), then serial and concurrent
/// GET/SET plus the coverage-pass DELEX/LPUSHCAP against a live server on one multiplexed connection.
/// Usage: --quick [host:]port [seconds-per-case]
/// </summary>
internal static class QuickBench
{
    internal static async Task RunAsync(string[] args)
    {
        string target = args.Length > 0 ? args[0] : "127.0.0.1:6379";
        double seconds = args.Length > 1 ? double.Parse(args[1], System.Globalization.CultureInfo.InvariantCulture) : 1;
        int colon = target.LastIndexOf(':');
        string host = colon > 0 ? target[..colon] : "127.0.0.1";
        int port = int.Parse(colon > 0 ? target[(colon + 1)..] : target);

        Console.WriteLine($"runtime={Environment.Version} server={host}:{port} seconds/case={seconds}");
        Encode("build+encode GET", () => new RespCommand("GET", "session:ada"));
        Encode("build+encode SET", () => new RespCommand("SET", "session:ada", "hello"));
        Encode("build+encode DELEX", () => CoverageCommands.DeleteIfEqual("lock:ada", "0123456789abcdef"));
        Encode("build+encode LPUSHCAP", () => CoverageCommands.Capped("LPUSHCAP", "recent:ada", 100, Item));
        Encode("build+encode SET IFEQ PX", () => CoverageCommands.SetIfEqual("lock:ada", "tok", "tok", TimeSpan.FromSeconds(30)));
        await Parse("parse :1", ":1\r\n");
        await Parse("parse +OK", "+OK\r\n");
        await Parse("parse $5 bulk", "$5\r\nhello\r\n");
        await Parse("parse *2 ints", "*2\r\n:1\r\n:0\r\n");

        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = host, Port = port, Database = 11, DiscoverCluster = false, Reconnect = false,
        });
        RuvioClient db = await connection.GetDefaultDatabaseAsync();
        await db.FlushDatabaseAsync();
        string value = new('x', 256);
        await db.SetAsync("bench:get", value);

        foreach (int callers in new[] { 1, 16 })
        {
            await Load($"GET c={callers}", callers, seconds, (_, _) => db.GetAsync("bench:get").AsTask());
            await Load($"SET c={callers}", callers, seconds, (worker, _) => db.SetAsync("bench:set:" + worker, value).AsTask());
            await Load($"DELEX c={callers}", callers, seconds, (worker, _) => db.DeleteIfEqualAsync("bench:lock:" + worker, "t").AsTask());
            await Load($"LPUSHCAP c={callers}", callers, seconds, (worker, _) => db.PushListLeftCappedAsync("bench:recent:" + worker, 100, "item").AsTask());
        }

        await db.FlushDatabaseAsync();
    }

    private static readonly string[] Item = ["item"];

    private static void Encode(string name, Func<RespCommand> build)
    {
        const int Iterations = 200_000;
        for (int index = 0; index < 150_000; index++)
        {
            using PooledFrame warm = build().EncodePooled();
        }

        long bytes = GC.GetAllocatedBytesForCurrentThread();
        long start = Stopwatch.GetTimestamp();
        for (int index = 0; index < Iterations; index++)
        {
            using PooledFrame frame = build().EncodePooled();
        }

        double ns = Stopwatch.GetElapsedTime(start).TotalNanoseconds / Iterations;
        Console.WriteLine($"{name,-26} {ns,8:F1} ns/op {(GC.GetAllocatedBytesForCurrentThread() - bytes) / (double)Iterations,6:F1} B/op");
    }

    private static async Task Parse(string name, string reply)
    {
        const int Iterations = 200_000;
        var stream = new ReplayStream(Encoding.ASCII.GetBytes(reply));
        using var reader = new RespReader(stream);
        for (int index = 0; index < 150_000; index++)
        {
            stream.Rewind();
            _ = await reader.ReadAsync(CancellationToken.None);
        }

        long bytes = GC.GetAllocatedBytesForCurrentThread();
        long start = Stopwatch.GetTimestamp();
        for (int index = 0; index < Iterations; index++)
        {
            stream.Rewind();
            _ = await reader.ReadAsync(CancellationToken.None);
        }

        double ns = Stopwatch.GetElapsedTime(start).TotalNanoseconds / Iterations;
        Console.WriteLine($"{name,-26} {ns,8:F1} ns/op {(GC.GetAllocatedBytesForCurrentThread() - bytes) / (double)Iterations,6:F1} B/op");
    }

    private static async Task Load(string name, int callers, double seconds, Func<int, long, Task> operation)
    {
        await Run(callers, TimeSpan.FromMilliseconds(200), operation);
        long allocated = GC.GetTotalAllocatedBytes(precise: true);
        (long done, TimeSpan elapsed) = await Run(callers, TimeSpan.FromSeconds(seconds), operation);
        long bytes = GC.GetTotalAllocatedBytes(precise: true) - allocated;
        Console.WriteLine($"{name,-26} {done / elapsed.TotalSeconds,10:F0} ops/s {bytes / (double)done,7:F0} B/op (process, incl. harness)");
    }

    private static async Task<(long Done, TimeSpan Elapsed)> Run(int callers, TimeSpan length, Func<int, long, Task> operation)
    {
        long done = 0;
        long start = Stopwatch.GetTimestamp();
        long deadline = start + (long)(length.TotalSeconds * Stopwatch.Frequency);
        await Task.WhenAll(Enumerable.Range(0, callers).Select(async worker =>
        {
            long local = 0;
            while (Stopwatch.GetTimestamp() < deadline)
            {
                await operation(worker, local).ConfigureAwait(false);
                local++;
            }

            Interlocked.Add(ref done, local);
        }));

        return (done, Stopwatch.GetElapsedTime(start));
    }
}
