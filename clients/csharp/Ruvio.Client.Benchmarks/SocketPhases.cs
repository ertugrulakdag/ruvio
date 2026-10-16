using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;
using Ruvio.Client;

internal static class SocketPhases
{
    internal static async Task RunAsync(string[] args)
    {
        if (args.Length is not (2 or 3 or 4) || !int.TryParse(args[0], out int port) ||
            port is < 1 or > 65535 || !int.TryParse(args[1], out int seconds) || seconds is < 1 or > 60)
            throw new ArgumentException("Expected port seconds (1-60) [concurrency (1/16/128)], using a disposable standalone server.");

        int concurrency = args.Length >= 3 ? int.Parse(args[2]) : 1;

        if (concurrency is not (1 or 16 or 128))
            throw new ArgumentException("Concurrency must be 1, 16 or 128.");

        bool pipeline = args.Length == 4 && args[3] == "--pipeline";

        if (args.Length == 4 && !pipeline)
            throw new ArgumentException("Expected --pipeline as the fourth argument.");

        string key = "socket-phases:" + Guid.NewGuid().ToString("N");
        string value = new('x', 256);
        using var connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Port = port, DiscoverCluster = false, Reconnect = false,
        });
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        await client.SetAsync(key, value);
        var results = new List<object>();
        try
        {
            foreach (string operation in new[] { "get", "set" })
            {
                IReadOnlyList<string>[] batch = Enumerable.Range(0, concurrency)
                    .Select(_ => (IReadOnlyList<string>)(operation == "get"
                        ? new[] { "GET", key } : new[] { "SET", key, value })).ToArray();
                async Task<long> Pipeline(int duration)
                {
                    long deadline = Stopwatch.GetTimestamp() + duration * Stopwatch.Frequency;
                    long completed = 0;

                    while (Stopwatch.GetTimestamp() < deadline)
                    {
                        IReadOnlyList<RespValue> replies = await client.ExecuteManyAsync(batch);

                        if (replies.Count != batch.Length || replies.Any(reply => operation == "get"
                            ? reply.Kind != RespKind.Bulk || reply.Bytes?.Length != 256
                            : reply.AsString() != "OK"))
                            throw new InvalidOperationException("Unexpected pipeline reply.");

                        completed += replies.Count;
                    }

                    return completed;
                }

                async ValueTask Execute()
                {
                    if (operation == "get")
                    {
                        if ((await client.GetAsync(key).ConfigureAwait(false))?.Length != 256)
                            throw new InvalidOperationException("Unexpected GET reply.");
                    }
                    else
                        await client.SetAsync(key, value).ConfigureAwait(false);
                }

                async Task<long> Workers(int duration)
                {
                    if (pipeline)
                        return await Pipeline(duration);

                    long deadline = Stopwatch.GetTimestamp() + duration * Stopwatch.Frequency;
                    long completed = 0;
                    await Task.WhenAll(Enumerable.Range(0, concurrency).Select(async _ =>
                    {
                        long local = 0;

                        while (Stopwatch.GetTimestamp() < deadline)
                        {
                            await Execute();
                            local++;
                        }

                        Interlocked.Add(ref completed, local);
                    }));

                    return completed;
                }

                await Workers(1);
                var phases = new Dictionary<string, (long Count, double TotalMs)>();
                object gate = new();
                long writes = 0, writtenBytes = 0;
                using var listener = new MeterListener();
                listener.InstrumentPublished = (instrument, meterListener) =>
                {
                    if (instrument.Meter.Name == "Ruvio.Client" &&
                        instrument.Name is "ruvio_client_socket_phase_duration_milliseconds"
                            or "ruvio_client_socket_write_calls" or "ruvio_client_socket_written_bytes")
                        meterListener.EnableMeasurementEvents(instrument);
                };
                listener.SetMeasurementEventCallback<long>((instrument, amount, _, _) =>
                {
                    if (instrument.Name == "ruvio_client_socket_write_calls")
                        Interlocked.Add(ref writes, amount);
                    else if (instrument.Name == "ruvio_client_socket_written_bytes")
                        Interlocked.Add(ref writtenBytes, amount);
                });
                listener.SetMeasurementEventCallback<double>((_, elapsed, tags, _) =>
                {
                    foreach (var tag in tags)
                    {
                        if (tag.Key != "phase" || tag.Value is not string phase)
                            continue;

                        lock (gate)
                        {
                            phases.TryGetValue(phase, out var previous);
                            phases[phase] = (previous.Count + 1, previous.TotalMs + elapsed);
                        }
                    }
                });
                listener.Start();
                using var process = Process.GetCurrentProcess();
                TimeSpan cpu = process.TotalProcessorTime;
                long allocated = GC.GetTotalAllocatedBytes(true);
                long start = Stopwatch.GetTimestamp();
                long count = await Workers(seconds);
                double duration = Stopwatch.GetElapsedTime(start).TotalSeconds;
                process.Refresh();
                listener.Dispose();
                object[] measured;
                lock (gate)
                    measured = phases.Select(pair => (object)new
                    {
                        phase = pair.Key, samples = pair.Value.Count,
                        meanMicroseconds = pair.Value.TotalMs * 1000 / pair.Value.Count,
                    }).ToArray();
                results.Add(new
                {
                    operation, count, operationsPerSecond = count / duration,
                    wallMicrosecondsPerOperation = duration * 1_000_000 / count,
                    cpuMicrosecondsPerOperation = (process.TotalProcessorTime - cpu).TotalMilliseconds * 1000 / count,
                    allocatedBytesPerOperation = (GC.GetTotalAllocatedBytes(true) - allocated) / (double)count,
                    writes, writtenBytes, writesPerOperation = writes / (double)count,
                    meanBytesPerWrite = writes == 0 ? 0 : writtenBytes / (double)writes,
                    phases = measured,
                });
            }
        }
        finally
        {
            await client.DeleteAsync(key);
        }

        Console.WriteLine(JsonSerializer.Serialize(new
        {
            runtime = Environment.Version.ToString(), concurrency, pipeline,
            note = "Phase metrics enabled: instrumentation perturbs timings; phases overlap and are not additive. SET repeats identical values.",
            results,
        }, new JsonSerializerOptions { WriteIndented = true }));
    }
}
