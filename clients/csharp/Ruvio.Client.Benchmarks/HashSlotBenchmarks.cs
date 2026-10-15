using BenchmarkDotNet.Attributes;
using Ruvio.Client;

namespace Ruvio.Client.Benchmarks;

/// <summary>Cluster slot of a key, including a hash tag.</summary>
[MemoryDiagnoser]
public class HashSlotBenchmarks
{
    private readonly string _key = "session:ada";
    private readonly string _tagged = "{user1000}.following";

    [Benchmark(Baseline = true)]
    public int PlainKey()
    {
        return RuvioClient.GetHashSlot(_key);
    }

    [Benchmark]
    public int HashTag()
    {
        return RuvioClient.GetHashSlot(_tagged);
    }
}
