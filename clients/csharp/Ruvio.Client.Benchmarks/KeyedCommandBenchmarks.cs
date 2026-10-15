using BenchmarkDotNet.Attributes;
using Ruvio.Client;

namespace Ruvio.Client.Benchmarks;

[MemoryDiagnoser]
public class KeyedCommandBenchmarks
{
    private (string, string)[] _pairs = [];

    [Params(1, 2, 3)]
    public int PairCount { get; set; }

    [GlobalSetup]
    public void Setup() =>
        _pairs = Enumerable.Range(0, PairCount).Select(i => ($"field:{i}", "value")).ToArray();

    [Benchmark(Baseline = true)]
    public int List()
    {
        var parts = new string[2 + _pairs.Length * 2];
        parts[0] = "HSET";
        parts[1] = "key";
        int index = 2;
        foreach ((string field, string value) in _pairs)
        {
            parts[index++] = field;
            parts[index++] = value;
        }
        using PooledFrame frame = new RespCommand(parts).EncodePooled();
        return frame.Length;
    }

    [Benchmark]
    public int Inline()
    {
        using PooledFrame frame = RespCommand.KeyedPairs("HSET", "key", _pairs).EncodePooled();
        return frame.Length;
    }
}
