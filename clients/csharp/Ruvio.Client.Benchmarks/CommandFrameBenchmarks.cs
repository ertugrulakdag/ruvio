using BenchmarkDotNet.Attributes;
using Ruvio.Client;

namespace Ruvio.Client.Benchmarks;

/// <summary>Bytes the client writes before they reach the socket.</summary>
[MemoryDiagnoser]
public class CommandFrameBenchmarks
{
    private readonly string _key = "session:ada";
    private readonly string[] _get = ["GET", "session:ada"];
    private readonly string[] _geoAdd =
    [
        "cities",
        "13.361389",
        "38.115556",
        "Palermo",
        "15.087269",
        "37.502669",
        "Catania",
    ];
    
    private readonly IReadOnlyList<string>[] _pipeline = new IReadOnlyList<string>[32];

    public CommandFrameBenchmarks()
    {
        for (int index = 0; index < _pipeline.Length; index++)
        {
            _pipeline[index] = ["GET", "session:" + index];
        }
    }

    [Benchmark(Baseline = true)]
    public int WriteGet()
    {
        using PooledFrame frame = RespCodec.WriteVerb(RespVerbs.GET, _key);

        return frame.Length;
    }

    [Benchmark]
    public int EncodeGet()
    {
        using PooledFrame frame = RespCodec.EncodePooled(_get);

        return frame.Length;
    }

    [Benchmark]
    public int WriteGeoAdd()
    {
        using PooledFrame frame = RespCodec.WriteVerb(RespVerbs.GEOADD, _geoAdd);

        return frame.Length;
    }

    [Benchmark]
    public int EncodePipeline32()
    {
        using PooledFrame frame = RespCodec.EncodeManyPooled(_pipeline);

        return frame.Length;
    }
}
