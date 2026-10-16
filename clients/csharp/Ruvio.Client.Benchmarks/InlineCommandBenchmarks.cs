using BenchmarkDotNet.Attributes;
using Ruvio.Client;

namespace Ruvio.Client.Benchmarks;

[MemoryDiagnoser]
public class InlineCommandBenchmarks
{
    private RespCommand _command;

    [Params("PING", "GET", "SET", "INCR", "INCRBY", "TTL", "HGET", "IDEM", "CUSTOM")]
    public string Verb { get; set; } = "GET";

    [GlobalSetup]
    public void Setup()
    {
        _command = Verb switch
        {
            "PING" => new RespCommand(Verb),
            "SET" => new RespCommand(Verb, "session:ada", "hello"),
            "INCRBY" => new RespCommand(Verb, "counter:ada", 1L),
            "HGET" => new RespCommand(Verb, "hash:ada", "field"),
            "IDEM" => new RespCommand(Verb, "request:ada", "BEGIN", "fp", "owner", 60000L),
            _ => new RespCommand(Verb, "session:ada"),
        };
    }

    [Benchmark]
    public int Encode()
    {
        using PooledFrame frame = _command.EncodePooled();

        return frame.Length;
    }
}
