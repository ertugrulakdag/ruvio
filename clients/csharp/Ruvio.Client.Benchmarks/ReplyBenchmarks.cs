using System.Text;
using BenchmarkDotNet.Attributes;
using Ruvio.Client;

namespace Ruvio.Client.Benchmarks;

/// <summary>One reply on a reused reader, the way a connection parses the next frame.</summary>
[MemoryDiagnoser]
public class ReplyBenchmarks
{
    private readonly ReplayStream _integer = new(Encoding.ASCII.GetBytes(":7\r\n"));
    private readonly ReplayStream _bulk = new(Encoding.ASCII.GetBytes("$5\r\nhello\r\n"));
    private readonly ReplayStream _array = new(Encoding.ASCII.GetBytes("*2\r\n$1\r\na\r\n$1\r\nb\r\n"));
    private RespReader _integerReader = null!;
    private RespReader _bulkReader = null!;
    private RespReader _arrayReader = null!;

    [GlobalSetup]
    public void Setup()
    {
        _integerReader = new RespReader(_integer);
        _bulkReader = new RespReader(_bulk);
        _arrayReader = new RespReader(_array);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _integerReader.Dispose();
        _bulkReader.Dispose();
        _arrayReader.Dispose();
    }

    [Benchmark(Baseline = true)]
    public async Task<long> ReadInteger()
    {
        _integer.Rewind();
        RespValue value = await _integerReader.ReadAsync(CancellationToken.None);

        return value.Integer;
    }

    [Benchmark]
    public async Task<int> ReadBulk()
    {
        _bulk.Rewind();
        RespValue value = await _bulkReader.ReadAsync(CancellationToken.None);

        return value.Bytes!.Length;
    }

    [Benchmark]
    public async Task<int> ReadArray()
    {
        _array.Rewind();
        RespValue value = await _arrayReader.ReadAsync(CancellationToken.None);

        return value.Items!.Count;
    }
}
