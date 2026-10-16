using System.Text;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class BinaryCommandTests
{
    [Fact(Timeout = 15000)]
    public async Task ExecuteBinaryAsyncSendsArbitraryBytesAndReadsBinaryReply()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        byte[] value = [0, 255, 128, 13, 10];
        ValueTask<RespValue> pending = session.Client.ExecuteBinaryAsync(
            new ReadOnlyMemory<byte>[] { "SET"u8.ToArray(), "key"u8.ToArray(), value });

        await session.ExpectBytes(new ReadOnlyMemory<byte>[] { "SET"u8.ToArray(), "key"u8.ToArray(), value });

        await session.Reply("+OK\r\n");
        Assert.Equal("OK", (await pending).AsString());

        pending = session.Client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[] { "GET"u8.ToArray(), "key"u8.ToArray() });

        await session.Expect("GET", "key");
        await session.ReplyBytes("$5\r\n"u8.ToArray().Concat(value).Concat("\r\n"u8.ToArray()).ToArray());
        Assert.Equal(value, (await pending).Bytes);
    }

    [Fact(Timeout = 15000)]
    public async Task ExecuteBinaryAsyncThrowsServerErrorsAndValidatesArgumentsAndCancellation()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await session.Client.ExecuteBinaryAsync(null!));
        await Assert.ThrowsAsync<ArgumentException>(async () => await session.Client.ExecuteBinaryAsync([]));
        using var canceled = new CancellationTokenSource();

        canceled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await session.Client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[] { "PING"u8.ToArray() }, canceled.Token));

        ValueTask<RespValue> pending = session.Client.ExecuteBinaryAsync(
            new ReadOnlyMemory<byte>[] { "SET"u8.ToArray(), "key"u8.ToArray() });

        await session.Expect("SET", "key");
        await session.Reply("-ERR wrong arity\r\n");
        await Assert.ThrowsAsync<RuvioException>(async () => await pending);
    }

    [Fact]
    public void BinaryHashSlotsUseOriginalBytesAndHashTags()
    {
        Assert.Equal(ClusterKeys.Slot("é"), ClusterKeys.Slot(Encoding.UTF8.GetBytes("é")));
        Assert.Equal(ClusterKeys.Slot(new byte[] { 255 }), ClusterKeys.Slot(new byte[] { 0, (byte)'{', 255, (byte)'}', 128 }));

        Assert.NotEqual(ClusterKeys.Slot(new byte[] { 255 }), ClusterKeys.Slot(Encoding.UTF8.GetBytes("\ufffd")));
    }

    [Theory]
    [InlineData(16, false, false)]
    [InlineData(16, true, false)]
    [InlineData(1024, false, false)]
    [InlineData(1024, true, true)]
    public void ExistingStringHashSlotPathRemainsAllocationFreeAfterWarmup(
        int length, bool tagged, bool unicode)
    {
        string key = new(unicode ? 'é' : 'x', length);

        if (tagged)
            key = "prefix:{" + key + "}:suffix";

        int expected = RuvioClient.GetHashSlot(key);

        for (int i = 0; i < 1000; i++)
            RuvioClient.GetHashSlot(key);

        long before = GC.GetAllocatedBytesForCurrentThread();
        int actual = 0;

        for (int i = 0; i < 10000; i++)
            actual = RuvioClient.GetHashSlot(key);

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(expected, actual);
        Assert.Equal(0, allocated);
    }
}
