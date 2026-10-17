using System.Text;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>
/// Allocation budgets, counted on the test's own thread. They run alone, after the parallel
/// tests, and each one keeps the best of three trials: a Gen2 GC caused by other work can trim
/// the shared array pool, so one trial may allocate a buffer again, while a real regression
/// allocates in every trial.
/// </summary>
[Collection("Allocation")]
public class AllocationTests
{
    private const int Trials = 3;

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
        long fewest = long.MaxValue;

        for (int trial = 0; trial < Trials; trial++)
        {
            for (int i = 0; i < 1000; i++)
                RuvioClient.GetHashSlot(key);

            long before = GC.GetAllocatedBytesForCurrentThread();
            int actual = 0;

            for (int i = 0; i < 10000; i++)
                actual = RuvioClient.GetHashSlot(key);

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            Assert.Equal(expected, actual);
            fewest = Math.Min(fewest, allocated);
        }

        Assert.Equal(0, fewest);
    }

    [Fact]
    public async Task ReaderBufferedBulkDoesNotAllocateHelperTasks()
    {
        const int replies = 1000;
        string frame = "$256\r\n" + new string('x', 256) + "\r\n";
        byte[] payload = Encoding.ASCII.GetBytes(string.Concat(Enumerable.Repeat(frame, replies + 1)));
        long fewest = long.MaxValue;

        for (int trial = 0; trial < Trials; trial++)
        {
            using var reader = new RespReader(new MemoryStream(payload));
            await reader.ReadAsync(CancellationToken.None);
            long before = GC.GetAllocatedBytesForCurrentThread();
            int totalBytes = 0;
            bool allSynchronous = true;

            for (int index = 0; index < replies; index++)
            {
                ValueTask<RespValue> read = reader.ReadAsync(CancellationToken.None);
                allSynchronous &= read.IsCompletedSuccessfully;
                totalBytes += (await read).Bytes!.Length;
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.True(allSynchronous);
            Assert.Equal(replies * 256, totalBytes);
            fewest = Math.Min(fewest, allocated);
        }

        Assert.InRange(fewest / replies, 0, 400);
    }
}

/// <summary>Allocation tests run without other tests beside them.</summary>
[CollectionDefinition("Allocation", DisableParallelization = true)]
public sealed class AllocationCollection;
