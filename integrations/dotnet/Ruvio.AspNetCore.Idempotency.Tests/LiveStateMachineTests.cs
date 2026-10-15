using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.Idempotency.Tests;

public sealed class LiveFactAttribute : FactAttribute
{
    public LiveFactAttribute()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")))
            Skip = "Set RUVIO_TEST_ADDR to run against live Ruvio.";
    }
}

public class LiveStateMachineTests
{
    [LiveFact]
    public async Task Atomic_states_ownership_and_retention()
    {
        var endpoint = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")!.Split(':');

        await using var connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = endpoint[0], Port = int.Parse(endpoint[1])
        });
        IRuvioClient client = await connection.GetDefaultDatabaseAsync();

        string key = "idempotency-test:{" + Guid.NewGuid().ToString("N") + "}";

        try
        {
            await client.ExecuteAsync(["HSET", key, "fingerprint", "fp", "owner", "owner", "state", "pending"], default);
            await client.ExecuteAsync(["PEXPIRE", key, "10000"], default);
            await Assert.ThrowsAsync<RuvioException>(() =>
                IdempotencyStore.AcquireAsync(client, key, "fp", "owner", TimeSpan.FromSeconds(10), default));
            Assert.Equal("fp", (await client.ExecuteAsync(["HGET", key, "fingerprint"], default)).AsString());
            await client.ExecuteAsync(["DEL", key], default);
            var acquired = await IdempotencyStore.AcquireAsync(client, key, "fp", "owner", TimeSpan.FromSeconds(10), default);

            Assert.Equal("acquired", acquired.State);
            Assert.Equal("pending", (await IdempotencyStore.AcquireAsync(client, key, "fp", "other", TimeSpan.FromDays(1), default)).State);
            Assert.Equal("conflict", (await IdempotencyStore.AcquireAsync(client, key, "different", "other", TimeSpan.FromDays(1), default)).State);
            long before = (await client.ExecuteAsync(["PTTL", key], default)).Integer;

            Assert.InRange(before, 1, 10_000);
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                IdempotencyStore.CompleteAsync(client, key, "fp", "wrong-owner", "wrong", default));
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                IdempotencyStore.CompleteAsync(client, key, "wrong-fp", "owner", "wrong", default));
            await IdempotencyStore.CompleteAsync(client, key, "fp", "owner", "saved-result", default);
            long after = (await client.ExecuteAsync(["PTTL", key], default)).Integer;

            Assert.InRange(after, 1, before);
            Assert.Equal(("completed", "saved-result"),
                await IdempotencyStore.AcquireAsync(client, key, "fp", "retry", TimeSpan.FromDays(1), default));
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                IdempotencyStore.CompleteAsync(client, key, "fp", "owner", "overwrite", default));

            await client.ExecuteAsync(["PEXPIRE", key, "1"], default);
            await Task.Delay(30);
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                IdempotencyStore.CompleteAsync(client, key, "fp", "owner", "expired", default));
            Assert.Equal("acquired",
                (await IdempotencyStore.AcquireAsync(client, key, "fp", "new-owner", TimeSpan.FromSeconds(10), default)).State);
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                IdempotencyStore.CompleteAsync(client, key, "fp", "owner", "stale-owner", default));
        }

        finally
        {
            await client.ExecuteAsync(["DEL", key], default);
        }
    }

    [LiveFact]
    public async Task Concurrent_clients_have_one_owner()
    {
        var endpoint = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")!.Split(':');
        var options = new RuvioClientOptions { Host = endpoint[0], Port = int.Parse(endpoint[1]) };

        await using var firstConnection = await RuvioConnection.ConnectAsync(options);
        await using var secondConnection = await RuvioConnection.ConnectAsync(options);
        IRuvioClient first = await firstConnection.GetDefaultDatabaseAsync();
        IRuvioClient second = await secondConnection.GetDefaultDatabaseAsync();
        string key = "idempotency-test:{" + Guid.NewGuid().ToString("N") + "}";

        try
        {
            var replies = await Task.WhenAll(
                IdempotencyStore.AcquireAsync(first, key, "fp", "first", TimeSpan.FromSeconds(10), default),
                IdempotencyStore.AcquireAsync(second, key, "fp", "second", TimeSpan.FromSeconds(10), default));

            Assert.Single(replies, reply => reply.State == "acquired");
            Assert.Single(replies, reply => reply.State == "pending");
            string owner = replies[0].State == "acquired" ? "first" : "second";

            await IdempotencyStore.CompleteAsync(first, key, "fp", owner, "result", default);
            Assert.Equal(("completed", "result"),
                await IdempotencyStore.AcquireAsync(second, key, "fp", "retry", TimeSpan.FromSeconds(10), default));
        }

        finally
        {
            await first.ExecuteAsync(["DEL", key], default);
        }

    }

    [LiveFact]
    public async Task Native_binary_command_routes_and_replays_without_text_conversion()
    {
        var endpoint = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")!.Split(':');

        await using var connection = await RuvioConnection.ConnectAsync(endpoint[0], int.Parse(endpoint[1]));
        IRuvioClient client = await connection.GetDefaultDatabaseAsync();
        byte[] key = System.Text.Encoding.UTF8.GetBytes("native-idempotency:" + Guid.NewGuid().ToString("N"));
        byte[] fingerprint = [0, 255, 128];
        byte[] owner = [255, 0];
        byte[] result = Enumerable.Repeat((byte)255, 1_500_000).ToArray();
        ReadOnlyMemory<byte>[] begin = ["IDEM"u8.ToArray(), key, "BEGIN"u8.ToArray(), fingerprint, owner, "10000"u8.ToArray()];
        ReadOnlyMemory<byte>[] complete = ["IDEM"u8.ToArray(), key, "COMPLETE"u8.ToArray(), fingerprint, owner, result];

        try
        {
            Assert.Equal("acquired", (await client.ExecuteBinaryAsync(begin)).Items![0].AsString());
            Assert.Equal(1, (await client.ExecuteBinaryAsync(complete)).Integer);
            Assert.Equal(1, (await client.ExecuteBinaryAsync(complete)).Integer);
            var replay = await client.ExecuteBinaryAsync(begin);

            Assert.Equal("completed", replay.Items![0].AsString());
            Assert.Equal(result, replay.Items[1].Bytes);
        }

        finally
        {
            await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[] { "DEL"u8.ToArray(), key });
        }
    }
}
