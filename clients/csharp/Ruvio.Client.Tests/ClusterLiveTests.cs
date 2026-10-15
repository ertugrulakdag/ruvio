using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class ClusterLiveTests
{
    [Fact(Timeout = 15000)]
    public async Task RoutesKeysAndSplitsMgetAgainstALiveCluster()
    {
        string? address = Environment.GetEnvironmentVariable("RUVIO_CLUSTER_ADDR");

        if (string.IsNullOrWhiteSpace(address))
        {
            return;
        }

        string[] parts = address.Split(':');

        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(parts[0], int.Parse(parts[1]));
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        Assert.True(client.IsCluster);
        Assert.True(client.ShardCount >= 2);
        Assert.Equal(11058, RuvioClient.GetHashSlot("somekey"));
        await client.FlushDatabaseAsync();
        await client.SetAsync("{user1000}.following", "left");
        await client.SetAsync("somekey", "right");
        Assert.Equal(
            ["left", "right"],
            await client.GetManyAsync("{user1000}.following", "somekey"));
        Assert.Equal(11058, await client.GetClusterKeySlotAsync("somekey"));
        Assert.Contains("cluster_state:ok", await client.GetClusterInfoAsync());
        Assert.Equal(2, await client.GetDatabaseSizeAsync());
        await client.ReadOnlyAsync();
        await client.ReadWriteAsync();
    }

    [Fact(Timeout = 15000)]
    public async Task RenamesAKeyOntoAnotherSlot()
    {
        string? address = Environment.GetEnvironmentVariable("RUVIO_CLUSTER_ADDR");

        if (string.IsNullOrWhiteSpace(address))
        {
            return;
        }

        string[] parts = address.Split(':');

        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(parts[0], int.Parse(parts[1]));
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        const string source = "portal-rename-cs";
        const string destination = "key1-rename-cs";

        Assert.NotEqual(RuvioClient.GetHashSlot(source), RuvioClient.GetHashSlot(destination));
        await client.DeleteAsync([source, destination]);
        await client.SetAsync(source, "from-screen");
        await client.ExecuteAsync("RENAME", source, destination);
        Assert.Equal("from-screen", await client.GetStringAsync(destination));
        Assert.Null(await client.GetStringAsync(source));
        await client.DeleteAsync(destination);
    }
}
