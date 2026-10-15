using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;
using Xunit;

namespace Ruvio.AspNetCore.OutputCaching.Tests;

public sealed class KeyFormatTests
{
    private static string Hex(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)));

    [Theory]
    [InlineData("/orders?page=1")]
    [InlineData("sipariş-🚀")]
    public void KeysKeepTheDocumentedHashFormat(string value)
    {
        var store = new RuvioOutputCacheStore(
            () => throw new InvalidOperationException("No client needed."),
            Options.Create(new RuvioOutputCacheOptions { InstanceName = "app-ç", MaxKeyBytes = 8192 }));
        string prefix = "ruvio:output:v1:{" + Hex("app-ç") + "}:";
        string large = new('ş', 3000);

        Assert.Equal(prefix + "entry:" + Hex(value), store.EntryKey(value));
        Assert.Equal(prefix + "tag:" + Hex(value), store.TagKey(value));
        Assert.Equal(prefix + "tag:" + Hex(large), store.TagKey(large));
    }
}
