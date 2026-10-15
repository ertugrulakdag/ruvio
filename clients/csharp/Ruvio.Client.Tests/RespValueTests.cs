using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class RespValueTests
{
    [Fact]
    public void AsStringDoesNotThrowOnAPubsubArray()
    {
        RespValue message = RespValue.Array(
        [
            RespValue.Bulk("message"u8.ToArray()),
            RespValue.Bulk("feat-news"u8.ToArray()),
            RespValue.Bulk("hello-ruvio"u8.ToArray()),
        ]);

        Assert.Null(message.AsString());
        Assert.Equal("hello-ruvio", message.Items![2].AsString());
        Assert.Equal("3", RespValue.FromInteger(3).AsString());
        Assert.True(PubSubMessage.TryParse(message, out PubSubMessage parsed));
        Assert.Equal("feat-news", parsed.Channel);
        Assert.Equal("hello-ruvio", parsed.Payload);
        Assert.False(PubSubMessage.TryParse(
            RespValue.Array(
            [
                RespValue.Bulk("subscribe"u8.ToArray()),
                RespValue.Bulk("feat-news"u8.ToArray()),
                RespValue.FromInteger(1),
            ]),
            out _));
    }

    [Theory]
    [InlineData("message", null, "news", "hi")]
    [InlineData("smessage", null, "slot", "x")]
    public void TryParseReadsExactAndShardedMessages(string kind, string? pattern, string channel, string payload)
    {
        RespValue value = RespValue.Array(
        [
            RespValue.Bulk(System.Text.Encoding.UTF8.GetBytes(kind)),
            RespValue.Bulk(System.Text.Encoding.UTF8.GetBytes(channel)),
            RespValue.Bulk(System.Text.Encoding.UTF8.GetBytes(payload)),
        ]);

        Assert.True(PubSubMessage.TryParse(value, out PubSubMessage parsed));
        Assert.Equal(kind, parsed.Kind);
        Assert.Equal(pattern, parsed.Pattern);
        Assert.Equal(channel, parsed.Channel);
        Assert.Equal(payload, parsed.Payload);
    }

    [Fact]
    public void TryParseReadsPmessage()
    {
        RespValue value = RespValue.Array(
        [
            RespValue.Bulk("pmessage"u8.ToArray()),
            RespValue.Bulk("n*"u8.ToArray()),
            RespValue.Bulk("news"u8.ToArray()),
            RespValue.Bulk("hi"u8.ToArray()),
        ]);

        Assert.True(PubSubMessage.TryParse(value, out PubSubMessage parsed));
        Assert.Equal("n*", parsed.Pattern);
        Assert.Equal("news", parsed.Channel);
        Assert.Equal("hi", parsed.Payload);
    }

    [Fact]
    public void TryParseRejectsEmptyAndIntegerReplies()
    {
        Assert.False(PubSubMessage.TryParse(RespValue.Array([]), out _));
        Assert.False(PubSubMessage.TryParse(RespValue.FromInteger(1), out _));
        Assert.False(PubSubMessage.TryParse(RespValue.Null(), out _));
    }
}
