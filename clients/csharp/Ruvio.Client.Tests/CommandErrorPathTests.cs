using System.Text.Json;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>Replies of the wrong shape, rejected options and argument checks on the typed command surface.</summary>
public partial class CommandSurfaceTests
{
    private static async Task Rejects<T>(ScriptedSession session, ValueTask<T> call, string reply, params string[] expected)
    {
        await session.Expect(expected);
        await session.Reply(reply);
        await Assert.ThrowsAsync<RuvioException>(async () => await call);
    }

    [Fact(Timeout = 15000)]
    public async Task WrongShapedRepliesAreReportedAsRuvioExceptions()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        await Rejects(session, db.CompleteIdempotencyAsync("k", "fp", "me", "r"), ":5\r\n", "IDEM", "k", "COMPLETE", "fp", "me", "r");
        await Rejects(session, db.CompleteIdempotencyAsync("k", "fp", "me", "r"), "+OK\r\n", "IDEM", "k", "COMPLETE", "fp", "me", "r");
        Assert.False(await Exchange(session, db.CompleteIdempotencyAsync("k", "fp", "me", "r"), ":0\r\n", "IDEM", "k", "COMPLETE", "fp", "me", "r"));

        await Rejects(session, db.ReadChangesAsync(4), "$1\r\nx\r\n", "CHANGES", "4");
        await Rejects(session, db.ReadChangesAsync(4), "*1\r\n*1\r\n:1\r\n", "CHANGES", "4");

        Assert.Empty(await Exchange(session, db.JsonObjectKeysAsync("d", "$.o"), "$-1\r\n", "JSON.OBJKEYS", "d", "$.o"));

        await Rejects(session, db.GeoPositionAsync("g", "a"), ":1\r\n", "GEOPOS", "g", "a");
        await Rejects(session, db.GeoPositionAsync("g", "a"), "*1\r\n:1\r\n", "GEOPOS", "g", "a");
        await Rejects(session, db.GeoPositionAsync("g", "a"), "*1\r\n*1\r\n$1\r\n1\r\n", "GEOPOS", "g", "a");
        IReadOnlyList<(double Longitude, double Latitude)?> points = await Exchange(
            session, db.GeoPositionAsync("g", "a", "b"), "*2\r\n*2\r\n$2\r\n10\r\n$1\r\n5\r\n*-1\r\n", "GEOPOS", "g", "a", "b");
        Assert.Equal((10d, 5d), points[0]);
        Assert.Null(points[1]);

        await Rejects(session, db.GetSortedSetRankAsync("z", "m"), "+OK\r\n", "ZRANK", "z", "m");
        Assert.Null(await Exchange(session, db.GetSortedSetRankAsync("z", "m", descending: true), "$-1\r\n", "ZREVRANK", "z", "m"));
        Assert.Equal(2, await Exchange(session, db.GetSortedSetRankAsync("z", "m"), ":2\r\n", "ZRANK", "z", "m"));

        await Rejects(session, db.GetStreamPendingAsync("s", "g", "-", "+", 10), "*1\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n", "XPENDING", "s", "g", "-", "+", "10");
        IReadOnlyList<StreamPendingEntry> pending = await Exchange(
            session,
            db.GetStreamPendingAsync("s", "g", "1-0", "2-0", 5, "alice", TimeSpan.FromMilliseconds(1500)),
            "*1\r\n*4\r\n$3\r\n1-0\r\n$5\r\nalice\r\n:2000\r\n:3\r\n",
            "XPENDING", "s", "g", "IDLE", "1500", "1-0", "2-0", "5", "alice");
        Assert.Equal(new StreamPendingEntry("1-0", "alice", 2000, 3), Assert.Single(pending));

        await Rejects(session, db.PopListManyAsync(["a"], true), ":1\r\n", "LMPOP", "1", "a", "LEFT", "COUNT", "1");
        Assert.Null(await Exchange(session, db.PopListManyAsync(["a"], false, 2), "$-1\r\n", "LMPOP", "1", "a", "RIGHT", "COUNT", "2"));

        await Rejects(session, db.BitFieldAsync("b", "GET", "u8", "0"), ":1\r\n", "BITFIELD", "b", "GET", "u8", "0");
        await Rejects(session, db.BitFieldAsync("b", "GET", "u8", "0"), "*1\r\n$1\r\nx\r\n", "BITFIELD", "b", "GET", "u8", "0");
        IReadOnlyList<long?> fields = await Exchange(session, db.BitFieldReadOnlyAsync("b", "GET", "u8", "0"), "*2\r\n:7\r\n*-1\r\n", "BITFIELD_RO", "b", "GET", "u8", "0");
        Assert.Equal(7, fields[0]);
        Assert.Null(fields[1]);
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await db.BitFieldAsync("b", null!));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await db.PopListManyAsync(null!, true));

        await Rejects(session, db.PopListLeftBlockingAsync(["a"], TimeSpan.FromSeconds(1)), "*1\r\n$1\r\nx\r\n", "BLPOP", "a", "1");
        await Rejects(session, db.DumpAsync("k"), ":1\r\n", "DUMP", "k");
        Assert.Null(await Exchange(session, db.DumpAsync("k"), "$-1\r\n", "DUMP", "k"));
        Assert.Equal([1, 2], await Exchange(session, db.DumpAsync("k"), "$2\r\n\u0001\u0002\r\n", "DUMP", "k"));
    }

    [Fact(Timeout = 15000)]
    public async Task NullElementsWhereTextIsRequiredAndNonArrayRepliesAreRejected()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        await Rejects(session, db.GetSortedSetRangeByRankAsync("z", 0, -1), "*1\r\n$-1\r\n", "ZRANGE", "z", "0", "-1");
        await Rejects(session, db.GetSortedSetRangeByRankAsync("z", 0, -1), ":3\r\n", "ZRANGE", "z", "0", "-1");
        Assert.Empty(await Exchange(session, db.GetSortedSetRangeByRankAsync("z", 0, -1), "$-1\r\n", "ZRANGE", "z", "0", "-1"));
    }

    [Fact(Timeout = 15000)]
    public async Task OptionCombinationsThatCannotBeSentAreRefusedLocally()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        await Assert.ThrowsAsync<ArgumentNullException>(async () => await db.GetStreamUpdatesAsync(null!, ("s", "0")));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await db.GetStreamUpdatesAsync(new StreamReadOptions { Block = TimeSpan.FromSeconds(-1) }, ("s", "0")));
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await db.GetStreamGroupUpdatesAsync("g", "c", "s", ">", null!));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await db.GetStreamGroupUpdatesAsync("g", "c", "s", ">", new StreamReadOptions { Block = TimeSpan.FromMilliseconds(-1) }));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await db.ClaimStreamEntriesAsync("s", "g", "c", 0, new StreamClaimOptions { Idle = TimeSpan.FromSeconds(1), Time = DateTimeOffset.UtcNow }, "1-0"));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await db.ClaimStreamEntryIdsAsync("s", "g", "c", 0, new StreamClaimOptions { Idle = TimeSpan.FromSeconds(1), Time = DateTimeOffset.UtcNow }, "1-0"));
    }

    [Fact(Timeout = 15000)]
    public async Task StreamClaimAndGroupReadSendEveryOption()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;
        var time = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000);

        IReadOnlyList<string> ids = await Exchange(
            session,
            db.ClaimStreamEntryIdsAsync("s", "g", "c", 100,
                new StreamClaimOptions { Time = time, RetryCount = 3, Force = true, LastId = "9-0" }, "1-0", "2-0"),
            "*1\r\n$3\r\n1-0\r\n",
            "XCLAIM", "s", "g", "c", "100", "1-0", "2-0", "TIME", "1700000000000", "RETRYCOUNT", "3", "FORCE", "LASTID", "9-0", "JUSTID");
        Assert.Equal(["1-0"], ids);

        IReadOnlyList<StreamEntry> entries = await Exchange(
            session,
            db.ClaimStreamEntriesAsync("s", "g", "c", 100, new StreamClaimOptions { Idle = TimeSpan.FromMilliseconds(250.5) }, "1-0"),
            "*0\r\n",
            "XCLAIM", "s", "g", "c", "100", "1-0", "IDLE", "251");
        Assert.Empty(entries);

        IReadOnlyList<StreamReadResult> read = await Exchange(
            session,
            db.GetStreamGroupUpdatesAsync("g", "c", "s", ">", new StreamReadOptions { Count = 5, Block = TimeSpan.FromMilliseconds(10), NoAcknowledge = true }),
            "*-1\r\n",
            "XREADGROUP", "GROUP", "g", "c", "COUNT", "5", "BLOCK", "10", "NOACK", "STREAMS", "s", ">");
        Assert.Empty(read);
    }

    [Fact(Timeout = 15000)]
    public async Task SortedSetAddSendsEachFlagInOrder()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;
        SortedSetAddFlags all = SortedSetAddFlags.IfNotExists | SortedSetAddFlags.IfExists | SortedSetAddFlags.GreaterThan
            | SortedSetAddFlags.LessThan | SortedSetAddFlags.Changed;

        Assert.Equal(1, await Exchange(
            session, db.AddSortedSetAsync("z", all, ("m", 1.5)), ":1\r\n",
            "ZADD", "z", "NX", "XX", "GT", "LT", "CH", "1.5", "m"));
        Assert.Equal(1, await Exchange(
            session, db.AddSortedSetAsync("z", SortedSetAddFlags.None, new SortedSetEntry("m", 2)), ":1\r\n",
            "ZADD", "z", "2", "m"));
        Assert.Null(await Exchange(
            session, db.IncrementSortedSetAsync("z", "m", 1, SortedSetAddFlags.GreaterThan), "$-1\r\n",
            "ZADD", "z", "GT", "INCR", "1", "m"));
    }

    [Fact(Timeout = 15000)]
    public async Task TypedJsonOverloadsWithTypeInfoSerializeThroughTheContext()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        var address = new Address("Izmir", 35000);

        Assert.True(await Exchange(
            session, session.Client.JsonMergeValueAsync("doc", "$", address, AddressContext.Default.Address), "+OK\r\n",
            "JSON.MERGE", "doc", "$", "{\"City\":\"Izmir\",\"Zip\":35000}"));
        Assert.Equal(2, await Exchange(
            session, session.Client.JsonArrayAppendValueAsync("doc", "$.l", address, AddressContext.Default.Address), ":2\r\n",
            "JSON.ARRAPPEND", "doc", "$.l", "{\"City\":\"Izmir\",\"Zip\":35000}"));
    }
}
