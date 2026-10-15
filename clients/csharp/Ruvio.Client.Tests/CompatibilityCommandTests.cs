using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class CompatibilityCommandTests
{
    [Fact(Timeout = 15000)]
    public async Task KeyspaceCommandsSendTheExpectedWordsAndReadTheReplies()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        ValueTask<IReadOnlyList<string>> keys = db.GetKeysAsync("user:*");
        await session.Expect("KEYS", "user:*");
        await session.Reply("*2\r\n$6\r\nuser:1\r\n$6\r\nuser:2\r\n");
        Assert.Equal(new[] { "user:1", "user:2" }, await keys);

        ValueTask<string?> random = db.GetRandomKeyAsync();
        await session.Expect("RANDOMKEY");
        await session.Reply("$1\r\nk\r\n");
        Assert.Equal("k", await random);

        ValueTask<long> touch = db.TouchAsync("a", "b");
        await session.Expect("TOUCH", "a", "b");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await touch);

        ValueTask<bool> renamed = db.RenameIfNotExistsAsync("a", "b");
        await session.Expect("RENAMENX", "a", "b");
        await session.Reply(":0\r\n");
        Assert.False(await renamed);

        ValueTask<long> expire = db.GetExpireTimeAsync("a");
        await session.Expect("EXPIRETIME", "a");
        await session.Reply(":-1\r\n");
        Assert.Equal(-1, await expire);

        expire = db.GetExpireTimeMillisecondsAsync("a");
        await session.Expect("PEXPIRETIME", "a");
        await session.Reply(":-2\r\n");
        Assert.Equal(-2, await expire);

        ValueTask<bool> copied = db.CopyAsync("a", "b");
        await session.Expect("COPY", "a", "b");
        await session.Reply(":1\r\n");
        Assert.True(await copied);
        copied = db.CopyAsync("a", "b", replace: true);
        await session.Expect("COPY", "a", "b", "REPLACE");
        await session.Reply(":1\r\n");
        Assert.True(await copied);

        ValueTask<string> echo = db.EchoAsync("hi");
        await session.Expect("ECHO", "hi");
        await session.Reply("$2\r\nhi\r\n");
        Assert.Equal("hi", await echo);
    }

    [Fact(Timeout = 15000)]
    public async Task IntrospectionCommandsReadNumbersNullsAndLines()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        ValueTask<string?> encoding = db.GetObjectEncodingAsync("k");
        await session.Expect("OBJECT", "ENCODING", "k");
        await session.Reply("$3\r\nraw\r\n");
        Assert.Equal("raw", await encoding);

        ValueTask<long?> number = db.GetObjectIdleSecondsAsync("k");
        await session.Expect("OBJECT", "IDLETIME", "k");
        await session.Reply(":7\r\n");
        Assert.Equal(7L, await number);

        number = db.GetObjectFrequencyAsync("k");
        await session.Expect("OBJECT", "FREQ", "k");
        await session.Reply("$-1\r\n");
        Assert.Null(await number);

        number = db.GetMemoryUsageAsync("k");
        await session.Expect("MEMORY", "USAGE", "k");
        await session.Reply(":64\r\n");
        Assert.Equal(64L, await number);

        ValueTask<long> length = db.GetSlowLogLengthAsync();
        await session.Expect("SLOWLOG", "LEN");
        await session.Reply(":0\r\n");
        Assert.Equal(0, await length);

        ValueTask reset = db.ResetSlowLogAsync();
        await session.Expect("SLOWLOG", "RESET");
        await session.Reply("+OK\r\n");
        await reset;

        ValueTask<IReadOnlyList<RespValue>> entries = db.GetSlowLogAsync(5);
        await session.Expect("SLOWLOG", "GET", "5");
        await session.Reply("*1\r\n*1\r\n:1\r\n");
        Assert.Single(await entries);

        length = db.GetClientIdAsync();
        await session.Expect("CLIENT", "ID");
        await session.Reply(":42\r\n");
        Assert.Equal(42, await length);

        ValueTask<string> line = db.GetClientInfoAsync();
        await session.Expect("CLIENT", "INFO");
        await session.Reply("$7\r\nid=42 x\r\n");
        Assert.Equal("id=42 x", await line);

        line = db.GetClientListAsync();
        await session.Expect("CLIENT", "LIST");
        await session.Reply("$4\r\nid=1\r\n");
        Assert.Equal("id=1", await line);

        length = db.KillClientAsync(42);
        await session.Expect("CLIENT", "KILL", "ID", "42");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await length);
    }

    [Fact(Timeout = 15000)]
    public async Task ListCommandsSendSidesCountsAndTimeouts()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        ValueTask<long> count = db.InsertListAsync("l", before: true, "b", "x");
        await session.Expect("LINSERT", "l", "BEFORE", "b", "x");
        await session.Reply(":4\r\n");
        Assert.Equal(4, await count);

        ValueTask set = db.SetListItemAsync("l", 0, "A");
        await session.Expect("LSET", "l", "0", "A");
        await session.Reply("+OK\r\n");
        await set;

        count = db.RemoveListItemsAsync("l", 0, "x");
        await session.Expect("LREM", "l", "0", "x");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await count);

        ValueTask<long?> position = db.GetListPositionAsync("l", "c");
        await session.Expect("LPOS", "l", "c");
        await session.Reply(":2\r\n");
        Assert.Equal(2L, await position);

        count = db.PushListLeftIfExistsAsync("l", "a", "b");
        await session.Expect("LPUSHX", "l", "a", "b");
        await session.Reply(":5\r\n");
        Assert.Equal(5, await count);

        count = db.PushListRightIfExistsAsync("l", "z");
        await session.Expect("RPUSHX", "l", "z");
        await session.Reply(":0\r\n");
        Assert.Equal(0, await count);

        ValueTask<string?> moved = db.MoveListItemAsync("l", "m", fromLeft: true, toLeft: false);
        await session.Expect("LMOVE", "l", "m", "LEFT", "RIGHT");
        await session.Reply("$1\r\na\r\n");
        Assert.Equal("a", await moved);

        moved = db.PopListRightPushLeftAsync("l", "m");
        await session.Expect("RPOPLPUSH", "l", "m");
        await session.Reply("$-1\r\n");
        Assert.Null(await moved);

        moved = db.MoveListItemBlockingAsync("l", "m", fromLeft: false, toLeft: true, TimeSpan.FromSeconds(2));
        await session.Expect("BLMOVE", "l", "m", "RIGHT", "LEFT", "2");
        await session.Reply("$1\r\nz\r\n");
        Assert.Equal("z", await moved);

        ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> popped = db.PopListManyAsync(["a", "b"], fromLeft: true, 2);
        await session.Expect("LMPOP", "2", "a", "b", "LEFT", "COUNT", "2");
        await session.Reply("*2\r\n$1\r\nb\r\n*2\r\n$1\r\nx\r\n$1\r\ny\r\n");
        KeyValuePair<string, IReadOnlyList<string>>? result = await popped;

        Assert.Equal("b", result!.Value.Key);
        Assert.Equal(new[] { "x", "y" }, result.Value.Value);

        popped = db.PopListManyAsync(["a"], fromLeft: false);
        await session.Expect("LMPOP", "1", "a", "RIGHT", "COUNT", "1");
        await session.Reply("*-1\r\n");
        Assert.Null(await popped);
    }

    [Fact(Timeout = 15000)]
    public async Task DumpRestoreAndBitfieldRoundTripTheirArguments()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        ValueTask<byte[]?> dump = db.DumpAsync("k");
        await session.Expect("DUMP", "k");
        await session.Reply("$3\r\nabc\r\n");
        Assert.Equal("abc"u8.ToArray(), await dump);

        dump = db.DumpAsync("missing");
        await session.Expect("DUMP", "missing");
        await session.Reply("$-1\r\n");
        Assert.Null(await dump);

        ValueTask restore = db.RestoreAsync("k", "abc"u8.ToArray(), 1000, replace: true, absoluteTtl: true);
        await session.Expect("RESTORE", "k", "1000", "abc", "REPLACE", "ABSTTL");
        await session.Reply("+OK\r\n");
        await restore;

        ValueTask<IReadOnlyList<long?>> fields = db.BitFieldAsync("bf", "SET", "u8", "0", "200", "GET", "u8", "0");
        await session.Expect("BITFIELD", "bf", "SET", "u8", "0", "200", "GET", "u8", "0");
        await session.Reply("*2\r\n:0\r\n:200\r\n");
        Assert.Equal(new long?[] { 0, 200 }, await fields);

        fields = db.BitFieldReadOnlyAsync("bf", "GET", "i8", "0");
        await session.Expect("BITFIELD_RO", "bf", "GET", "i8", "0");
        await session.Reply("*1\r\n$-1\r\n");
        Assert.Equal(new long?[] { null }, await fields);
    }

    [Fact(Timeout = 15000)]
    public async Task FillCommandsParseEveryStateAndSendWaitAndSwr()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        ValueTask<FillResult> fill = db.FillGetAsync("c", TimeSpan.FromSeconds(5));
        await session.Expect("FILLGET", "c", "5000");
        await session.Reply("*2\r\n$4\r\nFILL\r\n$1\r\n7\r\n");
        FillResult result = await fill;

        Assert.Equal(FillState.Fill, result.State);
        Assert.Equal(7L, result.Token);

        fill = db.FillGetAsync("c", TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(300));
        await session.Expect("FILLGET", "c", "5000", "WAIT", "300");
        await session.Reply("*2\r\n$3\r\nHIT\r\n$1\r\nv\r\n");
        result = await fill;

        Assert.Equal(FillState.Hit, result.State);
        Assert.Equal("v"u8.ToArray(), result.Value);

        fill = db.FillGetAsync("c", TimeSpan.FromSeconds(5));
        await session.Expect("FILLGET", "c", "5000");
        await session.Reply("*2\r\n$4\r\nBUSY\r\n$-1\r\n");
        Assert.Equal(FillState.Busy, (await fill).State);

        fill = db.FillGetAsync("c", TimeSpan.FromSeconds(5));
        await session.Expect("FILLGET", "c", "5000");
        await session.Reply("*3\r\n$5\r\nSTALE\r\n$3\r\nold\r\n$-1\r\n");
        result = await fill;

        Assert.Equal(FillState.Stale, result.State);
        Assert.Null(result.Token);

        ValueTask<bool> stored = db.FillSetAsync("c", "v", TimeSpan.FromSeconds(10), 7);
        await session.Expect("FILLSET", "c", "v", "10000", "7");
        await session.Reply(":1\r\n");
        Assert.True(await stored);

        stored = db.FillSetAsync("c", "v", TimeSpan.FromSeconds(10), 7, stale: TimeSpan.FromSeconds(30));
        await session.Expect("FILLSET", "c", "v", "10000", "7", "SWR", "30000");
        await session.Reply(":0\r\n");
        Assert.False(await stored);
    }
}
