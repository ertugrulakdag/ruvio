using System.Text;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class ProtocolEdgeTests
{
    private sealed class OneByteStream(byte[] payload) : MemoryStream(payload)
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Yield();

            return Read(buffer.Span[..Math.Min(1, buffer.Length)]);
        }
    }

    private static Stream StreamOf(string text, bool fragmented)
    {
        byte[] bytes = Encoding.ASCII.GetBytes(text);

        return fragmented ? new OneByteStream(bytes) : new MemoryStream(bytes);
    }

    private static async Task<RuvioException> ReadFails(string text, bool fragmented)
    {
        using var reader = new RespReader(StreamOf(text, fragmented));

        return await Assert.ThrowsAsync<RuvioException>(async () => await reader.ReadAsync(CancellationToken.None));
    }

    public static IEnumerable<object[]> Modes => [[false], [true]];

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task ReaderRejectsMalformedIntegersAndLengths(bool fragmented)
    {
        Assert.Contains("integer", (await ReadFails(":abc\r\n", fragmented)).Message);
        Assert.Contains("integer", (await ReadFails(":-\r\n", fragmented)).Message);
        Assert.Contains("integer", (await ReadFails(":\r\n", fragmented)).Message);
        Assert.Contains("integer", (await ReadFails(":99999999999999999999\r\n", fragmented)).Message);
        Assert.Contains("integer", (await ReadFails("$x\r\n", fragmented)).Message);
        Assert.Contains("out of range", (await ReadFails("$-2\r\n", fragmented)).Message);
        Assert.Contains("out of range", (await ReadFails($"${RespCodec.MaxBulkBytes + 1}\r\n", fragmented)).Message);
        Assert.Contains("out of range", (await ReadFails("*-2\r\n", fragmented)).Message);
        Assert.Contains("out of range", (await ReadFails($"*{RespCodec.MaxArrayLength + 1}\r\n", fragmented)).Message);
        Assert.Contains("unexpected RESP prefix", (await ReadFails("!oops\r\n", fragmented)).Message);
        Assert.Contains("unexpected RESP prefix", (await ReadFails("*1\r\n~\r\n", fragmented)).Message);
        Assert.Contains("not terminated", (await ReadFails("$2\r\nabXY", fragmented)).Message);
        Assert.Contains("LF after CR", (await ReadFails("+OK\rX\r\n", fragmented)).Message);
        Assert.Contains("LF after CR", (await ReadFails(":1\rX\r\n", fragmented)).Message);
        Assert.Contains("LF after CR", (await ReadFails("-ERR\rX\r\n", fragmented)).Message);
    }

    [Fact]
    public async Task ReaderRejectsOverlongLines()
    {
        string longLine = "+" + new string('x', RespCodec.MaxLineBytes + 8);

        Assert.Contains("too long", (await ReadFails(longLine, false)).Message);
        Assert.Contains("too long", (await ReadFails("-" + new string('x', RespCodec.MaxLineBytes + 8), false)).Message);
        Assert.Contains("too long", (await ReadFails(longLine + "\r\n", false)).Message);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task ReaderTruncationReportsWhetherTheReplyStarted(bool fragmented)
    {
        foreach (string text in new[] { "+OK", "-ERR", ":12", "$5\r\nab", "$2\r\nab", "*2\r\n:1\r\n", "$3", "*" })
        {
            using var reader = new RespReader(StreamOf(text, fragmented));
            reader.PrepareReply();
            RuvioException error = await Assert.ThrowsAsync<RuvioException>(
                async () => await reader.ReadAsync(CancellationToken.None));

            Assert.False(error.ClosedBeforeReply, text);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task ReaderReadsNullArraysEmptyArraysAndErrorText(bool fragmented)
    {
        using var reader = new RespReader(StreamOf("*-1\r\n*0\r\n-ERR bad thing\r\n$0\r\n\r\n", fragmented));

        Assert.Equal(RespKind.Null, (await reader.ReadAsync(CancellationToken.None)).Kind);
        Assert.Empty((await reader.ReadAsync(CancellationToken.None)).Items!);
        RespValue error = await reader.ReadAsync(CancellationToken.None);
        Assert.Equal("ERR bad thing", error.Text);
        Assert.Equal("ERR bad thing", error.AsString());
        Assert.Equal("", (await reader.ReadAsync(CancellationToken.None)).AsString());
    }

    [Fact]
    public async Task ReaderGrowsItsBufferForALargeBulkAndArray()
    {
        string big = new('z', 100_000);
        using var reader = new RespReader(StreamOf($"${big.Length}\r\n{big}\r\n*3\r\n:1\r\n:2\r\n:3\r\n", true));

        Assert.Equal(big, (await reader.ReadAsync(CancellationToken.None)).AsString());
        Assert.Equal(3, (await reader.ReadAsync(CancellationToken.None)).Items!.Count);
    }

    [Fact]
    public async Task ReaderHonoursCancellationWhileWaitingForBytes()
    {
        var pipe = new System.IO.Pipes.AnonymousPipeServerStream(System.IO.Pipes.PipeDirection.In);
        using var reader = new RespReader(pipe);
        using var cancel = new CancellationTokenSource(50);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.ReadAsync(cancel.Token));
        pipe.Dispose();
    }

    [Fact]
    public void EncoderRejectsEmptyCommandsAndAcceptsEveryListShape()
    {
        Assert.Throws<ArgumentException>(() => RespCodec.Encode(Array.Empty<string>()));
        Assert.Throws<ArgumentException>(() => RespCodec.Encode(new List<string>()));
        Assert.Throws<ArgumentException>(() => RespCodec.Encode(Array.Empty<ReadOnlyMemory<byte>>()));
        Assert.Throws<ArgumentException>(() => RespCodec.EncodeMany([new List<string>()]));

        byte[] fromArray = RespCodec.Encode(new[] { "SET", "k", "é" });
        byte[] fromList = RespCodec.Encode(new List<string> { "SET", "k", "é" });

        Assert.Equal(fromArray, fromList);
        Assert.Equal("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nÃ©\r\n", Encoding.Latin1.GetString(fromArray));
        Assert.Equal(
            Encoding.ASCII.GetBytes("*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
            RespCodec.EncodeMany([["PING"], ["GET", "k"]]));

        // A frame above the 2048 byte stack threshold still round-trips.
        string large = new('q', 5000);
        byte[] big = RespCodec.Encode(new[] { "SET", "k", large });

        Assert.Equal(5000 + "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5000\r\n\r\n".Length, big.Length);

        using PooledFrame verb1 = RespCodec.WriteVerb("PING"u8, "hello");
        Assert.Equal("*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n", Encoding.ASCII.GetString(verb1.Buffer, 0, verb1.Length));
        string[] many = Enumerable.Range(0, 40).Select(i => "a" + i).ToArray();
        using PooledFrame verb2 = RespCodec.WriteVerb("DEL"u8, many);
        Assert.StartsWith("*41\r\n$3\r\nDEL\r\n$2\r\na0\r\n", Encoding.ASCII.GetString(verb2.Buffer, 0, verb2.Length));
        using PooledFrame verb3 = RespCodec.WriteVerb("DEL"u8, many.AsSpan(0, 2));
        Assert.StartsWith("*3\r\n", Encoding.ASCII.GetString(verb3.Buffer, 0, verb3.Length));
        using PooledFrame copy = RespCodec.CopyPooled("abc"u8);
        Assert.Equal(3, copy.Length);
    }

    [Fact]
    public void RespCommandStoresInlineArgumentsAndSpillsToAList()
    {
        var command = new RespCommand("SET", "k", 42L);

        Assert.Equal(3, command.Count);
        Assert.Equal("SET", command.Name);
        Assert.Equal("k", command.TextAt(1));
        Assert.Null(command.TextAt(2));
        Assert.False(command.IsCallerList);
        Assert.Equal(["SET", "k", "42"], command.AsList());

        RespCommand five = new("C", "a", "b", "c", "d");
        Assert.Equal(["C", "a", "b", "c", "d"], five.AsList());
        RespCommand six = new("C", "a", "b", "c", "d", "e");
        Assert.Equal(["C", "a", "b", "c", "d", "e"], six.AsList());
        Assert.Equal("e", six.TextAt(5));
        Assert.Throws<ArgumentOutOfRangeException>(() => six.TextAt(6));

        // With() appends at every inline position, then spills into a caller-independent list.
        RespCommand grown = new("X");
        for (int i = 1; i <= 8; i++)
        {
            grown = grown.With(i % 2 == 0 ? (RespArg)i : (RespArg)("s" + i));
        }

        Assert.Equal(["X", "s1", "2", "s3", "4", "s5", "6", "s7", "8"], grown.AsList());
        Assert.True(grown.IsCallerList);
        Assert.Equal(9, grown.Count);

        Assert.Equal(RespCodec.Encode(grown.AsList()), Drain(grown.EncodePooled()));
        Assert.Equal(RespCodec.Encode(new[] { "SET", "k", "-42" }), Drain(new RespCommand("SET", "k", -42L).EncodePooled()));
        Assert.Equal(RespCodec.Encode(new[] { "GET", "é" }), Drain(new RespCommand("GET", "é").EncodePooled()));
        Assert.Equal(RespCodec.Encode(new[] { "ZZ", "a", long.MinValue.ToString() }), Drain(new RespCommand("ZZ", "a", long.MinValue).EncodePooled()));
        Assert.Equal(RespCodec.Encode(new[] { "ZZ", "0" }), Drain(new RespCommand("ZZ", 0L).EncodePooled()));

        Assert.Throws<ArgumentNullException>(() => new RespCommand((string)null!));
        Assert.Throws<ArgumentNullException>(() => new RespCommand((IReadOnlyList<string>)null!));
        Assert.Throws<ArgumentNullException>(() => { RespArg a = (string)null!; });
        Assert.Null(new RespCommand(Array.Empty<string>()).Name);

        RespCommand fromArray = new string[] { "A", "b" };
        RespCommand fromList = new List<string> { "A", "b" };
        Assert.Equal(fromArray.AsList(), fromList.AsList());
        Assert.True(fromArray.IsCallerList);
    }

    [Fact]
    public void KeyedCommandsPickInlineOrListStorage()
    {
        RespCommand small = RespCommand.Keyed("DEL", "k", ["a", "b"]);
        Assert.False(small.IsCallerList);
        Assert.Equal(["DEL", "k", "a", "b"], small.AsList());
        RespCommand large = RespCommand.Keyed("DEL", "k", ["a", "b", "c", "d", "e"]);
        Assert.Equal(["DEL", "k", "a", "b", "c", "d", "e"], large.AsList());
        Assert.Throws<ArgumentNullException>(() => RespCommand.Keyed("DEL", "k", null!));

        RespCommand pairs = RespCommand.KeyedPairs("HSET", "h", [("f", "v")]);
        Assert.Equal(["HSET", "h", "f", "v"], pairs.AsList());
        RespCommand twoPairs = RespCommand.KeyedPairs("HSET", "h", [("f", "v"), ("g", "w")]);
        Assert.Equal(["HSET", "h", "f", "v", "g", "w"], twoPairs.AsList());
        RespCommand threePairs = RespCommand.KeyedPairs("HSET", "h", [("a", "1"), ("b", "2"), ("c", "3")]);
        Assert.Equal(8, threePairs.Count);
        Assert.Throws<ArgumentNullException>(() => RespCommand.KeyedPairs("HSET", "h", null!));
    }

    private static byte[] Drain(PooledFrame frame)
    {
        using (frame)
        {
            return frame.Buffer.AsSpan(0, frame.Length).ToArray();
        }
    }

    [Fact]
    public void RespValueConversionsCoverEveryKind()
    {
        Assert.Equal("5", RespValue.FromInteger(5).AsString());
        Assert.Equal("100000", RespValue.FromInteger(100000).AsString());
        Assert.Equal(-2, RespValue.FromInteger(-2).Integer);
        Assert.Equal(-3, RespValue.FromInteger(-3).Integer);
        Assert.Equal(255, RespValue.FromInteger(255).Integer);
        Assert.Equal(256, RespValue.FromInteger(256).Integer);
        Assert.Null(RespValue.Null().AsString());
        Assert.Null(RespValue.Array([]).AsString());
        Assert.Equal("fine", RespValue.Simple("fine").AsString());
        Assert.Same(RespValue.Simple("OK"), RespValue.Simple("OK"u8));
        Assert.Same(RespValue.Simple("PONG"), RespValue.Simple("PONG"u8));
        Assert.Same(RespValue.Simple("QUEUED"), RespValue.Simple("QUEUED"u8));
        Assert.Equal("other", RespValue.Simple("other"u8).Text);
        Assert.Equal("", RespValue.Bulk([]).AsString());
        Assert.Same(RespValue.Bulk([]), RespValue.Bulk([]));
        Assert.Equal("é", RespValue.Bulk(Encoding.UTF8.GetBytes("é")).AsString());
        Assert.Throws<RuvioException>(() => RespValue.Bulk([0xFF, 0xFE]).AsString());
        Assert.Equal(RespKind.Error, RespValue.Error("x").Kind);
    }

    [Fact]
    public void ConvertReportsNonUtf8AndOverflow()
    {
        Assert.Throws<RuvioException>(() => RespConvert.FromBulk<string>([0xFF, 0xFE]));
        Assert.Throws<RuvioException>(() => RespConvert.FromBulk<int>(Encoding.ASCII.GetBytes("3000000000")));
        Assert.Throws<RuvioException>(() => RespConvert.FromBulk<int>(Encoding.ASCII.GetBytes("-3000000000")));
        Assert.Equal(-7L, RespConvert.FromBulk<long?>(Encoding.ASCII.GetBytes("-7")));
    }

    private static ValueTask<RespValue> Pending(RespValue value) => new(value);

    [Fact]
    public async Task TypedReplyReadersRejectTheWrongShape()
    {
        RespValue text = RespValue.Bulk("x"u8.ToArray());

        Assert.Equal(3, await RespRead.Integer(Pending(RespValue.FromInteger(3)), "N"));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.Integer(Pending(text), "N"));
        Assert.True(await RespRead.Flag(Pending(RespValue.FromInteger(1)), "F"));
        Assert.False(await RespRead.Flag(Pending(RespValue.FromInteger(0)), "F"));
        Assert.True(await RespRead.NotNull(Pending(text)));
        Assert.False(await RespRead.NotNull(Pending(RespValue.Null())));
        await RespRead.Ok(Pending(RespValue.Simple("OK")));
        Assert.Null(await RespRead.OptionalInteger(Pending(RespValue.Null()), "O"));
        Assert.Equal(9, await RespRead.OptionalInteger(Pending(RespValue.FromInteger(9)), "O"));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.OptionalInteger(Pending(text), "O"));
        Assert.Null(await RespRead.Text(Pending(RespValue.Null())));
        Assert.Equal("x", await RespRead.Text(Pending(text)));
        Assert.Equal("x", await RespRead.RequiredText(Pending(text), "T"));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.RequiredText(Pending(RespValue.Null()), "T"));
        Assert.Equal(1.5, await RespRead.Double(Pending(RespValue.Bulk("1.5"u8.ToArray())), "D"));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.Double(Pending(RespValue.Null()), "D"));

        Assert.Empty(await RespRead.Strings(Pending(RespValue.Null()), "S"));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.Strings(Pending(text), "S"));
        Assert.Equal(["x", ""], await RespRead.Strings(Pending(RespValue.Array([text, RespValue.Null()])), "S"));
    }

    [Fact]
    public async Task FlagsScoresEntriesAndPairsValidateTheirShapes()
    {
        RespValue one = RespValue.FromInteger(1), zero = RespValue.FromInteger(0);

        Assert.Equal([true, false], await RespRead.Flags(Pending(RespValue.Array([one, zero])), "F"));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.Flags(Pending(RespValue.Null()), "F"));
        RuvioException full = await Assert.ThrowsAsync<RuvioException>(
            async () => await RespRead.Flags(Pending(RespValue.Array([one, RespValue.Error("ERR full")])), "F"));
        Assert.Contains("full", full.Message);
        await Assert.ThrowsAsync<RuvioException>(
            async () => await RespRead.Flags(Pending(RespValue.Array([RespValue.Bulk("x"u8.ToArray())])), "F"));

        IReadOnlyList<double?> scores = await RespRead.Scores(
            Pending(RespValue.Array([RespValue.Null(), RespValue.Bulk("2.5"u8.ToArray()), RespValue.Bulk("inf"u8.ToArray()), RespValue.Bulk("-inf"u8.ToArray()), RespValue.Bulk("+inf"u8.ToArray())])), "S");
        Assert.Equal([null, 2.5, double.PositiveInfinity, double.NegativeInfinity, double.PositiveInfinity], scores);
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.Scores(Pending(one), "S"));

        RespValue member = RespValue.Bulk("m"u8.ToArray()), score = RespValue.Bulk("3"u8.ToArray());

        IReadOnlyList<SortedSetEntry> entries = await RespRead.Entries(Pending(RespValue.Array([member, score])), "E");
        Assert.Equal("m", Assert.Single(entries).Member);
        Assert.Empty(await RespRead.Entries(Pending(RespValue.Null()), "E"));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.Entries(Pending(RespValue.Array([member])), "E"));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.Entries(Pending(one), "E"));

        Assert.Equal("m", Assert.Single(await RespRead.Pairs(Pending(RespValue.Array([member, score])), "P")).Key);
        Assert.Empty(await RespRead.Pairs(Pending(RespValue.Null()), "P"));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.Pairs(Pending(RespValue.Array([member])), "P"));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.Pairs(Pending(one), "P"));

        Assert.Equal("1", RespRead.Number(1L));
        Assert.Equal("+inf", RespRead.Number(double.PositiveInfinity));
        Assert.Equal("-inf", RespRead.Number(double.NegativeInfinity));
        Assert.Equal("0.1", RespRead.Number(0.1));
        Assert.Equal(1, RespRead.Milliseconds(TimeSpan.FromTicks(1)));
        Assert.Equal(1500, RespRead.Milliseconds(TimeSpan.FromMilliseconds(1500)));
        Assert.Equal(2, RespRead.Seconds(TimeSpan.FromSeconds(1.2)));
    }

    [Fact]
    public async Task ScanPagesBloomInfoListPopAndIdempotencyValidateTheirShapes()
    {
        RespValue Page(string cursor, params string[] items) => RespValue.Array(
            [RespValue.Bulk(Encoding.ASCII.GetBytes(cursor)), RespValue.Array([.. items.Select(i => RespValue.Bulk(Encoding.ASCII.GetBytes(i)))])]);

        SetScanPage set = await RespRead.SetScan(Pending(Page("7", "a", "b")));
        Assert.Equal(7, set.Cursor);
        Assert.Equal(["a", "b"], set.Members);
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.SetScan(Pending(RespValue.Array([]))));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.SetScan(Pending(RespValue.Null())));
        SortedSetScanPage zset = await RespRead.SortedSetScan(Pending(Page("0", "m", "1")));
        Assert.Single(zset.Entries);
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.SortedSetScan(Pending(Page("0", "m"))));

        RespValue Info(params (string Name, RespValue Value)[] fields) =>
            RespValue.Array([.. fields.SelectMany(f => new[] { RespValue.Bulk(Encoding.ASCII.GetBytes(f.Name)), f.Value })]);

        BloomFilterInfo info = await RespRead.BloomInfo(Pending(Info(
            ("Capacity", RespValue.FromInteger(100)), ("Size", RespValue.FromInteger(5)),
            ("Number of filters", RespValue.FromInteger(2)), ("Number of items inserted", RespValue.FromInteger(9)),
            ("Expansion rate", RespValue.FromInteger(2)), ("Unknown", RespValue.FromInteger(1)))));
        Assert.Equal(100, info.Capacity);
        Assert.Equal(2, info.ExpansionRate);
        BloomFilterInfo noExpansion = await RespRead.BloomInfo(Pending(Info(("Expansion rate", RespValue.Null()))));
        Assert.Null(noExpansion.ExpansionRate);
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.BloomInfo(Pending(RespValue.Null())));
        await Assert.ThrowsAsync<RuvioException>(
            async () => await RespRead.BloomInfo(Pending(RespValue.Array([RespValue.FromInteger(1)]))));

        Assert.Null(await RespRead.ListPop(Pending(RespValue.Null()), "LMPOP"));
        var popped = await RespRead.ListPop(Pending(RespValue.Array([RespValue.Bulk("k"u8.ToArray()), RespValue.Array([RespValue.Bulk("v"u8.ToArray())])])), "LMPOP");
        Assert.Equal("k", popped!.Value.Key);
        Assert.Equal(["v"], popped.Value.Value);
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.ListPop(Pending(RespValue.FromInteger(1)), "LMPOP"));
        await Assert.ThrowsAsync<RuvioException>(
            async () => await RespRead.ListPop(Pending(RespValue.Array([RespValue.Bulk("k"u8.ToArray()), RespValue.FromInteger(1)])), "LMPOP"));

        RespValue Idem(params RespValue[] parts) => RespValue.Array(parts);
        RespValue Word(string w) => RespValue.Bulk(Encoding.ASCII.GetBytes(w));

        Assert.Equal(IdempotencyState.Acquired, (await RespRead.Idempotency(Pending(Idem(Word("acquired"))))).State);
        Assert.Equal(IdempotencyState.Pending, (await RespRead.Idempotency(Pending(Idem(Word("pending"))))).State);
        Assert.Equal(IdempotencyState.Conflict, (await RespRead.Idempotency(Pending(Idem(Word("conflict"))))).State);
        IdempotencyBegin done = await RespRead.Idempotency(Pending(Idem(Word("completed"), Word("body"))));
        Assert.Equal(IdempotencyState.Completed, done.State);
        Assert.Equal("body"u8.ToArray(), done.Result);
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.Idempotency(Pending(Idem(Word("completed")))));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.Idempotency(Pending(Idem(Word("weird")))));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.Idempotency(Pending(RespValue.Array([]))));
        await Assert.ThrowsAsync<RuvioException>(async () => await RespRead.Idempotency(Pending(RespValue.Null())));
    }

    [Fact]
    public void WordHelpersMapEnumsAndRejectUnknownValues()
    {
        Assert.Equal("AND", RespRead.Word(BitOperation.And));
        Assert.Equal("OR", RespRead.Word(BitOperation.Or));
        Assert.Equal("XOR", RespRead.Word(BitOperation.Xor));
        Assert.Equal("NOT", RespRead.Word(BitOperation.Not));
        Assert.Throws<ArgumentOutOfRangeException>(() => RespRead.Word((BitOperation)99));
        Assert.Equal("BIT", RespRead.Word(BitUnit.Bit));
        Assert.Equal("BYTE", RespRead.Word(BitUnit.Byte));
        Assert.Equal("SUM", RespRead.Word(SortedSetAggregate.Sum));
        Assert.Equal("MIN", RespRead.Word(SortedSetAggregate.Min));
        Assert.Equal("MAX", RespRead.Word(SortedSetAggregate.Max));
        Assert.Throws<ArgumentOutOfRangeException>(() => RespRead.Word((SortedSetAggregate)99));
        Assert.Equal("string", RespRead.Word(JsonIndexType.String));
        Assert.Equal("number", RespRead.Word(JsonIndexType.Number));
        Assert.Equal("bool", RespRead.Word(JsonIndexType.Bool));
        Assert.Throws<ArgumentOutOfRangeException>(() => RespRead.Word((JsonIndexType)99));
    }

    [Fact]
    public void ByteScanFindsBytesInVectorBlocksAndTheTail()
    {
        var data = new byte[200];

        Assert.Equal(-1, ByteScan.IndexOf(data, 7));
        data[150] = 7;
        Assert.Equal(150, ByteScan.IndexOf(data, 7));
        data[3] = 7;
        Assert.Equal(3, ByteScan.IndexOf(data, 7));
        data[3] = 0;
        data[150] = 0;
        data[199] = 7;
        Assert.Equal(199, ByteScan.IndexOf(data, 7));
        Assert.Equal(2, ByteScan.IndexOf(new byte[] { 0, 0, 7 }, 7));
        Assert.Equal(-1, ByteScan.IndexOf(ReadOnlySpan<byte>.Empty, 7));
    }
}
