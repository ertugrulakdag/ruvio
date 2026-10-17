using System.Text;
using Xunit;

namespace Ruvio.Client.Tests;

public class KeywordTests
{
    [Theory]
    [InlineData(GeoUnit.Meters, "m")]
    [InlineData(GeoUnit.Kilometers, "km")]
    [InlineData(GeoUnit.Feet, "ft")]
    [InlineData(GeoUnit.Miles, "mi")]
    public void GeoUnitTableMatchesTheServerWords(GeoUnit unit, string word)
    {
        Assert.Equal(word, Keywords.Word(unit));
        Assert.Equal(Encode(new RespCommand("GEODIST", "k", "a", "b", word)), Encode(new RespCommand("GEODIST", "k", "a", "b", Keywords.Arg(unit))));
    }

    [Theory]
    [InlineData(GeoOrder.Ascending, "ASC")]
    [InlineData(GeoOrder.Descending, "DESC")]
    public void GeoOrderTableMatchesTheServerWords(GeoOrder order, string word)
    {
        Assert.Equal(word, Keywords.Word(order));
        Assert.Equal(Encode(new RespCommand("GEORADIUSBYMEMBER", "k", "m", "1", "km", word)), Encode(new RespCommand("GEORADIUSBYMEMBER", "k", "m", "1", "km", Keywords.Arg(order))));
    }

    [Theory]
    [InlineData(InsertPosition.Before, "BEFORE")]
    [InlineData(InsertPosition.After, "AFTER")]
    public void InsertPositionTableMatchesTheServerWords(InsertPosition position, string word)
    {
        Assert.Equal(word, Keywords.Word(position));
        Assert.Equal(Encode(new RespCommand("LINSERT", "k", word, "p", "v")), Encode(new RespCommand("LINSERT", "k", Keywords.Arg(position), "p", "v")));
    }

    [Theory]
    [InlineData(ListSide.Left, "LEFT")]
    [InlineData(ListSide.Right, "RIGHT")]
    public void ListSideTableMatchesTheServerWords(ListSide side, string word)
    {
        Assert.Equal(word, Keywords.Word(side));
        Assert.Equal(Encode(new RespCommand("BLMOVE", "a", "b", word, "LEFT", 1)), Encode(new RespCommand("BLMOVE", "a", "b", Keywords.Arg(side), "LEFT", 1)));
    }

    [Fact]
    public void TokenArgumentsSurviveTheListPath()
    {
        RespCommand command = new RespCommand("LMOVE", "a", "b", Keywords.Arg(ListSide.Right), Keywords.Arg(ListSide.Left)).With(Keywords.Arg(GeoUnit.Miles));

        Assert.Equal(["LMOVE", "a", "b", "RIGHT", "LEFT", "mi"], command.AsList());
        Assert.Equal("RIGHT", new RespCommand("LMOVE", "a", "b", Keywords.Arg(ListSide.Right)).TextAt(3));
    }

    [Fact]
    public void KeyTypeTableListsEveryScanType()
    {
        string[] expected = ["string", "json", "list", "set", "hash", "zset", "bloomfilter", "stream", "lease", "limit", "semaphore", "once"];
        string[] actual = Enum.GetValues<KeyType>().Select(Keywords.Word).ToArray();

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void BitFieldHelpersProduceTheWireWords()
    {
        Assert.Equal("i16", BitFieldType.Signed(16));
        Assert.Equal("u8", BitFieldType.Unsigned(8));
        Assert.Same(BitFieldType.Unsigned(8), BitFieldType.Unsigned(8));
        Assert.Equal("i64", BitFieldType.Signed(64));
        Assert.Equal("u63", BitFieldType.Unsigned(63));
        Assert.Throws<ArgumentOutOfRangeException>(() => BitFieldType.Signed(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => BitFieldType.Signed(65));
        Assert.Throws<ArgumentOutOfRangeException>(() => BitFieldType.Unsigned(64));

        string[] words = BitFieldStep.Words(
            BitFieldStep.Overflow(BitFieldOverflow.Saturate),
            [
                BitFieldStep.IncrBy("u8", 0, 10),
                BitFieldStep.Set("i4", 8, -3),
                BitFieldStep.Overflow(BitFieldOverflow.Fail),
                BitFieldStep.Overflow(BitFieldOverflow.Wrap),
                BitFieldStep.Get("u8", 0),
            ]);

        Assert.Equal(
            ["OVERFLOW", "SAT", "INCRBY", "u8", "0", "10", "SET", "i4", "8", "-3", "OVERFLOW", "FAIL", "OVERFLOW", "WRAP", "GET", "u8", "0"],
            words);
    }

    private static string Encode(RespCommand command)
    {
        using PooledFrame frame = command.EncodePooled();

        return Encoding.UTF8.GetString(frame.Buffer, 0, frame.Length);
    }
}

public class KeywordWireTests
{
    [Fact(Timeout = 15000)]
    public async Task TypedOverloadsSendTheSameWordsAsTheLegacyOnes()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        ValueTask<double?> distance = db.GeoDistanceAsync("g", "a", "b", GeoUnit.Kilometers);
        await session.Expect("GEODIST", "g", "a", "b", "km");
        await session.Reply("$3\r\n1.5\r\n");
        Assert.Equal(1.5, await distance);

        ValueTask<IReadOnlyList<string>> near = db.GeoRadiusByMemberAsync("g", "a", 5, GeoUnit.Miles, GeoOrder.Descending);
        await session.Expect("GEORADIUSBYMEMBER", "g", "a", "5", "mi", "DESC");
        await session.Reply("*0\r\n");
        Assert.Empty(await near);

        near = db.GeoSearchAsync("g", 1, 2, 3, GeoUnit.Feet);
        await session.Expect("GEOSEARCH", "g", "FROMLONLAT", "1", "2", "BYRADIUS", "3", "ft", "ASC");
        await session.Reply("*0\r\n");
        Assert.Empty(await near);

        ValueTask<long> inserted = db.InsertListAsync("l", InsertPosition.After, "p", "v");
        await session.Expect("LINSERT", "l", "AFTER", "p", "v");
        await session.Reply(":2\r\n");
        Assert.Equal(2, await inserted);

        ValueTask<string?> moved = db.MoveListItemAsync("l", "m", ListSide.Right, ListSide.Left);
        await session.Expect("LMOVE", "l", "m", "RIGHT", "LEFT");
        await session.Reply("$-1\r\n");
        Assert.Null(await moved);

        ValueTask<ScanPage> page = db.ScanAsync(0, null, null, KeyType.SortedSet);
        await session.Expect("SCAN", "0", "TYPE", "zset");
        await session.Reply("*2\r\n$1\r\n0\r\n*0\r\n");
        Assert.Empty((await page).Keys);

        ValueTask<IReadOnlyList<long?>> fields = db.BitFieldAsync(
            "bf",
            BitFieldStep.Overflow(BitFieldOverflow.Saturate),
            BitFieldStep.IncrBy(BitFieldType.Unsigned(8), 0, 10));
        await session.Expect("BITFIELD", "bf", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "10");
        await session.Reply("*1\r\n:10\r\n");
        Assert.Equal(new long?[] { 10 }, await fields);
    }
}
