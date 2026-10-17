using System.Globalization;
using System.Text;

namespace Ruvio.Client;

/// <summary>
/// Wire words for the typed keyword enums. <c>Word</c> returns a shared string literal for the list-based command paths;
/// <c>Arg</c> returns a shared pre-encoded token, so the inline <see cref="RespCommand"/> path writes the bytes
/// without encoding a string per call.
/// </summary>
internal static class Keywords
{
    private static readonly byte[][] GeoUnits = Encode("m", "km", "ft", "mi");
    private static readonly byte[][] GeoOrders = Encode("ASC", "DESC");
    private static readonly byte[][] Positions = Encode("BEFORE", "AFTER");
    private static readonly byte[][] Sides = Encode("LEFT", "RIGHT");

    internal static string Number(long value) => value.ToString(CultureInfo.InvariantCulture);

    internal static string Word(GeoUnit unit) => unit switch
    {
        GeoUnit.Meters => "m",
        GeoUnit.Kilometers => "km",
        GeoUnit.Feet => "ft",
        GeoUnit.Miles => "mi",
        _ => throw new ArgumentOutOfRangeException(nameof(unit)),
    };

    internal static string Word(GeoOrder order) => order switch
    {
        GeoOrder.Ascending => "ASC",
        GeoOrder.Descending => "DESC",
        _ => throw new ArgumentOutOfRangeException(nameof(order)),
    };

    internal static string Word(InsertPosition position) => position switch
    {
        InsertPosition.Before => "BEFORE",
        InsertPosition.After => "AFTER",
        _ => throw new ArgumentOutOfRangeException(nameof(position)),
    };

    internal static string Word(ListSide side) => side switch
    {
        ListSide.Left => "LEFT",
        ListSide.Right => "RIGHT",
        _ => throw new ArgumentOutOfRangeException(nameof(side)),
    };

    internal static string Word(KeyType type) => type switch
    {
        KeyType.String => "string",
        KeyType.Json => "json",
        KeyType.List => "list",
        KeyType.Set => "set",
        KeyType.Hash => "hash",
        KeyType.SortedSet => "zset",
        KeyType.BloomFilter => "bloomfilter",
        KeyType.Stream => "stream",
        KeyType.Lease => "lease",
        KeyType.Limit => "limit",
        KeyType.Semaphore => "semaphore",
        KeyType.Once => "once",
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };

    internal static string Word(BitFieldOp op) => op switch
    {
        BitFieldOp.Get => "GET",
        BitFieldOp.Set => "SET",
        BitFieldOp.IncrBy => "INCRBY",
        _ => throw new ArgumentOutOfRangeException(nameof(op)),
    };

    internal static string Word(BitFieldOverflow mode) => mode switch
    {
        BitFieldOverflow.Wrap => "WRAP",
        BitFieldOverflow.Saturate => "SAT",
        BitFieldOverflow.Fail => "FAIL",
        _ => throw new ArgumentOutOfRangeException(nameof(mode)),
    };

    internal static RespArg Arg(GeoUnit unit) => RespArg.Token(Lookup(GeoUnits, (int)unit, nameof(unit)));

    internal static RespArg Arg(GeoOrder order) => RespArg.Token(Lookup(GeoOrders, (int)order, nameof(order)));

    internal static RespArg Arg(InsertPosition position) => RespArg.Token(Lookup(Positions, (int)position, nameof(position)));

    internal static RespArg Arg(ListSide side) => RespArg.Token(Lookup(Sides, (int)side, nameof(side)));

    private static byte[] Lookup(byte[][] table, int index, string parameter)
    {
        if ((uint)index >= (uint)table.Length)
        {
            throw new ArgumentOutOfRangeException(parameter);
        }

        return table[index];
    }

    private static byte[][] Encode(params string[] words)
    {
        var table = new byte[words.Length][];

        for (int index = 0; index < words.Length; index++)
        {
            table[index] = Encoding.ASCII.GetBytes(words[index]);
        }

        return table;
    }
}
