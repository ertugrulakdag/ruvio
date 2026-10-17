namespace Ruvio.Client;

/// <summary>The distance unit of the <c>GEO*</c> commands. The enum overloads send a pre-encoded token, with no string encoding per call.</summary>
/// <example>
/// <code>
/// double? km = await client.GeoDistanceAsync("cities", "ankara", "izmir", GeoUnit.Kilometers);
/// </code>
/// </example>
public enum GeoUnit
{
    /// <summary>Meters (<c>m</c>).</summary>
    Meters,

    /// <summary>Kilometers (<c>km</c>).</summary>
    Kilometers,

    /// <summary>Feet (<c>ft</c>).</summary>
    Feet,

    /// <summary>Miles (<c>mi</c>).</summary>
    Miles,
}

/// <summary>The result order of <c>GEOSEARCH</c>, <c>GEORADIUS</c> and <c>GEORADIUSBYMEMBER</c>.</summary>
/// <example>
/// <code>
/// var farthest = await client.GeoRadiusAsync("cities", 32.8, 39.9, 500, GeoUnit.Kilometers, GeoOrder.Descending);
/// </code>
/// </example>
public enum GeoOrder
{
    /// <summary>Nearest first (<c>ASC</c>).</summary>
    Ascending,

    /// <summary>Farthest first (<c>DESC</c>).</summary>
    Descending,
}

/// <summary>Where <c>LINSERT</c> puts the new element relative to the pivot.</summary>
/// <example>
/// <code>
/// long length = await client.InsertListAsync("queue", InsertPosition.After, "b", "c");
/// </code>
/// </example>
public enum InsertPosition
{
    /// <summary>Before the pivot (<c>BEFORE</c>).</summary>
    Before,

    /// <summary>After the pivot (<c>AFTER</c>).</summary>
    After,
}

/// <summary>An end of a list, used by <c>LMOVE</c>, <c>BLMOVE</c>, <c>LMPOP</c> and <c>BLMPOP</c>.</summary>
/// <example>
/// <code>
/// string? moved = await client.MoveListItemAsync("source", "target", ListSide.Right, ListSide.Left);
/// </code>
/// </example>
public enum ListSide
{
    /// <summary>The head (<c>LEFT</c>).</summary>
    Left,

    /// <summary>The tail (<c>RIGHT</c>).</summary>
    Right,
}

/// <summary>The value type a <c>SCAN ... TYPE</c> filter accepts.</summary>
/// <example>
/// <code>
/// ScanPage page = await client.ScanAsync(0, "user:*", 100, KeyType.Hash);
/// </code>
/// </example>
public enum KeyType
{
    /// <summary><c>string</c>.</summary>
    String,

    /// <summary><c>json</c>.</summary>
    Json,

    /// <summary><c>list</c>.</summary>
    List,

    /// <summary><c>set</c>.</summary>
    Set,

    /// <summary><c>hash</c>.</summary>
    Hash,

    /// <summary><c>zset</c>.</summary>
    SortedSet,

    /// <summary><c>bloomfilter</c>.</summary>
    BloomFilter,

    /// <summary><c>stream</c>.</summary>
    Stream,

    /// <summary><c>lease</c>.</summary>
    Lease,

    /// <summary><c>limit</c>.</summary>
    Limit,

    /// <summary><c>semaphore</c>.</summary>
    Semaphore,

    /// <summary><c>once</c>.</summary>
    Once,
}

/// <summary>What a <see cref="BitFieldStep"/> does.</summary>
public enum BitFieldOp
{
    /// <summary><c>GET type offset</c>.</summary>
    Get,

    /// <summary><c>SET type offset value</c>.</summary>
    Set,

    /// <summary><c>INCRBY type offset increment</c>.</summary>
    IncrBy,
}

/// <summary>What <c>BITFIELD</c> does when a <c>SET</c> or <c>INCRBY</c> leaves the range of the field type.</summary>
public enum BitFieldOverflow
{
    /// <summary>Wrap around (<c>WRAP</c>, the default).</summary>
    Wrap,

    /// <summary>Clamp to the smallest or largest value (<c>SAT</c>).</summary>
    Saturate,

    /// <summary>Refuse the write and return null for that step (<c>FAIL</c>).</summary>
    Fail,
}

/// <summary>
/// Field types for <c>BITFIELD</c>: <c>i1</c> to <c>i64</c> (signed) and <c>u1</c> to <c>u63</c> (unsigned). The server
/// requires the lowercase prefix. The returned strings are shared and never allocate.
/// </summary>
/// <example>
/// <code>
/// string type = BitFieldType.Unsigned(8); // "u8"
/// </code>
/// </example>
public static class BitFieldType
{
    private static readonly string[] SignedNames = Build('i', 64);
    private static readonly string[] UnsignedNames = Build('u', 63);

    /// <summary>The signed type of <paramref name="bits"/> bits, for example <c>i16</c>. Valid widths are 1 to 64.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bits"/> is outside 1 to 64.</exception>
    public static string Signed(int bits)
    {
        if (bits < 1 || bits > 64)
        {
            throw new ArgumentOutOfRangeException(nameof(bits), bits, "A signed field is 1 to 64 bits wide.");
        }

        return SignedNames[bits];
    }

    /// <summary>The unsigned type of <paramref name="bits"/> bits, for example <c>u8</c>. Valid widths are 1 to 63.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bits"/> is outside 1 to 63.</exception>
    public static string Unsigned(int bits)
    {
        if (bits < 1 || bits > 63)
        {
            throw new ArgumentOutOfRangeException(nameof(bits), bits, "An unsigned field is 1 to 63 bits wide.");
        }

        return UnsignedNames[bits];
    }

    private static string[] Build(char prefix, int max)
    {
        var names = new string[max + 1];

        for (int bits = 1; bits <= max; bits++)
        {
            names[bits] = prefix + bits.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        return names;
    }
}

/// <summary>
/// One <c>BITFIELD</c> subcommand: a <c>GET</c>, <c>SET</c> or <c>INCRBY</c>, or an <c>OVERFLOW</c> mode that applies to the
/// later <c>SET</c> and <c>INCRBY</c> steps. Build steps with the factory methods and pass them to
/// <c>BitFieldAsync</c>.
/// </summary>
/// <example>
/// <code>
/// var results = await client.BitFieldAsync(
///     "counters",
///     BitFieldStep.Overflow(BitFieldOverflow.Saturate),
///     BitFieldStep.IncrBy(BitFieldType.Unsigned(8), 0, 10),
///     BitFieldStep.Get(BitFieldType.Unsigned(8), 0));
/// </code>
/// </example>
public readonly struct BitFieldStep
{
    private readonly string? _type;
    private readonly long _offset;
    private readonly long _value;
    private readonly byte _kind;

    private BitFieldStep(byte kind, string? type, long offset, long value)
    {
        _kind = kind;
        _type = type;
        _offset = offset;
        _value = value;
    }

    /// <summary><c>GET type offset</c>. <paramref name="type"/> comes from <see cref="BitFieldType"/>; <paramref name="offset"/> is a bit offset.</summary>
    public static BitFieldStep Get(string type, long offset) => new(0, type, offset, 0);

    /// <summary><c>SET type offset value</c>. Returns the old value.</summary>
    public static BitFieldStep Set(string type, long offset, long value) => new(1, type, offset, value);

    /// <summary><c>INCRBY type offset increment</c>. Returns the new value.</summary>
    public static BitFieldStep IncrBy(string type, long offset, long increment) => new(2, type, offset, increment);

    /// <summary><c>OVERFLOW mode</c>. Applies to the <c>SET</c> and <c>INCRBY</c> steps after it.</summary>
    public static BitFieldStep Overflow(BitFieldOverflow mode) => new((byte)(3 + (int)mode), null, 0, 0);

    internal static string[] Words(BitFieldStep first, BitFieldStep[] rest) => Command(null, null, first, rest);

    internal static string[] Command(string? name, string? key, BitFieldStep first, BitFieldStep[] rest)
    {
        ArgumentNullException.ThrowIfNull(rest);

        var words = new List<string>((rest.Length + 1) * 4 + 2);

        if (name is not null)
        {
            words.Add(name);
            words.Add(key!);
        }

        first.AppendTo(words);

        foreach (BitFieldStep step in rest)
        {
            step.AppendTo(words);
        }

        return words.ToArray();
    }

    internal void AppendTo(List<string> arguments)
    {
        if (_kind >= 3)
        {
            arguments.Add("OVERFLOW");
            arguments.Add(Keywords.Word((BitFieldOverflow)(_kind - 3)));

            return;
        }

        ArgumentNullException.ThrowIfNull(_type, "type");
        arguments.Add(Keywords.Word((BitFieldOp)_kind));
        arguments.Add(_type);
        arguments.Add(Keywords.Number(_offset));

        if (_kind != 0)
        {
            arguments.Add(Keywords.Number(_value));
        }
    }
}
