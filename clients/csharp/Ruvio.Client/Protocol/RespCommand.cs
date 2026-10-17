using System.Buffers;
using System.Buffers.Text;
using System.Text;

namespace Ruvio.Client;

/// <summary>One command argument: a string, an integer written in decimal without a string, or a shared pre-encoded token.</summary>
internal readonly struct RespArg
{
    internal readonly string? Text;
    internal readonly long Number;
    internal readonly byte[]? Encoded;

    private RespArg(string? text, long number, byte[]? encoded = null)
    {
        Text = text;
        Number = number;
        Encoded = encoded;
    }

    public static implicit operator RespArg(string text) => new(text ?? throw new ArgumentNullException(nameof(text)), 0);

    public static implicit operator RespArg(long number) => new(null, number);

    /// <summary>An ASCII keyword whose UTF-8 bytes were encoded once; the writer copies them as they are.</summary>
    internal static RespArg Token(byte[] encoded) => new(null, 0, encoded);

    public override string ToString() =>
        Text
        ?? (Encoded is not null
            ? Encoding.ASCII.GetString(Encoded)
            : Number.ToString(System.Globalization.CultureInfo.InvariantCulture));
}

/// <summary>
/// A command whose name and up to five arguments are stored inline, so a typed call
/// allocates no argument array and no string for integer arguments. Longer commands
/// keep a list. Inline values are immutable, so a cancelled caller cannot change them
/// before the writer encodes them.
/// </summary>
internal readonly struct RespCommand
{
    private const int InlineArguments = 5;
    private static readonly Encoding Utf8 = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private readonly string? _name;
    private readonly RespArg _a, _b, _c, _d, _e;
    private readonly IReadOnlyList<string>? _list;

    /// <summary>Total parts including the command name.</summary>
    internal readonly int Count;

    internal RespCommand(string name) : this(name, default, default, default, default, default, 1) { }

    internal RespCommand(string name, RespArg a) : this(name, a, default, default, default, default, 2) { }

    internal RespCommand(string name, RespArg a, RespArg b) : this(name, a, b, default, default, default, 3) { }

    internal RespCommand(string name, RespArg a, RespArg b, RespArg c) : this(name, a, b, c, default, default, 4) { }

    internal RespCommand(string name, RespArg a, RespArg b, RespArg c, RespArg d) : this(name, a, b, c, d, default, 5) { }

    internal RespCommand(string name, RespArg a, RespArg b, RespArg c, RespArg d, RespArg e) : this(name, a, b, c, d, e, 6) { }

    private RespCommand(string name, RespArg a, RespArg b, RespArg c, RespArg d, RespArg e, int count)
    {
        ArgumentNullException.ThrowIfNull(name);
        _name = name;
        _a = a;
        _b = b;
        _c = c;
        _d = d;
        _e = e;
        _list = null;
        Count = count;
    }

    /// <summary>Appends one argument; past the inline capacity the command becomes a list.</summary>
    internal RespCommand With(RespArg argument)
    {
        if (_list is null && Count <= InlineArguments)
        {
            return Count switch
            {
                1 => new(_name!, argument, default, default, default, default, 2),
                2 => new(_name!, _a, argument, default, default, default, 3),
                3 => new(_name!, _a, _b, argument, default, default, 4),
                4 => new(_name!, _a, _b, _c, argument, default, 5),
                _ => new(_name!, _a, _b, _c, _d, argument, 6),
            };
        }

        IReadOnlyList<string> current = AsList();
        var arguments = new string[current.Count + 1];

        for (int index = 0; index < current.Count; index++)
        {
            arguments[index] = current[index];
        }

        arguments[^1] = argument.ToString();

        return new RespCommand(arguments);
    }

    internal RespCommand(IReadOnlyList<string> arguments)
    {
        ArgumentNullException.ThrowIfNull(arguments);
        _list = arguments;
        _name = null;
        _a = _b = _c = _d = _e = default;
        Count = arguments.Count;
    }

    internal static RespCommand Keyed(string name, string key, string[] arguments)
    {
        ArgumentNullException.ThrowIfNull(arguments);

        if (arguments.Length <= InlineArguments - 1)
        {
            RespCommand command = new(name, key);

            foreach (string argument in arguments)
                command = command.With(argument);

            return command;
        }

        var parts = new string[arguments.Length + 2];
        parts[0] = name;
        parts[1] = key;
        arguments.AsSpan().CopyTo(parts.AsSpan(2));

        return new RespCommand(parts);
    }

    internal static RespCommand KeyedPairs(
        string name, string key, (string Left, string Right)[] pairs)
    {
        ArgumentNullException.ThrowIfNull(pairs);

        if (pairs.Length <= (InlineArguments - 1) / 2)
        {
            RespCommand command = new(name, key);

            foreach ((string left, string right) in pairs)
                command = command.With(left).With(right);

            return command;
        }

        var parts = new string[2 + pairs.Length * 2];
        parts[0] = name;
        parts[1] = key;
        int index = 2;

        foreach ((string left, string right) in pairs)
        {
            parts[index++] = left;
            parts[index++] = right;
        }

        return new RespCommand(parts);
    }

    public static implicit operator RespCommand(string[] arguments) => new(arguments);

    public static implicit operator RespCommand(List<string> arguments) => new(arguments);

    /// <summary>True when the arguments live in a caller-owned list that may change after a cancelled wait.</summary>
    internal bool IsCallerList => _list is not null;

    /// <summary>The command name; null for an empty list.</summary>
    internal string? Name => _list is null ? _name : _list.Count > 0 ? _list[0] : null;

    private RespArg Part(int index) => index switch
    {
        0 => _name!,
        1 => _a,
        2 => _b,
        3 => _c,
        4 => _d,
        5 => _e,
        _ => throw new ArgumentOutOfRangeException(nameof(index)),
    };

    /// <summary>The text of one part, or null when it is a number.</summary>
    internal string? TextAt(int index) => _list is not null ? _list[index] : Part(index) is { Encoded: not null } token ? token.ToString() : Part(index).Text;

    /// <summary>The list form, for cluster routing, retries, and other rare paths.</summary>
    internal IReadOnlyList<string> AsList()
    {
        if (_list is not null)
            return _list;

        var arguments = new string[Count];

        for (int index = 0; index < Count; index++)
        {
            arguments[index] = Part(index).ToString();
        }

        return arguments;
    }

    internal PooledFrame EncodePooled()
    {
        if (_list is not null)
            return RespCodec.EncodePooled(_list);

        ReadOnlySpan<byte> verbBulk = RespVerbs.Bulk(_name!);
        int firstPart = verbBulk.IsEmpty ? 0 : 1;
        Span<int> lengths = stackalloc int[InlineArguments + 1];
        int size = 1 + Digits(Count) + 2 + verbBulk.Length;

        for (int index = firstPart; index < Count; index++)
        {
            RespArg part = Part(index);
            int length = part.Encoded is { } token
                ? token.Length
                : part.Text is null ? Digits(part.Number) : Utf8.GetByteCount(part.Text);
            lengths[index] = length;
            size += 1 + Digits(length) + 2 + length + 2;
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            Span<byte> destination = rented.AsSpan(0, size);
            int cursor = WriteLine(destination, 0, (byte)'*', Count);
            verbBulk.CopyTo(destination[cursor..]);
            cursor += verbBulk.Length;

            for (int index = firstPart; index < Count; index++)
            {
                RespArg part = Part(index);
                cursor = WriteLine(destination, cursor, (byte)'$', lengths[index]);

                if (part.Encoded is { } encoded)
                {
                    encoded.CopyTo(destination[cursor..]);
                    cursor += encoded.Length;
                }
                else if (part.Text is null)
                {
                    Utf8Formatter.TryFormat(part.Number, destination[cursor..], out int written);
                    cursor += written;
                }
                else
                {
                    cursor += Utf8.GetBytes(part.Text, destination[cursor..]);
                }

                destination[cursor++] = (byte)'\r';
                destination[cursor++] = (byte)'\n';
            }

            return new PooledFrame(rented, cursor);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(rented);
            throw;
        }
    }

    private static int WriteLine(Span<byte> destination, int cursor, byte prefix, int value)
    {
        destination[cursor++] = prefix;
        Utf8Formatter.TryFormat(value, destination[cursor..], out int written);
        cursor += written;
        destination[cursor++] = (byte)'\r';
        destination[cursor++] = (byte)'\n';

        return cursor;
    }

    private static int Digits(long value)
    {
        if (value == long.MinValue)
            return 20;

        int digits = value < 0 ? 2 : 1;
        ulong magnitude = (ulong)Math.Abs(value);

        while (magnitude >= 10)
        {
            magnitude /= 10;
            digits++;
        }

        return digits;
    }
}
