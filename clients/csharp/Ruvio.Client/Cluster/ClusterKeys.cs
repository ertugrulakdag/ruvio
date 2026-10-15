using System.Globalization;
using System.Text;

namespace Ruvio.Client;

/// <summary>Hash slots and the key positions the cluster router needs.</summary>
internal static class ClusterKeys
{
    public const int SlotCount = 16384;

    private static readonly HashSet<string> Keyless = new(StringComparer.Ordinal)
    {
        "ACL", "AUTH", "BGSAVE", "CHANGES", "CLIENT", "CLUSTER", "COMMAND", "CONFIG", "DBSIZE", "DISCARD",
        "ECHO", "EXEC", "FLUSHALL", "FLUSHDB", "FUNCTION", "HELLO", "INFO", "JSON.FIND", "JSON.INDEX", "KEYS", "LASTSAVE", "LATENCY",
        "MULTI", "PING", "PSUBSCRIBE", "PUBLISH", "PUNSUBSCRIBE", "QUIT", "RANDOMKEY", "READONLY",
        "READWRITE", "RESET", "RUVIO", "SAVE", "SCAN", "SCRIPT", "SELECT", "SLOWLOG", "SUBSCRIBE", "SWAPDB",
        "TIME", "UNSUBSCRIBE", "UNWATCH", "WAIT",
    };

    private static readonly HashSet<string> EveryArgument = new(StringComparer.Ordinal)
    {
        "DEL", "EXISTS", "MGET", "PFCOUNT", "PFMERGE", "SDIFF", "SDIFFSTORE", "SINTER", "SINTERSTORE",
        "SSUBSCRIBE", "SUNION", "SUNIONSTORE", "SUNSUBSCRIBE", "TOUCH", "UNLINK", "WATCH",
    };

    private static readonly HashSet<string> TwoKeys = new(StringComparer.Ordinal)
    {
        "BLMOVE", "BRPOPLPUSH", "COPY", "LMOVE", "RENAME", "RENAMENX", "RPOPLPUSH", "SMOVE",
    };

    public static string Name(IReadOnlyList<string> arguments)
    {
        return arguments[0].ToUpperInvariant();
    }

    /// <summary>True when the only key is the first argument, the common case for routing.</summary>
    public static bool IsSingleKey(string name) =>
        !Keyless.Contains(name) && !EveryArgument.Contains(name) && !TwoKeys.Contains(name) && name switch
        {
            "MSET" or "MSETNX" or "BLPOP" or "BRPOP" or "BZPOPMIN" or "BZPOPMAX"
                or "EVAL" or "EVALSHA" or "EVAL_RO" or "EVALSHA_RO" or "FCALL" or "FCALL_RO"
                or "LMPOP" or "BLMPOP" or "ZUNION" or "ZINTER" or "ZDIFF" or "ZUNIONSTORE" or "ZINTERSTORE" or "ZDIFFSTORE"
                or "XREAD" or "XREADGROUP" or "OBJECT" or "MEMORY" or "BITOP" or "SINTERCARD" or "JSON.MGET" => false,
            _ => true,
        };

    /// <summary>The first key of a command without building the key list; null for keyless commands.</summary>
    public static string? First(string name, IReadOnlyList<string> arguments)
    {
        if (arguments.Count < 2 || Keyless.Contains(name))
        {
            return null;
        }

        if (IsSingleKey(name) || EveryArgument.Contains(name) || TwoKeys.Contains(name) || name is "MSET" or "MSETNX")
        {
            return arguments[1];
        }

        IReadOnlyList<string> keys = Of(name, arguments);

        return keys.Count > 0 ? keys[0] : null;
    }

    /// <summary>The keys of a command, in argument order. Empty for keyless commands.</summary>
    public static IReadOnlyList<string> Of(string name, IReadOnlyList<string> arguments)
    {
        int count = arguments.Count;

        if (count < 2 || Keyless.Contains(name))
        {
            return [];
        }

        if (EveryArgument.Contains(name))
        {
            return Slice(arguments, 1, count - 1);
        }

        if (TwoKeys.Contains(name))
        {
            return Slice(arguments, 1, Math.Min(2, count - 1));
        }

        return name switch
        {
            "MSET" or "MSETNX" => Every(arguments, 1, 2),
            "BLPOP" or "BRPOP" or "BZPOPMIN" or "BZPOPMAX" => Slice(arguments, 1, count - 2),
            "EVAL" or "EVALSHA" or "EVAL_RO" or "EVALSHA_RO" or "FCALL" or "FCALL_RO" => Counted(arguments, 2),
            "ZUNION" or "ZINTER" or "ZDIFF" or "LMPOP" => Counted(arguments, 1),
            "BLMPOP" => Counted(arguments, 2),
            "ZUNIONSTORE" or "ZINTERSTORE" or "ZDIFFSTORE" => [arguments[1], .. Counted(arguments, 2)],
            "XREAD" or "XREADGROUP" => Streams(arguments),
            "OBJECT" or "MEMORY" => count > 2 ? [arguments[2]] : [],
            "BITOP" => Slice(arguments, 2, count - 2),
            "SINTERCARD" => Counted(arguments, 1),
            "JSON.MGET" => Slice(arguments, 1, count - 2),
            _ => [arguments[1]],
        };
    }

    /// <summary>CRC16-XMODEM of the key, or of the first non-empty <c>{tag}</c> in it.</summary>
    public static int Slot(string key)
    {
        int bytes = Encoding.UTF8.GetByteCount(key);
        Span<byte> utf8 = stackalloc byte[256];
        byte[]? rented = null;
       
        if (bytes > utf8.Length)
        {
            rented = System.Buffers.ArrayPool<byte>.Shared.Rent(bytes);
            utf8 = rented;
        }

        try
        {
            Encoding.UTF8.GetBytes(key, utf8[..bytes]);

            return Slot(utf8[..bytes]);
        }

        finally
        {
            if (rented is not null)
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    internal static int Slot(ReadOnlySpan<byte> key)
    {
        int open = ByteScan.IndexOf(key, (byte)'{');

        if (open >= 0)
        {
            int close = ByteScan.IndexOf(key[(open + 1)..], (byte)'}');

            if (close > 0)
            {
                key = key.Slice(open + 1, close);
            }
        }

        return Crc16(key) % SlotCount;
    }

    private static readonly ushort[] CrcTable = CreateCrcTable();

    private static ushort[] CreateCrcTable()
    {
        var table = new ushort[256];

        for (int value = 0; value < table.Length; value++)
        {
            int crc = value << 8;

            for (int bit = 0; bit < 8; bit++)
            {
                crc = (crc & 0x8000) != 0 ? (crc << 1) ^ 0x1021 : crc << 1;
            }

            table[value] = (ushort)(crc & 0xFFFF);
        }

        return table;
    }

    private static int Crc16(ReadOnlySpan<byte> bytes)
    {
        int crc = 0;

        foreach (byte value in bytes)
        {
            crc = ((crc << 8) ^ CrcTable[((crc >> 8) ^ value) & 0xFF]) & 0xFFFF;
        }

        return crc;
    }

    private static string[] Slice(IReadOnlyList<string> arguments, int start, int length)
    {
        if (length <= 0)
        {
            return [];
        }

        var keys = new string[length];
        
        for (int index = 0; index < length; index++)
        {
            keys[index] = arguments[start + index];
        }

        return keys;
    }

    private static List<string> Every(IReadOnlyList<string> arguments, int start, int step)
    {
        var keys = new List<string>();

        for (int index = start; index < arguments.Count; index += step)
        {
            keys.Add(arguments[index]);
        }

        return keys;
    }

    private static string[] Counted(IReadOnlyList<string> arguments, int countIndex)
    {
        if (countIndex >= arguments.Count
            || !int.TryParse(arguments[countIndex], NumberStyles.None, CultureInfo.InvariantCulture, out int count))
        {
            return [];
        }

        return Slice(arguments, countIndex + 1, Math.Min(count, arguments.Count - countIndex - 1));
    }

    private static string[] Streams(IReadOnlyList<string> arguments)
    {
        for (int index = 1; index < arguments.Count; index++)
        {
            if (arguments[index].Equals("STREAMS", StringComparison.OrdinalIgnoreCase))
            {
                int rest = arguments.Count - index - 1;

                return Slice(arguments, index + 1, rest / 2);
            }
        }

        return [];
    }
}
