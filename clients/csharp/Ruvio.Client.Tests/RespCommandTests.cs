using System.Text;
using Xunit;

namespace Ruvio.Client.Tests;

public class RespCommandTests
{
    [Fact]
    public void KeyedFactoriesRejectNullArgumentArrays()
    {
        Assert.Throws<ArgumentNullException>(() => RespCommand.Keyed("LPUSH", "key", null!));
        Assert.Throws<ArgumentNullException>(() => RespCommand.KeyedPairs("HSET", "key", null!));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(16)]
    public void KeyedFactoryMatchesListAndCopiesArguments(int count)
    {
        string[] arguments = Enumerable.Range(0, count).Select(i => $"değer:{i}").ToArray();
        string[] expected = new[] { "LPUSH", "kéy" }.Concat(arguments).ToArray();
        RespCommand command = RespCommand.Keyed("LPUSH", "kéy", arguments);
        Assert.Equal(count > 4, command.IsCallerList);
        Array.Fill(arguments, "changed");
        Assert.Equal(expected, command.AsList());
        Assert.Equal(RespCodec.Encode(expected), Encode(command));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(16)]
    public void KeyedPairFactoryMatchesListAndCopiesArguments(int count)
    {
        (string, string)[] pairs = Enumerable.Range(0, count)
            .Select(i => ($"field:{i}", $"değer:{i}")).ToArray();
        string[] expected = new[] { "HSET", "kéy" }
            .Concat(pairs.SelectMany(pair => new[] { pair.Item1, pair.Item2 })).ToArray();
        RespCommand command = RespCommand.KeyedPairs("HSET", "kéy", pairs);
        Assert.Equal(count > 2, command.IsCallerList);
        Array.Fill(pairs, ("changed", "changed"));
        Assert.Equal(expected, command.AsList());
        Assert.Equal(RespCodec.Encode(expected), Encode(command));
    }

    public static TheoryData<string[]> Commands => new()
    {
        new[] { "PING" },
        new[] { "GET", "key" },
        new[] { "SET", "kéy", "değer 🚀", "PX", "1500" },
        new[] { "IDEM", "k", "BEGIN", "fp", "owner", "60000" },
        new[] { "INCRBY", "k", "-9223372036854775808" },
        new[] { "INCRBY", "k", "9223372036854775807" },
        new[] { "SET", "k", "" },
        new[] { "get", "kéy" },
        new[] { "CUSTOM", "key" },
        new[] { "özel", "key" },
        new[] { "", "key" },
    };

    [Theory]
    [MemberData(nameof(Commands))]
    public void InlineEncodingMatchesListEncoding(string[] parts)
    {
        RespCommand inline = Build(parts);

        Assert.False(inline.IsCallerList);
        Assert.Equal(parts.Length, inline.Count);
        Assert.Equal(parts[0], inline.Name);
        Assert.Equal(parts, inline.AsList());
        Assert.Equal(Encode(new RespCommand(parts)), Encode(inline));
    }

    [Fact]
    public void AppendingPastInlineCapacityBecomesAList()
    {
        RespCommand command = new RespCommand("SET", "k", "v")
            .With("PX").With(1000L).With("NX").With("GET");

        Assert.True(command.IsCallerList);
        Assert.Equal(new[] { "SET", "k", "v", "PX", "1000", "NX", "GET" }, command.AsList());
        Assert.Equal(RespCodec.Encode(new[] { "SET", "k", "v", "PX", "1000", "NX", "GET" }), Encode(command));
    }

    [Fact]
    public void EveryGeneratedVerbMatchesGenericEncodingAtEveryInlineArity()
    {
        foreach (string line in File.ReadLines(Path.Combine(RespVerbGeneratorTests.FindClientDirectory(), "RespVerbs.txt")))
        {
            string verb = line.Trim();

            if (verb.Length == 0 || verb.StartsWith('#'))
                continue;

            RespCommand command = new(verb);

            for (int arguments = 0; arguments <= 5; arguments++)
            {
                Assert.Equal(RespCodec.Encode(command.AsList()), Encode(command));

                if (arguments < 5)
                    command = command.With(arguments % 2 == 0 ? (RespArg)"kéy" : (RespArg)long.MinValue);
            }
        }
    }

    [Fact]
    public void GeneratedVerbStillRejectsInvalidUtf16Arguments()
    {
        Assert.Throws<EncoderFallbackException>(() => Encode(new RespCommand("GET", "\ud800")));
    }

    [Fact]
    public void NumbersAreWrittenWithoutCultureFormatting()
    {
        var previous = Thread.CurrentThread.CurrentCulture;
        Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("ar-SA");
        try
        {
            Assert.Equal("*2\r\n$4\r\nINCR\r\n$5\r\n-1234\r\n", Encoding.ASCII.GetString(Encode(new RespCommand("INCR", -1234L))));
        }
        finally
        {
            Thread.CurrentThread.CurrentCulture = previous;
        }
    }

    // Integer parts are passed as numbers so the inline integer path is exercised.
    private static RespCommand Build(string[] parts)
    {
        RespCommand command = new(parts[0]);

        for (int index = 1; index < parts.Length; index++)
        {
            command = long.TryParse(parts[index], out long number) && number.ToString(System.Globalization.CultureInfo.InvariantCulture) == parts[index]
                ? command.With(number)
                : command.With(parts[index]);
        }

        return command;
    }

    private static byte[] Encode(RespCommand command)
    {
        using PooledFrame frame = command.EncodePooled();

        return frame.Buffer.AsSpan(0, frame.Length).ToArray();
    }
}
