using System.Text;
using System.Text.Json;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// The RESP wire vectors in clients/testdata/resp.json are shared by every client, so the six
/// encoders and parsers cannot drift apart.
public sealed class RespVectorTests
{
    [Fact]
    public void EncoderWritesTheSharedVectors()
    {
        using JsonDocument document = Load();
        int count = 0;

        foreach (JsonElement entry in document.RootElement.GetProperty("encode").EnumerateArray())
        {
            string name = entry.GetProperty("name").GetString()!;
            byte[] expected = Wire(entry);
            List<ReadOnlyMemory<byte>> binary = [];

            if (entry.TryGetProperty("args", out JsonElement args))
            {
                string[] text = args.EnumerateArray().Select(arg => arg.GetString()!).ToArray();

                AssertBytes(expected, RespCodec.Encode(text), $"{name}: string[]");
                AssertBytes(expected, RespCodec.Encode(text.ToList()), $"{name}: List<string>");
                binary.AddRange(text.Select(arg => (ReadOnlyMemory<byte>)Encoding.UTF8.GetBytes(arg)));
            }
            else
            {
                binary.AddRange(entry.GetProperty("args_hex").EnumerateArray().Select(arg => (ReadOnlyMemory<byte>)Convert.FromHexString(arg.GetString()!)));
            }

            AssertBytes(expected, RespCodec.Encode(binary), $"{name}: bytes");
            count++;
        }

        Assert.True(count > 0);
    }

    [Fact]
    public async Task ReaderReadsTheSharedVectors()
    {
        using JsonDocument document = Load();
        int count = 0;

        foreach (JsonElement entry in document.RootElement.GetProperty("decode").EnumerateArray())
        {
            string name = entry.GetProperty("name").GetString()!;
            using var reader = new RespReader(new MemoryStream(Wire(entry)));
            RespValue value;

            try
            {
                value = await reader.ReadAsync(CancellationToken.None);
            }
            catch (Exception exception)
            {
                throw new Xunit.Sdk.XunitException($"{name}: {exception.Message}");
            }

            AssertValue(entry.GetProperty("value"), value, name);

            // Nothing may be left: the next read finds the end of the stream before a reply starts.
            reader.PrepareReply();
            RuvioException closed = await Assert.ThrowsAsync<RuvioException>(() => reader.ReadAsync(CancellationToken.None).AsTask());
            Assert.True(closed.ClosedBeforeReply, $"{name}: bytes left over");
            count++;
        }

        Assert.True(count > 0);
    }

    /// The reader waits for more bytes; over a finished stream that shows as a closed connection.
    [Fact]
    public async Task ReaderWaitsForTheRestOfAnIncompleteReply()
    {
        using JsonDocument document = Load();
        int count = 0;

        foreach (JsonElement entry in document.RootElement.GetProperty("incomplete").EnumerateArray())
        {
            string name = entry.GetProperty("name").GetString()!;
            using var reader = new RespReader(new MemoryStream(Wire(entry)));

            RuvioException error = await Assert.ThrowsAsync<RuvioException>(() => reader.ReadAsync(CancellationToken.None).AsTask());
            Assert.True(error.Message == "connection closed", $"{name}: {error.Message}");
            count++;
        }

        Assert.True(count > 0);
    }

    private static void AssertValue(JsonElement expected, RespValue actual, string name)
    {
        JsonProperty shape = expected.EnumerateObject().Single();
        JsonElement inner = shape.Value;

        switch (shape.Name)
        {
            case "simple":
                Assert.True(actual.Kind == RespKind.Simple, $"{name}: {actual.Kind}");
                Assert.True(inner.GetString() == actual.Text, name);
                break;
            case "error":
                Assert.True(actual.Kind == RespKind.Error, $"{name}: {actual.Kind}");
                Assert.True(inner.GetString() == actual.Text, name);
                break;
            case "int":
                long number = inner.ValueKind == JsonValueKind.String ? long.Parse(inner.GetString()!) : inner.GetInt64();
                Assert.True(actual.Kind == RespKind.Integer, $"{name}: {actual.Kind}");
                Assert.True(number == actual.Integer, name);
                break;
            case "bulk":
                Assert.True(actual.Kind == RespKind.Bulk, $"{name}: {actual.Kind}");
                AssertBytes(Encoding.UTF8.GetBytes(inner.GetString()!), actual.Bytes!, name);
                break;
            case "bulk_hex":
                Assert.True(actual.Kind == RespKind.Bulk, $"{name}: {actual.Kind}");
                AssertBytes(Convert.FromHexString(inner.GetString()!), actual.Bytes!, name);
                break;
            case "null":
                Assert.True(actual.Kind == RespKind.Null, $"{name}: {actual.Kind}");
                break;
            case "array":
                Assert.True(actual.Kind == RespKind.Array, $"{name}: {actual.Kind}");
                JsonElement[] items = inner.EnumerateArray().ToArray();
                Assert.True(items.Length == actual.Items!.Count, name);

                for (int index = 0; index < items.Length; index++)
                {
                    AssertValue(items[index], actual.Items[index], name);
                }

                break;
            default:
                throw new Xunit.Sdk.XunitException($"{name}: unknown value kind {shape.Name}");
        }
    }

    private static void AssertBytes(byte[] expected, byte[] actual, string name) =>
        Assert.True(expected.AsSpan().SequenceEqual(actual), $"{name}: got {Convert.ToHexString(actual)}");

    private static byte[] Wire(JsonElement entry) =>
        entry.TryGetProperty("bytes_hex", out JsonElement hex)
            ? Convert.FromHexString(hex.GetString()!)
            : Encoding.UTF8.GetBytes(entry.GetProperty("bytes").GetString()!);

    private static JsonDocument Load() => JsonDocument.Parse(File.ReadAllText(FindVectors()));

    private static string FindVectors()
    {
        string? directory = AppContext.BaseDirectory;

        for (int depth = 0; depth < 10 && directory is not null; depth++)
        {
            string candidate = Path.Combine(directory, "clients", "testdata", "resp.json");

            if (File.Exists(candidate))
            {
                return candidate;
            }

            directory = Path.GetDirectoryName(directory);
        }

        throw new FileNotFoundException("clients/testdata/resp.json was not found above the test output.");
    }
}
