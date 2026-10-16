using System.Text.Json;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// The key to hash slot vectors in clients/testdata/slots.json are shared by the server and
/// every client.
public sealed class SlotVectorTests
{
    [Fact]
    public void GetHashSlotMatchesTheSharedVectors()
    {
        using JsonDocument document = JsonDocument.Parse(File.ReadAllText(FindVectors()));
        int count = 0;

        foreach (JsonElement entry in document.RootElement.GetProperty("keys").EnumerateArray())
        {
            string key = entry.GetProperty("key").GetString()!;

            Assert.True(entry.GetProperty("slot").GetInt32() == RuvioClient.GetHashSlot(key), key);
            count++;
        }

        Assert.True(count > 0);
    }

    private static string FindVectors()
    {
        string? directory = AppContext.BaseDirectory;

        for (int depth = 0; depth < 10 && directory is not null; depth++)
        {
            string candidate = Path.Combine(directory, "clients", "testdata", "slots.json");

            if (File.Exists(candidate))
            {
                return candidate;
            }

            directory = Path.GetDirectoryName(directory);
        }

        throw new FileNotFoundException("clients/testdata/slots.json was not found above the test output.");
    }
}
