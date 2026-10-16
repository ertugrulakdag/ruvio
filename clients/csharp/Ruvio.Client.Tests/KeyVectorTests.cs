using System.Text.Json;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// The command to key vectors in clients/testdata/key_vectors.json come from the server's
/// command table; the cluster router must find the same keys for every one.
public sealed class KeyVectorTests
{
    // Sharded Pub/Sub commands have no keys in the file; the router sends them to the slot
    // of their channel through the same key lookup.
    private static readonly HashSet<string> ChannelCommands = new(StringComparer.Ordinal)
    {
        "SPUBLISH", "SSUBSCRIBE", "SUNSUBSCRIBE",
    };

    [Fact]
    public void ClusterKeysMatchTheSharedVectors()
    {
        using JsonDocument document = JsonDocument.Parse(File.ReadAllText(FindVectors()));
        var failures = new List<string>();
        int count = 0;

        foreach (JsonElement entry in document.RootElement.GetProperty("vectors").EnumerateArray())
        {
            string[] arguments = [.. entry.GetProperty("args").EnumerateArray().Select(value => value.GetString()!)];
            string name = ClusterKeys.Name(arguments);

            if (ChannelCommands.Contains(name))
            {
                continue;
            }

            List<string> expected = [.. entry.GetProperty("keys").EnumerateArray().Select(value => value.GetString()!)];
            IReadOnlyList<string> keys = ClusterKeys.Of(name, arguments);
            List<string> actual = [.. keys];

            expected.Sort(StringComparer.Ordinal);
            actual.Sort(StringComparer.Ordinal);

            if (!expected.SequenceEqual(actual) || ClusterKeys.First(name, arguments) != (keys.Count > 0 ? keys[0] : null))
            {
                failures.Add($"{string.Join(' ', arguments)} => [{string.Join(", ", keys)}], want [{string.Join(", ", expected)}]");
            }

            count++;
        }

        Assert.True(count > 0);
        Assert.True(failures.Count == 0, $"{failures.Count} of {count} vectors failed:\n{string.Join('\n', failures)}");
    }

    private static string FindVectors()
    {
        string? directory = AppContext.BaseDirectory;

        for (int depth = 0; depth < 10 && directory is not null; depth++)
        {
            string candidate = Path.Combine(directory, "clients", "testdata", "key_vectors.json");

            if (File.Exists(candidate))
            {
                return candidate;
            }

            directory = Path.GetDirectoryName(directory);
        }

        throw new FileNotFoundException("clients/testdata/key_vectors.json was not found above the test output.");
    }
}
