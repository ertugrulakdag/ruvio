using System.Reflection;
using System.Text.Json;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>
/// Reads docs/compatibility-matrix.json and fails when a command has no typed method. The "csharp" field of
/// clients/testdata/command_methods.json maps each matrix command to the method that sends it ("RuvioConnection.X"
/// lives on the connection); the live tests in LiveCommandTests exercise them on a server.
/// </summary>
public class MatrixCoverageTests
{
    /// <summary>Matrix command to the method(s) that send it: the "csharp" field of clients/testdata/command_methods.json.</summary>
    internal static readonly Dictionary<string, string[]> Methods = LoadMethods();

    [Fact]
    public void EveryMatrixCommandMapsToATypedMethod()
    {
        string matrix = FindMatrix();
        using JsonDocument document = JsonDocument.Parse(File.ReadAllText(matrix));
        List<string> commands = document.RootElement.GetProperty("commands").EnumerateArray()
            .Select(command => command.GetProperty("name").GetString()!)
            .ToList();
        List<string> missing = [];

        foreach (string command in commands)
        {
            if (!Methods.TryGetValue(command, out string[]? methods) || methods.Length == 0)
            {
                missing.Add(command + " (not in command_methods.json)");
                continue;
            }

            foreach (string method in methods.Where(method => !Exists(method)))
            {
                missing.Add(command + " -> " + method + " (no such public method)");
            }
        }

        List<string> stale = Methods.Keys.Except(commands, StringComparer.Ordinal).ToList();

        Assert.True(missing.Count == 0, "Matrix commands without a typed method: " + string.Join(", ", missing));
        Assert.True(stale.Count == 0, "command_methods.json entries that are not in the matrix: " + string.Join(", ", stale));
        Assert.Equal(commands.Count, commands.Count(command => Methods.ContainsKey(command)));
    }

    [Fact]
    public void EveryMappedMethodIsOnIRuvioClientOrTheConnection()
    {
        foreach (string method in Methods.Values.SelectMany(methods => methods).Distinct())
        {
            if (method.StartsWith("RuvioConnection.", StringComparison.Ordinal))
            {
                Assert.NotNull(typeof(IRuvioConnection).GetMethod(method["RuvioConnection.".Length..]));
                continue;
            }

            Assert.Contains(typeof(IRuvioClient).GetMethods(), info => info.Name == method);
        }
    }

    private static bool Exists(string method)
    {
        const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;

        return method.StartsWith("RuvioConnection.", StringComparison.Ordinal)
            ? typeof(RuvioConnection).GetMethods(flags).Any(info => info.Name == method["RuvioConnection.".Length..])
            : typeof(RuvioClient).GetMethods(flags).Any(info => info.Name == method);
    }

    private static Dictionary<string, string[]> LoadMethods()
    {
        using JsonDocument document = JsonDocument.Parse(File.ReadAllText(FindFile(Path.Combine("clients", "testdata", "command_methods.json"))));
        var methods = new Dictionary<string, string[]>(StringComparer.Ordinal);

        foreach (JsonProperty command in document.RootElement.GetProperty("commands").EnumerateObject())
        {
            methods[command.Name] = command.Value.TryGetProperty("csharp", out JsonElement value)
                ? value.ValueKind == JsonValueKind.Array
                    ? [.. value.EnumerateArray().Select(method => method.GetString()!)]
                    : [value.GetString()!]
                : [];
        }

        return methods;
    }

    internal static string FindMatrix() => FindFile(Path.Combine("docs", "compatibility-matrix.json"));

    private static string FindFile(string relative)
    {
        string? directory = AppContext.BaseDirectory;

        for (int depth = 0; depth < 10 && directory is not null; depth++)
        {
            string candidate = Path.Combine(directory, relative);

            if (File.Exists(candidate))
            {
                return candidate;
            }

            directory = Directory.GetParent(directory)?.FullName;
        }

        throw new FileNotFoundException(relative + " was not found above the test output.");
    }
}
