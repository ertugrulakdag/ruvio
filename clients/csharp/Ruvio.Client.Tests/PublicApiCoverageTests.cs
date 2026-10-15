using System.Reflection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class PublicApiCoverageTests
{
    [Fact]
    public void EveryPublicRuvioClientMethodIsNamedInATest()
    {
        string testsDir = Path.GetDirectoryName(typeof(PublicApiCoverageTests).Assembly.Location)!;
        string? search = testsDir;
        string[] sources = [];

        for (int depth = 0; depth < 8 && search is not null; depth++)
        {
            string[] found = Directory.GetFiles(search, "*Tests.cs");

            if (found.Length > 0 && found.Any(path => path.EndsWith("CommandSurfaceTests.cs", StringComparison.Ordinal)))
            {
                sources = found;
                break;
            }

            search = Directory.GetParent(search)?.FullName;
        }

        Assert.NotEmpty(sources);
        string body = string.Join('\n', sources.Select(File.ReadAllText));
        string[] ignored =
        [
            "ToString", "Equals", "GetHashCode", "GetType", "Dispose", "DisposeAsync",
        ];

        List<string> missing = typeof(RuvioClient)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method => !method.IsSpecialName && !ignored.Contains(method.Name) && !method.Name.Contains('.'))
            .Select(method => method.Name)
            .Distinct()
            .Where(name => !body.Contains(name, StringComparison.Ordinal))
            .OrderBy(name => name)
            .ToList();

        Assert.True(
            missing.Count == 0,
            "Untested public APIs: " + string.Join(", ", missing) + Environment.NewLine +
            "Add a scripted test that calls each name.");
    }
}
