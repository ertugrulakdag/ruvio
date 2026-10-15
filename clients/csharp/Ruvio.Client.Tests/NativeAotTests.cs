using System.Diagnostics;
using System.Runtime.InteropServices;
using Xunit;

namespace Ruvio.Client.Tests;

public class NativeAotTests
{
    [Fact]
    public void TourPublishesANativeBinaryWithoutTheManagedClientDll()
    {
        if (Environment.GetEnvironmentVariable("RUVIO_AOT_PUBLISH") != "1")
        {
            return;
        }

        string tour = FindTourProject();
        string output = Path.Combine(Path.GetTempPath(), "ruvio-aot-" + Guid.NewGuid().ToString("N"));
        var start = new ProcessStartInfo(
            "dotnet",
            $"publish \"{tour}\" -c Release -o \"{output}\" --nologo")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        using Process process = Process.Start(start) ?? throw new InvalidOperationException("dotnet did not start");
        string stdout = process.StandardOutput.ReadToEnd();
        string stderr = process.StandardError.ReadToEnd();

        if (!process.WaitForExit(180_000))
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException("NativeAOT publish did not finish");
        }

        Assert.True(
            process.ExitCode == 0,
            $"dotnet publish exited {process.ExitCode}{Environment.NewLine}{stdout}{Environment.NewLine}{stderr}");

        string executable = Path.Combine(
            output,
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Ruvio.Client.Tour.exe" : "Ruvio.Client.Tour");

        Assert.True(File.Exists(executable));
        Assert.False(File.Exists(Path.Combine(output, "Ruvio.Client.dll")));
    }

    private static string FindTourProject()
    {
        string? directory = AppContext.BaseDirectory;

        for (int depth = 0; depth < 8 && directory is not null; depth++)
        {
            string candidate = Path.Combine(directory, "clients", "csharp", "Ruvio.Client.Tour", "Ruvio.Client.Tour.csproj");

            if (File.Exists(candidate))
            {
                return candidate;
            }

            directory = Directory.GetParent(directory)?.FullName;
        }

        throw new FileNotFoundException("Ruvio.Client.Tour.csproj was not found above the test output.");
    }
}
