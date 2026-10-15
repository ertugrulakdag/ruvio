using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.AspNetCore.Tests;

public class HealthCheckTests
{
    [Theory]
    [InlineData("PONG", HealthStatus.Healthy)]
    [InlineData("unexpected", HealthStatus.Unhealthy)]
    public async Task ChecksTheActiveCommandConnection(string reply, HealthStatus expected)
    {
        var client = DispatchProxy.Create<IRuvioClient, PingStub>();

        ((PingStub)client).Reply = Task.FromResult(reply);
        var result = await new RuvioHealthCheck(client).CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(expected, result.Status);
    }

    [Fact]
    public async Task ReportsBackendErrorsAndPreservesCancellation()
    {
        var client = DispatchProxy.Create<IRuvioClient, PingStub>();
        var stub = (PingStub)client;

        stub.Reply = Task.FromException<string>(new IOException("connection lost"));
        var check = new RuvioHealthCheck(client);
        var result = await check.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.IsType<IOException>(result.Exception);
        using var cancellation = new CancellationTokenSource();

        cancellation.Cancel();
        stub.Reply = Task.FromCanceled<string>(cancellation.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            check.CheckHealthAsync(new HealthCheckContext(), cancellation.Token));
        Assert.Equal(cancellation.Token, stub.Token);
    }

    [Fact]
    public async Task RegistrationIsLazyAndUsesTheSharedClient()
    {
        var services = new ServiceCollection();
        int resolutions = 0;

        services.AddSingleton<IRuvioClient>(_ =>
        {
            resolutions++;

            return DispatchProxy.Create<IRuvioClient, PingStub>();
        });

        services.AddLogging();
        services.AddHealthChecks().AddRuvio();
        await using var provider = services.BuildServiceProvider();
        Assert.Equal(0, resolutions);
        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        Assert.Equal(HealthStatus.Healthy, report.Status);
        Assert.Equal(1, resolutions);
    }

    public class PingStub : DispatchProxy
    {
        public Task<string> Reply = Task.FromResult("PONG");
        public CancellationToken Token;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod?.Name != nameof(IRuvioClient.PingAsync)) throw new NotSupportedException();
            Token = (CancellationToken)args![0]!;

            return new ValueTask<string>(Reply);
        }
    }
}
