using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Ruvio.Client.AspNetCore;
using Xunit;

namespace Ruvio.Client.AspNetCore.Tests;

public class RegistrationEdgeTests
{
    [Fact]
    public void RegistrationRejectsNullArguments()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddRuvioClient(_ => { }));
        Assert.Throws<ArgumentNullException>(() => services.AddRuvioClient((Action<RuvioClientOptions>)null!));
        Assert.Throws<ArgumentNullException>(() => services.AddRuvioClient(_ => { }, null!));
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddRuvioClient(new ConfigurationBuilder().Build()));
        Assert.Throws<ArgumentNullException>(() => services.AddRuvioClient((IConfiguration)null!));
        Assert.Throws<ArgumentNullException>(() => ((IHealthChecksBuilder)null!).AddRuvio());
    }

    [Fact]
    public void HealthCheckTimeoutMustBePositiveAndAtMostOneMinute()
    {
        IHealthChecksBuilder builder = new ServiceCollection().AddHealthChecks();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.AddRuvio(timeout: TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.AddRuvio(timeout: TimeSpan.FromSeconds(-1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.AddRuvio(timeout: TimeSpan.FromMinutes(1) + TimeSpan.FromTicks(1)));
        builder.AddRuvio(timeout: TimeSpan.FromMinutes(1));
        builder.AddRuvio("tiny", timeout: TimeSpan.FromTicks(1));
    }

    [Fact]
    public void HealthCheckRegistrationCarriesNameTagsAndTimeout()
    {
        var services = new ServiceCollection();

        services.AddHealthChecks().AddRuvio("cache", ["ready", "db"], TimeSpan.FromSeconds(2)).AddRuvio();
        using ServiceProvider provider = services.BuildServiceProvider();
        HealthCheckServiceOptions options = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value;
        HealthCheckRegistration named = Assert.Single(options.Registrations, r => r.Name == "cache");
        HealthCheckRegistration defaults = Assert.Single(options.Registrations, r => r.Name == "ruvio");

        Assert.Equal(["db", "ready"], named.Tags.OrderBy(t => t));
        Assert.Equal(TimeSpan.FromSeconds(2), named.Timeout);
        Assert.Empty(defaults.Tags);
        Assert.Equal(TimeSpan.FromSeconds(5), defaults.Timeout);
    }

    [Fact]
    public async Task HealthCheckDescriptionAndExceptionDescribeTheFailure()
    {
        var failing = System.Reflection.DispatchProxy.Create<IRuvioClient, HealthCheckTests.PingStub>();

        ((HealthCheckTests.PingStub)failing).Reply = Task.FromException<string>(new InvalidOperationException("down"));
        HealthCheckResult down = await new RuvioHealthCheck(failing).CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, down.Status);
        Assert.Contains("unavailable", down.Description);
        Assert.Equal("down", down.Exception!.Message);

        var odd = System.Reflection.DispatchProxy.Create<IRuvioClient, HealthCheckTests.PingStub>();

        ((HealthCheckTests.PingStub)odd).Reply = Task.FromResult("pong");
        HealthCheckResult unexpected = await new RuvioHealthCheck(odd).CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, unexpected.Status);
        Assert.Contains("unexpected", unexpected.Description);
        Assert.Null(unexpected.Exception);

        // A timeout that is not the caller's own token is a failure, not a cancellation.
        var timedOut = System.Reflection.DispatchProxy.Create<IRuvioClient, HealthCheckTests.PingStub>();

        ((HealthCheckTests.PingStub)timedOut).Reply = Task.FromCanceled<string>(new CancellationToken(canceled: true));
        HealthCheckResult result = await new RuvioHealthCheck(timedOut).CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    [Fact(Timeout = 15000)]
    public async Task AThrowingConnectionSetupDisposesTheConnectionAndSurfacesTheError()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint).Port;
        RuvioConnection? seen = null;
        var services = new ServiceCollection();

        services.AddRuvioClient(options =>
        {
            options.Host = "127.0.0.1";
            options.Port = port;
            options.DiscoverCluster = false;
        }, connection =>
        {
            seen = connection;

            throw new InvalidOperationException("setup failed");
        });

        await using ServiceProvider provider = services.BuildServiceProvider();
        InvalidOperationException error = await Task.Run(() =>
            Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<RuvioConnection>()));

        Assert.Equal("setup failed", error.Message);
        Assert.NotNull(seen);
        Assert.False(seen!.IsConnected);
    }

    [Fact(Timeout = 15000)]
    public async Task SeveralSetupsAllRunInRegistrationOrder()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint).Port;
        var order = new List<int>();
        var services = new ServiceCollection();

        services.AddRuvioClient(o => { o.Host = "127.0.0.1"; o.Port = port; o.DiscoverCluster = false; }, _ => order.Add(1));
        services.AddSingleton(new RuvioConnectionSetupProbe());
        services.AddRuvioClient(o => { o.Host = "127.0.0.1"; o.Port = port; o.DiscoverCluster = false; }, _ => order.Add(2));
        await using ServiceProvider provider = services.BuildServiceProvider();

        await Task.Run(() => provider.GetRequiredService<RuvioConnection>());
        Assert.Equal([1, 2], order);
    }

    private sealed class RuvioConnectionSetupProbe;
}
