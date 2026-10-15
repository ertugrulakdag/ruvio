using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.DistributedLock.Tests;

/// <summary>Real Kestrel + real server. Set RUVIO_TEST_ADDR to host:port, otherwise each test returns at once.</summary>
public class HttpTests
{
    private sealed class Host : IAsyncDisposable
    {
        public required RuvioConnection Connection { get; init; }
        public required WebApplication App { get; init; }
        public required HttpClient Http { get; init; }

        public async ValueTask DisposeAsync()
        {
            Http.Dispose();
            await App.DisposeAsync();
            await Connection.DisposeAsync();
        }
    }

    private static async Task<Host?> StartAsync(Action<WebApplication> map, bool mvc = false)
    {
        string? address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR");

        if (string.IsNullOrWhiteSpace(address)) return null;
        var uri = new Uri("tcp://" + address);
        RuvioConnection connection = await RuvioConnection.ConnectAsync(uri.Host, uri.Port);
        IRuvioClient client = await connection.GetDefaultDatabaseAsync();
        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(client);
        builder.Services.AddRuvioDistributedLock(options =>
            options.KeyPrefix = "test:http:" + Guid.NewGuid().ToString("N") + ":");

        if (mvc)
            builder.Services.AddControllers().AddApplicationPart(typeof(HttpTests).Assembly);

        WebApplication app = builder.Build();

        if (mvc) app.MapControllers();
        map(app);
        await app.StartAsync();

        string url = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single();

        return new Host
        {
            Connection = connection,
            App = app,
            Http = new HttpClient { BaseAddress = new Uri(url), Timeout = TimeSpan.FromSeconds(15) },
        };
    }

    [Fact]
    public async Task ASecondRequestForTheSameResourceGets409WhileOtherResourcesAndLaterRequestsProceed()
    {
        var gate = new TaskCompletionSource();
        var started = new TaskCompletionSource();
        int runs = 0;

        await using Host? host = await StartAsync(app => app.MapPost("/orders/{orderId}/pay", async (int orderId) =>
        {
            Interlocked.Increment(ref runs);

            if (orderId == 1)
            {
                started.TrySetResult();
                await gate.Task;
            }

            return Results.Ok(orderId);
        }).WithDistributedLock("pay:{orderId}"));

        if (host is null) return;

        Task<HttpResponseMessage> first = host.Http.PostAsync("/orders/1/pay", null);

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        using HttpResponseMessage duplicate = await host.Http.PostAsync("/orders/1/pay", null);
        using HttpResponseMessage other = await host.Http.PostAsync("/orders/2/pay", null);

        Assert.Equal(HttpStatusCode.Conflict, duplicate.StatusCode);
        Assert.Equal(HttpStatusCode.OK, other.StatusCode);
        Assert.Equal(2, runs); // the duplicate never reached the handler

        gate.SetResult();
        using HttpResponseMessage firstDone = await first;

        Assert.Equal(HttpStatusCode.OK, firstDone.StatusCode);
        using HttpResponseMessage again = await host.Http.PostAsync("/orders/1/pay", null);

        Assert.Equal(HttpStatusCode.OK, again.StatusCode); // released when the first finished
    }

    [Fact]
    public async Task TheLockIsReleasedWhenTheHandlerThrows()
    {
        int calls = 0;

        await using Host? host = await StartAsync(app => app.MapGet("/boom", () =>
        {
            if (Interlocked.Increment(ref calls) == 1) throw new InvalidOperationException("boom");

            return Results.Ok();
        }).WithDistributedLock("boom"));

        if (host is null) return;

        using HttpResponseMessage failed = await host.Http.GetAsync("/boom");
        using HttpResponseMessage next = await host.Http.GetAsync("/boom");

        Assert.Equal(HttpStatusCode.InternalServerError, failed.StatusCode);
        Assert.Equal(HttpStatusCode.OK, next.StatusCode);
    }

    [Fact]
    public async Task WaitLetsTheSecondRequestRunAfterTheFirstAndTheStatusCodeIsConfigurable()
    {
        var gate = new TaskCompletionSource();
        var started = new TaskCompletionSource();
        var slowStarted = new TaskCompletionSource();
        var slowGate = new TaskCompletionSource();

        await using Host? host = await StartAsync(app =>
        {
            app.MapGet("/queued", async () =>
            {
                started.TrySetResult();
                await gate.Task;

                return Results.Ok();
            }).WithDistributedLock("queued", wait: TimeSpan.FromSeconds(5));

            app.MapGet("/impatient", async () =>
            {
                slowStarted.TrySetResult();
                await slowGate.Task;

                return Results.Ok();
            }).WithDistributedLock("impatient", conflictStatusCode: 423);
        });

        if (host is null) return;

        Task<HttpResponseMessage> first = host.Http.GetAsync("/queued");

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Task<HttpResponseMessage> second = host.Http.GetAsync("/queued");

        await Task.Delay(200);
        Assert.False(second.IsCompleted); // waiting, not rejected
        gate.SetResult();

        Assert.Equal(HttpStatusCode.OK, (await first).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await second).StatusCode);

        Task<HttpResponseMessage> slow = host.Http.GetAsync("/impatient");

        await slowStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        using HttpResponseMessage locked = await host.Http.GetAsync("/impatient");

        Assert.Equal((HttpStatusCode)423, locked.StatusCode);
        slowGate.SetResult();
        (await slow).Dispose();
    }

    [Fact]
    public async Task TheHandlerCanReadItsLockAndAMissingRouteValueFailsLoudly()
    {
        await using Host? host = await StartAsync(app =>
        {
            app.MapGet("/job", (HttpContext context) =>
                Results.Ok(new { held = context.GetRuvioLock()?.IsHeld, resource = context.GetRuvioLock()?.Resource }))
                .WithDistributedLock("nightly-job");
            app.MapGet("/typo/{id}", () => Results.Ok()).WithDistributedLock("typo:{ident}");
        });

        if (host is null) return;

        Assert.Contains("\"resource\":\"nightly-job\"", await host.Http.GetStringAsync("/job"));
        using HttpResponseMessage typo = await host.Http.GetAsync("/typo/1");

        Assert.Equal(HttpStatusCode.InternalServerError, typo.StatusCode);
    }

    [Fact]
    public async Task TheMvcAttributeLocksPerRouteValue()
    {
        await using Host? host = await StartAsync(app => app.UseRuvioLock(), mvc: true);

        if (host is null) return;

        Task<HttpResponseMessage> first = host.Http.PostAsync("/mvc-lock/1", null);

        await MvcLockController.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        using HttpResponseMessage duplicate = await host.Http.PostAsync("/mvc-lock/1", null);
        using HttpResponseMessage other = await host.Http.PostAsync("/mvc-lock/2", null);

        Assert.Equal(HttpStatusCode.Conflict, duplicate.StatusCode);
        Assert.Equal(HttpStatusCode.OK, other.StatusCode);
        MvcLockController.Gate.SetResult();
        Assert.Equal(HttpStatusCode.OK, (await first).StatusCode);
    }

    [Fact]
    public void OptionsRejectNonsense()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioLockOptions { ConflictStatusCode = 200 }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioLockOptions { DefaultLease = TimeSpan.Zero }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioLockOptions { DefaultWait = TimeSpan.FromSeconds(-1) }.Validate());
        Assert.Throws<ArgumentException>(() => new RuvioLockHttp.Template("bad:{"));
    }
}

[ApiController]
[Route("mvc-lock")]
public class MvcLockController : ControllerBase
{
    public static readonly TaskCompletionSource Started = new();
    public static readonly TaskCompletionSource Gate = new();

    [HttpPost("{orderId}")]
    [RuvioLock("mvc:{orderId}")]
    public async Task<IActionResult> Pay(int orderId)
    {
        if (orderId == 1)
        {
            Started.TrySetResult();
            await Gate.Task;
        }

        return Ok();
    }
}
