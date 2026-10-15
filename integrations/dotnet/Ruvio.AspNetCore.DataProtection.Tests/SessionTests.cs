using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ruvio.Client;
using Ruvio.Extensions.Caching;
using Xunit;

namespace Ruvio.AspNetCore.DataProtection.Tests;

public class SessionTests
{
    [LiveFact]
    public async Task Framework_session_cookie_and_binary_session_data_work_across_hosts()
    {
        var address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")!.Split(':');

        await using var firstConnection = await RuvioConnection.ConnectAsync(address[0], int.Parse(address[1]));
        await using var secondConnection = await RuvioConnection.ConnectAsync(address[0], int.Parse(address[1]));
        IRuvioClient firstClient = await firstConnection.GetDefaultDatabaseAsync();
        IRuvioClient secondClient = await secondConnection.GetDefaultDatabaseAsync();
        string name = "session-test-" + Guid.NewGuid().ToString("N");
        string keyRing = name + ":keys";
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=ruvio-session-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddHours(1));

        await using var first = App(firstConnection, firstClient, name, keyRing, certificate);
        await using var second = App(secondConnection, secondClient, name, keyRing, certificate);
        await first.StartAsync();
        await second.StartAsync();
        try
        {
            var cookies = new CookieContainer();
            using var a = Http(first, cookies);
            using var b = Http(second, cookies);

            Assert.Equal("1", await a.GetStringAsync("/visits"));
            Assert.Equal("2", await b.GetStringAsync("/visits"));
            Assert.Equal("3", await a.GetStringAsync("/visits"));
        }

        finally
        {
            await first.StopAsync();
            await second.StopAsync();
            await firstClient.DeleteAsync(keyRing);
        }
    }

    private static WebApplication App(IRuvioConnection connection, IRuvioClient client, string name, string keyRing, X509Certificate2 certificate)
    {
        var builder = WebApplication.CreateBuilder();

        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddSingleton(client);
        builder.Services.AddSingleton(connection);
        builder.Services.AddKeyedSingleton<IRuvioConnection>("key-ring", connection);
        builder.Services.AddRuvioDistributedCache(o => o.InstanceName = name);
        builder.Services.AddSession(o =>
        {
            o.Cookie.Name = name;
            o.Cookie.IsEssential = true;
            o.IdleTimeout = TimeSpan.FromSeconds(5);
        });

        builder.Services.AddDataProtection().SetApplicationName(name)
            .PersistKeysToRuvio(provider => provider.GetRequiredKeyedService<IRuvioConnection>("key-ring"),
                o => o.Key = keyRing)
            .ProtectKeysWithCertificate(certificate);
        var app = builder.Build();

        app.UseSession();
        app.MapGet("/visits", async (HttpContext context) =>
        {
            await context.Session.LoadAsync(context.RequestAborted);
            int visits = (context.Session.GetInt32("visits") ?? 0) + 1;

            context.Session.SetInt32("visits", visits);

            return visits.ToString();
        });

        return app;
    }

    private static HttpClient Http(WebApplication app, CookieContainer cookies) => new(
        new HttpClientHandler { CookieContainer = cookies })
    {
        BaseAddress = new Uri(app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single()),
        Timeout = TimeSpan.FromSeconds(10),
    };
}
