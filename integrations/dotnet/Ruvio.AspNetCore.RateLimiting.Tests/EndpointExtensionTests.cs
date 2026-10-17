using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Ruvio.AspNetCore.RateLimiting;
using Xunit;

namespace Ruvio.AspNetCore.RateLimiting.Tests;

public class EndpointExtensionTests
{
    [Fact]
    public void WithRuvioRateLimitAddsTheAttributeAsMetadata()
    {
        var app = WebApplication.CreateBuilder().Build();
        var endpoint = app.MapGet("/x", () => "ok").WithRuvioRateLimit("p", 5, TimeSpan.FromMilliseconds(1500));
        var attribute = ((IEndpointRouteBuilder)app).DataSources.SelectMany(d => d.Endpoints)
            .Single().Metadata.GetMetadata<RuvioRateLimitAttribute>();

        Assert.NotNull(endpoint);
        Assert.Equal(("p", 5L, 2), (attribute!.Policy, attribute.Permits, attribute.WindowSeconds));
    }
}
