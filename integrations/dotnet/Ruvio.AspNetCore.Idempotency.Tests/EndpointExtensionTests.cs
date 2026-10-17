using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Ruvio.AspNetCore.Idempotency;
using Xunit;

namespace Ruvio.AspNetCore.Idempotency.Tests;

public class EndpointExtensionTests
{
    [Fact]
    public void WithRuvioIdempotencyAddsTheAttributeAsMetadata()
    {
        var app = WebApplication.CreateBuilder().Build();

        app.MapPost("/x", () => "ok").WithRuvioIdempotency();

        Assert.NotNull(((IEndpointRouteBuilder)app).DataSources.SelectMany(d => d.Endpoints)
            .Single().Metadata.GetMetadata<RuvioIdempotencyAttribute>());
    }
}
