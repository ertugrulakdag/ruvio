using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace Ruvio.AspNetCore.RateLimiting;

/// <summary>Registers and applies Ruvio-backed rate limiting.</summary>
public static class RuvioRateLimitingExtensions
{
    /// <summary>
    /// Adds Ruvio rate limiting services to the dependency injection container.
    /// </summary>
    /// <param name="services">The service collection to add the rate limiting services to.</param>
    /// <param name="configure">A delegate to configure the Ruvio rate limiting options.</param>
    /// <returns>The updated service collection.</returns>
    public static IServiceCollection AddRuvioRateLimiting(
        this IServiceCollection services, Action<RuvioRateLimitingOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<RuvioRateLimitingOptions>()
            .Configure(configure)
            .Validate(options => options.PartitionKey is not null, "PartitionKey is required.")
            .Validate(options => !string.IsNullOrWhiteSpace(options.KeyPrefix)
                && options.KeyPrefix.Length <= 128
                && !options.KeyPrefix.Contains('{') && !options.KeyPrefix.Contains('}'),
                "KeyPrefix must have 1-128 characters and no cluster hash tags.")
            .ValidateOnStart();

        return services;
    }

    /// <summary>Place after routing, authentication and authorization, before endpoint execution.</summary>
    public static IApplicationBuilder UseRuvioRateLimiting(this IApplicationBuilder app)
        => app.UseMiddleware<RuvioRateLimitingMiddleware>();
}
