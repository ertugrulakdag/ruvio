using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Ruvio.Client.AspNetCore;

/// <summary>
/// Actively checks the client's command connection. This is not a readiness test for
/// every shard, replication freshness, persistence, or write availability.
/// </summary>
/// <remarks>
/// Sends <c>PING</c> on the registered <see cref="IRuvioClient"/>. A <c>PONG</c> is healthy; any other reply, a server
/// error or a connection failure is unhealthy. Register it with <see cref="RuvioHealthCheckExtensions.AddRuvio"/>.
/// </remarks>
/// <param name="client">The client to probe.</param>
public sealed class RuvioHealthCheck(IRuvioClient client) : IHealthCheck
{
    /// <summary>Sends <c>PING</c> and reports whether the server answered <c>PONG</c>.</summary>
    /// <param name="context">The health-check context supplied by the framework.</param>
    /// <param name="cancellationToken">Cancels the probe; the cancellation is rethrown rather than reported as unhealthy.</param>
    /// <returns>Healthy on <c>PONG</c>; unhealthy (with the exception, if any) otherwise.</returns>
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            string reply = await client.PingAsync(cancellationToken);

            return reply == "PONG"
                ? HealthCheckResult.Healthy()
                : HealthCheckResult.Unhealthy("Ruvio returned an unexpected PING response.");
        }

        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }

        catch (Exception error)
        {
            return HealthCheckResult.Unhealthy("Ruvio command connection is unavailable.", error);
        }
    }
}

/// <summary>Opt-in Ruvio connectivity checks for the .NET health-check system.</summary>
public static class RuvioHealthCheckExtensions
{
    /// <summary>
    /// Adds a bounded PING probe using the existing IRuvioClient. No polling task is created.
    /// </summary>
    /// <param name="builder">The health-check builder.</param>
    /// <param name="name">The check name shown in the health report. Default <c>ruvio</c>.</param>
    /// <param name="tags">Optional tags for filtering checks, such as <c>ready</c>.</param>
    /// <param name="timeout">How long the probe may take before the check fails. Default 5 seconds; at most one minute.</param>
    /// <returns>The same builder, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is zero, negative or over one minute.</exception>
    /// <example>
    /// <code>
    /// builder.Services.AddHealthChecks().AddRuvio(tags: new[] { "ready" });
    /// </code>
    /// </example>
    public static IHealthChecksBuilder AddRuvio(
        this IHealthChecksBuilder builder,
        string name = "ruvio",
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        TimeSpan limit = timeout ?? TimeSpan.FromSeconds(5);

        if (limit <= TimeSpan.Zero || limit > TimeSpan.FromMinutes(1))
            throw new ArgumentOutOfRangeException(nameof(timeout), "Use a timeout up to one minute.");

        return builder.AddCheck<RuvioHealthCheck>(name, tags: tags, timeout: limit);
    }
}
