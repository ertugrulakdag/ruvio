using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Ruvio.Client.AspNetCore;

/// <summary>
/// Actively checks the client's command connection. This is not a readiness test for
/// every shard, replication freshness, persistence, or write availability.
/// </summary>
public sealed class RuvioHealthCheck(IRuvioClient client) : IHealthCheck
{
    /// <inheritdoc />
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
