using Microsoft.AspNetCore.Http;

namespace Ruvio.AspNetCore.RateLimiting;

/// <summary>Settings for Ruvio-backed rate limiting.</summary>
public sealed class RuvioRateLimitingOptions
{
    /// <summary>Application-specific namespace. Distinct deployments should use distinct prefixes.</summary>
    public string KeyPrefix { get; set; } = "ruvio:rate:v1:";

    /// <summary>
    /// Required trusted tenant/user identifier, or an explicit constant for a global budget.
    /// Runs after authentication/authorization. Do not trust arbitrary forwarded headers.
    /// </summary>
    public Func<HttpContext, string>? PartitionKey { get; set; }
}
