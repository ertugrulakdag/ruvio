namespace Ruvio.AspNetCore.RateLimiting;

/// <summary>A shared fixed-window budget. Endpoints with the same policy share its budget.</summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class)]
public sealed class RuvioRateLimitAttribute : Attribute
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RuvioRateLimitAttribute"/> class with the specified policy, permits, and window duration.
    /// </summary>
    /// <param name="policy">The rate-limiting policy name.</param>
    /// <param name="permits">The number of permits allowed within the window.</param>
    /// <param name="windowSeconds">The duration of the fixed window in seconds.</param>
    public RuvioRateLimitAttribute(string policy, long permits, int windowSeconds)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(policy);

        if (policy.Length > 128)
            throw new ArgumentOutOfRangeException(nameof(policy));

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(permits);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(windowSeconds);

        Policy = policy;
        Permits = permits;
        WindowSeconds = windowSeconds;
    }

    /// <summary>
    /// Gets the rate-limiting policy name.
    /// </summary>
    public string Policy { get; }

    /// <summary>
    /// Gets the number of permits allowed within the window.
    /// </summary>
    public long Permits { get; }

    /// <summary>
    /// Gets the duration of the fixed window in seconds.
    /// </summary>
    public int WindowSeconds { get; }
}
