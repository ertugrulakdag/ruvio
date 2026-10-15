using Microsoft.Extensions.Caching.Distributed;

namespace Ruvio.Extensions.Caching;

/// <summary>Cache-aside helpers for any <see cref="IDistributedCache"/>.</summary>
public static class RuvioDistributedCacheExtensions
{
    /// <summary>
    /// Returns the cached value or builds and stores it. On <see cref="RuvioDistributedCache"/> one caller across all
    /// processes builds the value while the rest wait for it. On any other cache every caller that misses builds it.
    /// </summary>
    public static async Task<byte[]> GetOrCreateAsync(
        this IDistributedCache cache,
        string key,
        Func<CancellationToken, Task<byte[]>> factory,
        DistributedCacheEntryOptions options,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(options);

        if (cache is RuvioDistributedCache ruvio)
            return await ruvio.GetOrCreateAsync(key, factory, options, token).ConfigureAwait(false);

        byte[]? cached = await cache.GetAsync(key, token).ConfigureAwait(false);

        if (cached is not null)
            return cached;

        byte[] value = await factory(token).ConfigureAwait(false);

        await cache.SetAsync(key, value, options, token).ConfigureAwait(false);

        return value;
    }
}
