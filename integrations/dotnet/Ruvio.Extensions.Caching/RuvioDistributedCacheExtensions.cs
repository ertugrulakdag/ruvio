using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
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

    private static readonly UTF8Encoding Utf8 = new(false);

    private static DistributedCacheEntryOptions Default(DistributedCacheEntryOptions? options) => options ?? new();

    /// <summary>Returns the cached string, or builds and stores it (see <see cref="GetOrCreateAsync(IDistributedCache, string, Func{CancellationToken, Task{byte[]}}, DistributedCacheEntryOptions, CancellationToken)"/>). Stored as UTF-8.</summary>
    public static async Task<string> GetOrCreateStringAsync(
        this IDistributedCache cache,
        string key,
        Func<CancellationToken, Task<string>> factory,
        DistributedCacheEntryOptions? options = null,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(factory);

        byte[] bytes = await cache.GetOrCreateAsync(
            key,
            async ct => Utf8.GetBytes(await factory(ct).ConfigureAwait(false)),
            Default(options),
            token).ConfigureAwait(false);

        return Utf8.GetString(bytes);
    }

    /// <summary>Stores <paramref name="value"/> serialized to UTF-8 JSON with <paramref name="serializerOptions"/> (web defaults when null).</summary>
    [RequiresUnreferencedCode("Reflection-based JSON; pass a JsonTypeInfo for trimming.")]
    [RequiresDynamicCode("Reflection-based JSON; pass a JsonTypeInfo for AOT.")]
    public static Task SetJsonAsync<T>(
        this IDistributedCache cache,
        string key,
        T value,
        DistributedCacheEntryOptions? options = null,
        JsonSerializerOptions? serializerOptions = null,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);

        return cache.SetAsync(key, JsonSerializer.SerializeToUtf8Bytes(value, serializerOptions), Default(options), token);
    }

    /// <summary>Stores <paramref name="value"/> serialized with a source-generated <paramref name="typeInfo"/> (trim and AOT safe).</summary>
    public static Task SetJsonAsync<T>(
        this IDistributedCache cache,
        string key,
        T value,
        JsonTypeInfo<T> typeInfo,
        DistributedCacheEntryOptions? options = null,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(typeInfo);

        return cache.SetAsync(key, JsonSerializer.SerializeToUtf8Bytes(value, typeInfo), Default(options), token);
    }

    /// <summary>Reads the entry and deserializes it from JSON. Returns <c>default</c> on a miss.</summary>
    [RequiresUnreferencedCode("Reflection-based JSON; pass a JsonTypeInfo for trimming.")]
    [RequiresDynamicCode("Reflection-based JSON; pass a JsonTypeInfo for AOT.")]
    public static async Task<T?> GetJsonAsync<T>(
        this IDistributedCache cache,
        string key,
        JsonSerializerOptions? serializerOptions = null,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);

        byte[]? bytes = await cache.GetAsync(key, token).ConfigureAwait(false);

        return bytes is null ? default : JsonSerializer.Deserialize<T>(bytes, serializerOptions);
    }

    /// <summary>Reads the entry and deserializes it with a source-generated <paramref name="typeInfo"/>. Returns <c>default</c> on a miss.</summary>
    public static async Task<T?> GetJsonAsync<T>(
        this IDistributedCache cache,
        string key,
        JsonTypeInfo<T> typeInfo,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(typeInfo);

        byte[]? bytes = await cache.GetAsync(key, token).ConfigureAwait(false);

        return bytes is null ? default : JsonSerializer.Deserialize(bytes, typeInfo);
    }

    /// <summary>Returns the cached model, or builds it with <paramref name="factory"/> and stores it as JSON.</summary>
    [RequiresUnreferencedCode("Reflection-based JSON; pass a JsonTypeInfo for trimming.")]
    [RequiresDynamicCode("Reflection-based JSON; pass a JsonTypeInfo for AOT.")]
    public static async Task<T> GetOrCreateJsonAsync<T>(
        this IDistributedCache cache,
        string key,
        Func<CancellationToken, Task<T>> factory,
        DistributedCacheEntryOptions? options = null,
        JsonSerializerOptions? serializerOptions = null,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(factory);

        byte[] bytes = await cache.GetOrCreateAsync(
            key,
            async ct => JsonSerializer.SerializeToUtf8Bytes(await factory(ct).ConfigureAwait(false), serializerOptions),
            Default(options),
            token).ConfigureAwait(false);

        return JsonSerializer.Deserialize<T>(bytes, serializerOptions)!;
    }

    /// <summary>Same as the options overload, with a source-generated <paramref name="typeInfo"/>.</summary>
    public static async Task<T> GetOrCreateJsonAsync<T>(
        this IDistributedCache cache,
        string key,
        Func<CancellationToken, Task<T>> factory,
        JsonTypeInfo<T> typeInfo,
        DistributedCacheEntryOptions? options = null,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(typeInfo);

        byte[] bytes = await cache.GetOrCreateAsync(
            key,
            async ct => JsonSerializer.SerializeToUtf8Bytes(await factory(ct).ConfigureAwait(false), typeInfo),
            Default(options),
            token).ConfigureAwait(false);

        return JsonSerializer.Deserialize(bytes, typeInfo)!;
    }

    /// <summary>Synchronous <see cref="SetJsonAsync{T}(IDistributedCache, string, T, DistributedCacheEntryOptions?, JsonSerializerOptions?, CancellationToken)"/>.</summary>
    [RequiresUnreferencedCode("Reflection-based JSON; pass a JsonTypeInfo for trimming.")]
    [RequiresDynamicCode("Reflection-based JSON; pass a JsonTypeInfo for AOT.")]
    public static void SetJson<T>(
        this IDistributedCache cache,
        string key,
        T value,
        DistributedCacheEntryOptions? options = null,
        JsonSerializerOptions? serializerOptions = null)
    {
        ArgumentNullException.ThrowIfNull(cache);
        cache.Set(key, JsonSerializer.SerializeToUtf8Bytes(value, serializerOptions), Default(options));
    }

    /// <summary>Synchronous <see cref="GetJsonAsync{T}(IDistributedCache, string, JsonSerializerOptions?, CancellationToken)"/>.</summary>
    [RequiresUnreferencedCode("Reflection-based JSON; pass a JsonTypeInfo for trimming.")]
    [RequiresDynamicCode("Reflection-based JSON; pass a JsonTypeInfo for AOT.")]
    public static T? GetJson<T>(this IDistributedCache cache, string key, JsonSerializerOptions? serializerOptions = null)
    {
        ArgumentNullException.ThrowIfNull(cache);

        byte[]? bytes = cache.Get(key);

        return bytes is null ? default : JsonSerializer.Deserialize<T>(bytes, serializerOptions);
    }
}
