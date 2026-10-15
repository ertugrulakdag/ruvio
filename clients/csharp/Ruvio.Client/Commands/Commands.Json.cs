using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Ruvio.Client;

public sealed partial class RuvioClient
{
    private const string Reflection = "Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.";

    /// <inheritdoc />
    [RequiresUnreferencedCode(Reflection)]
    [RequiresDynamicCode(Reflection)]
    public async ValueTask<T?> JsonGetValueAsync<T>(string key, string path = "$", JsonSerializerOptions? options = null, CancellationToken cancellationToken = default)
    {
        string? json = await JsonGetAsync(key, path, cancellationToken);

        return json is null ? default : JsonSerializer.Deserialize<T>(json, options ?? _options.JsonSerializerOptions);
    }

    /// <inheritdoc />
    public async ValueTask<T?> JsonGetValueAsync<T>(string key, string path, JsonTypeInfo<T> typeInfo, CancellationToken cancellationToken = default)
    {
        string? json = await JsonGetAsync(key, path, cancellationToken);

        return json is null ? default : JsonSerializer.Deserialize(json, typeInfo);
    }

    /// <inheritdoc />
    [RequiresUnreferencedCode(Reflection)]
    [RequiresDynamicCode(Reflection)]
    public ValueTask<bool> JsonSetValueAsync<T>(string key, string path, T value, JsonSerializerOptions? options = null, CancellationToken cancellationToken = default)
    {
        return JsonSetAsync(key, path, JsonSerializer.Serialize(value, options ?? _options.JsonSerializerOptions), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> JsonSetValueAsync<T>(string key, string path, T value, JsonTypeInfo<T> typeInfo, CancellationToken cancellationToken = default)
    {
        return JsonSetAsync(key, path, JsonSerializer.Serialize(value, typeInfo), cancellationToken);
    }

    /// <inheritdoc />
    [RequiresUnreferencedCode(Reflection)]
    [RequiresDynamicCode(Reflection)]
    public ValueTask<bool> JsonMergeValueAsync<T>(string key, string path, T patch, JsonSerializerOptions? options = null, CancellationToken cancellationToken = default)
    {
        return JsonMergeAsync(key, path, JsonSerializer.Serialize(patch, options ?? _options.JsonSerializerOptions), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> JsonMergeValueAsync<T>(string key, string path, T patch, JsonTypeInfo<T> typeInfo, CancellationToken cancellationToken = default)
    {
        return JsonMergeAsync(key, path, JsonSerializer.Serialize(patch, typeInfo), cancellationToken);
    }

    /// <inheritdoc />
    [RequiresUnreferencedCode(Reflection)]
    [RequiresDynamicCode(Reflection)]
    public ValueTask<long?> JsonArrayAppendValueAsync<T>(string key, string path, T value, JsonSerializerOptions? options = null, CancellationToken cancellationToken = default)
    {
        return JsonArrayAppendAsync(key, path, JsonSerializer.Serialize(value, options ?? _options.JsonSerializerOptions), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long?> JsonArrayAppendValueAsync<T>(string key, string path, T value, JsonTypeInfo<T> typeInfo, CancellationToken cancellationToken = default)
    {
        return JsonArrayAppendAsync(key, path, JsonSerializer.Serialize(value, typeInfo), cancellationToken);
    }

}
