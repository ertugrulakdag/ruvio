using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using Ruvio.Client;
using StackExchange.Redis;

internal interface IScenarioStorage
{
    Task<byte[]?> GetCacheAsync(string key, CancellationToken cancellationToken);
    Task SetCacheAsync(string key, byte[] value, TimeSpan lifetime, CancellationToken cancellationToken);
    Task<(string State, string? Result)> BeginIdempotencyAsync(
        string key, string fingerprint, string owner, TimeSpan lifetime, CancellationToken cancellationToken);
    Task CompleteIdempotencyAsync(
        string key, string fingerprint, string owner, string result, CancellationToken cancellationToken);
    Task<long?> LimitAsync(string key, long permits, TimeSpan window, CancellationToken cancellationToken);
}

internal sealed class ScenarioStorage(
    IRuvioClient ruvio,
    IConnectionMultiplexer redis) : IScenarioStorage
{
    private static readonly ActivitySource Activities = new("Ruvio.ScenarioLab.Storage");
    private static readonly Meter Meter = new("Ruvio.ScenarioLab");
    private static readonly Counter<long> Calls = Meter.CreateCounter<long>("scenario_storage_calls_total");
    private static readonly Histogram<double> Duration = Meter.CreateHistogram<double>(
        "scenario_storage_duration_milliseconds", "ms");

    private const string BeginScript = """
        local fingerprint = redis.call('HGET', KEYS[1], 'fingerprint')
        if not fingerprint then
          redis.call('HSET', KEYS[1], 'fingerprint', ARGV[1], 'owner', ARGV[2], 'state', 'acquired')
          redis.call('PEXPIRE', KEYS[1], ARGV[3])
          return {'acquired'}
        end
        if fingerprint ~= ARGV[1] then return {'conflict'} end
        if redis.call('HGET', KEYS[1], 'state') == 'completed' then
          return {'completed', redis.call('HGET', KEYS[1], 'result')}
        end
        return {'pending'}
        """;

    private const string CompleteScript = """
        if redis.call('HGET', KEYS[1], 'fingerprint') ~= ARGV[1] then return 0 end
        if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return 0 end
        local existing = redis.call('HGET', KEYS[1], 'result')
        if existing then return existing == ARGV[3] and 1 or 0 end
        redis.call('HSET', KEYS[1], 'state', 'completed', 'result', ARGV[3])
        return 1
        """;

    private const string LimitScript = """
        local used = tonumber(redis.call('GET', KEYS[1]) or '0')
        local maximum = tonumber(ARGV[1])
        if used >= maximum then return -1 end
        if used == 0 then
          used = 1
          redis.call('SET', KEYS[1], used, 'PX', ARGV[2])
        else
          used = redis.call('INCR', KEYS[1])
        end
        return maximum - used
        """;

    private IDatabase Database => redis.GetDatabase();

    public Task<byte[]?> GetCacheAsync(string key, CancellationToken cancellationToken)
        => Measure("cache.get", async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (BackendForCurrentRequest() == "redis")
            {
                RedisValue value = await Database.StringGetAsync(key).ConfigureAwait(false);

                return value.IsNull ? null : (byte[]?)value;
            }

            RespValue reply = await RuvioCall(
                [Text("GET"), Text(key)], cancellationToken).ConfigureAwait(false);

            return reply.Kind switch
            {
                RespKind.Null => null,
                RespKind.Bulk => reply.Bytes,
                _ => throw new RuvioException("Unexpected cache GET response.")
            };
        });

    public Task SetCacheAsync(string key, byte[] value, TimeSpan lifetime, CancellationToken cancellationToken)
        => Measure("cache.set", async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (BackendForCurrentRequest() == "redis")
            {
                await Database.StringSetAsync(key, value, lifetime).ConfigureAwait(false);

                return;
            }

            RespValue reply = await RuvioCall(
                [Text("SET"), Text(key), value, Text("PX"), Text(
                    ((long)lifetime.TotalMilliseconds).ToString(CultureInfo.InvariantCulture))],
                cancellationToken).ConfigureAwait(false);

            if (reply.Kind != RespKind.Simple || reply.Text != "OK")
                throw new RuvioException("Unexpected cache SET response.");
        });

    public Task<(string State, string? Result)> BeginIdempotencyAsync(
        string key, string fingerprint, string owner, TimeSpan lifetime, CancellationToken cancellationToken)
        => Measure("idempotency.begin", async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (BackendForCurrentRequest() == "redis")
            {
                RedisResult result = await Database.ScriptEvaluateAsync(
                    BeginScript,
                    [key],
                    [
                        fingerprint,
                        owner,
                        (long)lifetime.TotalMilliseconds
                    ]).ConfigureAwait(false);
                RedisResult[] values = (RedisResult[])result!;

                return (values[0].ToString(), values.Length > 1 ? values[1].ToString() : null);
            }

            RespValue reply = await RuvioCall(
                [
                    Text("IDEM"), Text(key), Text("BEGIN"), Text(fingerprint), Text(owner),
                    Text(((long)lifetime.TotalMilliseconds).ToString(CultureInfo.InvariantCulture))
                ],
                cancellationToken).ConfigureAwait(false);

            if (reply.Kind != RespKind.Array || reply.Items is not { Count: > 0 } items)
                throw new RuvioException("Unexpected IDEM BEGIN response.");

            return (items[0].AsString() ?? throw new RuvioException("IDEM state is missing."),
                items.Count > 1 ? items[1].AsString() : null);
        });

    public Task CompleteIdempotencyAsync(
        string key, string fingerprint, string owner, string result, CancellationToken cancellationToken)
        => Measure("idempotency.complete", async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (BackendForCurrentRequest() == "redis")
            {
                long completed = (long)await Database.ScriptEvaluateAsync(
                    CompleteScript, [key], [fingerprint, owner, result]).ConfigureAwait(false);

                if (completed != 1)
                    throw new InvalidOperationException("Redis idempotency completion did not match its owner.");

                return;
            }

            if (!await ruvio.CompleteIdempotencyAsync(
                key, fingerprint, owner, result, cancellationToken).ConfigureAwait(false))
                throw new InvalidOperationException("Ruvio idempotency completion did not match its owner.");
        });

    public Task<long?> LimitAsync(
        string key, long permits, TimeSpan window, CancellationToken cancellationToken)
        => Measure("rate_limit", async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (BackendForCurrentRequest() == "redis")
            {
                long remaining = (long)await Database.ScriptEvaluateAsync(
                    LimitScript,
                    [key],
                    [permits, (long)window.TotalMilliseconds]).ConfigureAwait(false);

                return remaining < 0 ? null : remaining;
            }

            return await ruvio.LimitAsync(
                key, permits, (long)window.TotalMilliseconds, cancellationToken).ConfigureAwait(false);
        });

    private async Task<RespValue> RuvioCall(
        ReadOnlyMemory<byte>[] arguments, CancellationToken cancellationToken)
        => await ruvio.ExecuteBinaryAsync(arguments, cancellationToken).ConfigureAwait(false);

    private static async Task Measure(string operation, Func<Task> action)
    {
        await Measure(operation, async () =>
        {
            await action().ConfigureAwait(false);

            return true;
        }).ConfigureAwait(false);
    }

    private static async Task<T> Measure<T>(string operation, Func<Task<T>> action)
    {
        long started = Stopwatch.GetTimestamp();
        using var activity = Activities.StartActivity(operation, ActivityKind.Internal);
        activity?.SetTag("scenario.storage.operation", operation);
        activity?.SetTag("scenario.storage.backend", BackendForCurrentRequest());
        try
        {
            using var clientActivity = Activities.StartActivity("db.client", ActivityKind.Client);
            clientActivity?.SetTag("db.system", BackendForCurrentRequest());
            clientActivity?.SetTag("db.operation.name", operation);
            T result = await action().ConfigureAwait(false);
            activity?.SetStatus(ActivityStatusCode.Ok);

            return result;
        }
        catch (Exception error)
        {
            activity?.SetStatus(ActivityStatusCode.Error, error.GetType().Name);
            activity?.SetTag("error.type", error.GetType().FullName);
            throw;
        }
        finally
        {
            var tags = new TagList
            {
                { "backend", BackendForCurrentRequest() },
                { "operation", operation },
                { "test_run", (string?)ScenarioRequestContext.Current?.Items["scenario.test_run"] ?? "unscoped" },
                { "load_stage", (string?)ScenarioRequestContext.Current?.Items["scenario.load_stage"] ?? "unscoped" }
            };
            Calls.Add(1, tags);
            Duration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds, tags);
        }
    }

    private static string BackendForCurrentRequest()
        => (string?)ScenarioRequestContext.Current?.Items["scenario.backend"] ?? "ruvio";

    private static byte[] Text(string value) => Encoding.UTF8.GetBytes(value);
}

internal static class ScenarioRequestContext
{
    private static readonly AsyncLocal<HttpContext?> CurrentContext = new();
    public static HttpContext? Current
    {
        get => CurrentContext.Value;
        set => CurrentContext.Value = value;
    }
}
