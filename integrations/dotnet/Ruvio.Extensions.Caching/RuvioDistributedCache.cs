using System.Diagnostics;
using System.Buffers.Text;
using System.Globalization;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Ruvio.Client;

namespace Ruvio.Extensions.Caching;

/// <summary>
/// A binary-safe distributed cache. Set and the common get are one native
/// command. A sliding renewal is a second command that expires only when the
/// stored bytes are still the ones just read, so a replacement is left alone.
/// The injected client is not disposed by this provider.
/// </summary>
public sealed class RuvioDistributedCache : IDistributedCache
{
    // One compiled script per shard. It does not parse the cache envelope.
    private const string Renew = """
        if ruvio.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
        if ARGV[2] == '0' then return ruvio.call('DEL', KEYS[1]) end
        return ruvio.call('PEXPIREAT', KEYS[1], ARGV[2])
        """;

    private static readonly ReadOnlyMemory<byte> Eval = "EVAL"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> RenewScript = RuvioCacheOptions.Utf8.GetBytes(Renew);
    private static readonly ReadOnlyMemory<byte> One = "1"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> GetCommand = "GET"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> SetCommand = "SET"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> Px = "PX"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> Pxat = "PXAT"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> Delete = "DEL"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> FillGetCommand = "FILLGET"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> FillSetCommand = "FILLSET"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> PersistCommand = "PERSIST"u8.ToArray();
    private readonly IRuvioClient _client;
    private readonly byte[] _prefix;
    private readonly int _maxValueBytes;
    private readonly int _maxKeyBytes;
    private readonly long _fillMilliseconds;
    private readonly long _waitMilliseconds;

    /// <summary>Constructs a provider with validated, snapshotted options and a borrowed client.</summary>
    public RuvioDistributedCache(IRuvioClient client, IOptions<RuvioCacheOptions> options)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(options);
        RuvioCacheOptions settings = options.Value;

        settings.Validate();
        _client = client;
        _prefix = RuvioCacheOptions.Utf8.GetBytes(settings.InstanceName + RuvioCacheOptions.VersionPrefix);
        _maxValueBytes = settings.MaxValueBytes;
        _maxKeyBytes = settings.MaxKeyBytes;
        _fillMilliseconds = (long)settings.FillTimeout.TotalMilliseconds;
        _waitMilliseconds = (long)settings.FillWait.TotalMilliseconds;
    }

    /// <inheritdoc />
    public byte[]? Get(string key) => Task.Run(() => GetAsync(key)).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async Task<byte[]?> GetAsync(string key, CancellationToken token = default)
    {
        ReadOnlyMemory<byte> encoded = Key(key);

        token.ThrowIfCancellationRequested();
        // One async frame on the common path: the read is inlined and the expiry bookkeeping only awaits when it must touch.
        RespValue reply = await _client.ExecuteBinaryAsync([GetCommand, encoded], token).ConfigureAwait(false);

        CheckReply(reply);

        if (reply.Kind == RespKind.Null)
            return null;

        if (reply.Kind != RespKind.Bulk)
            throw UnexpectedReply();

        byte[] stored = reply.Bytes!;
        (long absolute, long sliding, int payload) = Parse(stored);
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        if (absolute > 0 && absolute <= now)
        {
            await Touch(encoded, stored, 0, token).ConfigureAwait(false);

            return null;
        }

        if (sliding > 0)
        {
            long deadline = absolute > 0 ? Math.Min(absolute, now + sliding) : now + sliding;

            await Touch(encoded, stored, deadline <= now ? 0 : deadline, token).ConfigureAwait(false);
        }

        return stored.AsSpan(payload).ToArray();
    }

    /// <summary>Applies the envelope's absolute and sliding expiry to bytes just read, and returns the payload.</summary>
    private async Task<byte[]?> Decode(ReadOnlyMemory<byte> encoded, byte[] stored, CancellationToken token)
    {
        (long absolute, long sliding, int payload) = Parse(stored);
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        if (absolute > 0 && absolute <= now)
        {
            await Touch(encoded, stored, 0, token).ConfigureAwait(false);

            return null;
        }

        if (sliding > 0)
        {
            long deadline = absolute > 0 ? Math.Min(absolute, now + sliding) : now + sliding;

            await Touch(encoded, stored, deadline <= now ? 0 : deadline, token).ConfigureAwait(false);
        }

        return stored.AsSpan(payload).ToArray();
    }

    /// <inheritdoc />
    public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
        => Task.Run(() => SetAsync(key, value, options)).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options,
        CancellationToken token = default)
    {
        ReadOnlyMemory<byte> encoded = Key(key);
        var prepared = Prepare(value, options);
        byte[] record = prepared.Record;

        token.ThrowIfCancellationRequested();
        ReadOnlyMemory<byte>[] command = prepared.Expiry switch
        {
            (string kind, long when) => [SetCommand, encoded, record, kind == "PX" ? Px : Pxat, Number(when)],
            null => [SetCommand, encoded, record],
        };

        RespValue reply = await _client.ExecuteBinaryAsync(command, token).ConfigureAwait(false);

        CheckReply(reply);

        if (reply.Kind != RespKind.Simple || reply.Text != "OK")
            throw UnexpectedReply();
    }

    /// <summary>
    /// Returns the cached value or builds it with <paramref name="factory"/>, letting one caller across every
    /// process build it while the others wait for the result (<c>FILLGET</c> / <c>FILLSET</c>). Without this,
    /// every caller that misses runs the factory. The wait is a polling loop that never blocks the shared connection. A caller that waits out <see cref="RuvioCacheOptions.FillWait"/>
    /// builds and stores the value itself. If the factory throws, the marker lapses after
    /// <see cref="RuvioCacheOptions.FillTimeout"/>.
    /// </summary>
    public async Task<byte[]> GetOrCreateAsync(string key, Func<CancellationToken, Task<byte[]>> factory,
        DistributedCacheEntryOptions options, CancellationToken token = default)
    {
        ReadOnlyMemory<byte> encoded = Key(key);

        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(options);
        token.ThrowIfCancellationRequested();

        // FILLGET ... WAIT parks the whole connection on the server, so on this shared multiplexed connection the
        // waiters would block the builder's own FILLSET (and every other caller) until FillWait ran out.
        // Poll the plain FILLGET instead: a lapsed marker turns the next poll into FILL, a finished fill into HIT.
        byte[] fillMilliseconds = Number(_fillMilliseconds);
        long started = Stopwatch.GetTimestamp();
        int delay = 2;
        FillResult fill;

        while (true)
        {
            RespValue reply = await Execute([FillGetCommand, encoded, fillMilliseconds], token).ConfigureAwait(false);

            fill = FillResult.FromReply(reply);

            if (fill.State != FillState.Busy || Stopwatch.GetElapsedTime(started).TotalMilliseconds >= _waitMilliseconds)
                break;

            await Task.Delay(delay, token).ConfigureAwait(false);
            delay = Math.Min(delay * 2, 25);
        }

        if (fill.State is FillState.Hit or FillState.Stale)
        {
            byte[]? payload = await Decode(encoded, fill.Value!, token).ConfigureAwait(false);

            if (payload is not null)
                return payload;
        }

        byte[] value = await factory(token).ConfigureAwait(false);

        if (fill.State == FillState.Fill)
        {
            var prepared = Prepare(value, options);
            // FILLSET needs a positive TTL; an entry without expiry is stored with one and then persisted.
            long ttl = prepared.Expiry switch
            {
                ("PX", long when) => when,
                (_, long when) => Math.Max(1, when - prepared.Now),
                null => 3_600_000,
            };
            bool persistent = prepared.Expiry is null;

            RespValue stored = await Execute(
                [FillSetCommand, encoded, prepared.Record, Number(ttl), Number(fill.Token!.Value)], token).ConfigureAwait(false);

            if (stored.Kind != RespKind.Integer)
                throw UnexpectedReply();

            // 0 means the marker lapsed while the factory ran; the value is still correct for this caller.
            if (stored.Integer == 1 && persistent)
                _ = await Execute([PersistCommand, encoded], token).ConfigureAwait(false);

            return value;
        }

        await SetAsync(key, value, options, token).ConfigureAwait(false);

        return value;
    }

    /// <summary>Validates a write and builds its envelope and expiry. Returns the current time used.</summary>
    private (byte[] Record, (string Kind, long When)? Expiry, long Now) Prepare(
        byte[] value, DistributedCacheEntryOptions options)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(options);

        if (value.Length > _maxValueBytes)
            throw new ArgumentException("Cache value exceeds MaxValueBytes.", nameof(value));

        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        long absolute = options.AbsoluteExpiration?.ToUnixTimeMilliseconds() ?? 0;

        if (options.AbsoluteExpiration.HasValue && absolute <= now)
            throw new ArgumentOutOfRangeException(nameof(options), "Absolute expiration must be in the future at millisecond precision.");

        long relative = Milliseconds(options.AbsoluteExpirationRelativeToNow);
        long sliding = Milliseconds(options.SlidingExpiration);

        if (relative > DateTimeOffset.MaxValue.ToUnixTimeMilliseconds() - now)
            throw new ArgumentOutOfRangeException(nameof(options), "Relative expiration exceeds the supported date range.");

        if (relative > 0)
            absolute = absolute == 0 ? now + relative : Math.Min(absolute, now + relative);

        return (Envelope(absolute, sliding, value), Expiry(now, absolute, sliding), now);
    }

    /// <inheritdoc />
    public void Refresh(string key) => Task.Run(() => RefreshAsync(key)).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async Task RefreshAsync(string key, CancellationToken token = default)
    {
        _ = await GetAsync(key, token).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Remove(string key) => Task.Run(() => RemoveAsync(key)).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async Task RemoveAsync(string key, CancellationToken token = default)
    {
        ReadOnlyMemory<byte> encoded = Key(key);

        token.ThrowIfCancellationRequested();
        RespValue reply = await Execute([Delete, encoded], token).ConfigureAwait(false);

        if (reply.Kind != RespKind.Integer || reply.Integer is < 0 or > 1)
            throw UnexpectedReply();
    }

    private async Task<byte[]?> Read(ReadOnlyMemory<byte> key, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        RespValue reply = await Execute([GetCommand, key], token).ConfigureAwait(false);

        if (reply.Kind == RespKind.Null)
            return null;

        if (reply.Kind != RespKind.Bulk)
            throw UnexpectedReply();

        return reply.Bytes;
    }

    private async Task Touch(ReadOnlyMemory<byte> key, byte[] stored, long deadline, CancellationToken token)
    {
        RespValue reply = await Execute(
            [Eval, RenewScript, One, key, stored, Number(deadline)], token).ConfigureAwait(false);

        if (reply.Kind != RespKind.Integer || reply.Integer < 0)
            throw UnexpectedReply();
    }

    private static (long Absolute, long Sliding, int Payload) Parse(byte[] stored)
    {
        ReadOnlySpan<byte> body = stored;
        ReadOnlySpan<byte> magic = "RDC1\n"u8;

        if (!body.StartsWith(magic))
            throw UnexpectedReply();

        body = body[magic.Length..];
        int first = body.IndexOf((byte)'\n');

        if (first <= 0 || !long.TryParse(body[..first], NumberStyles.None, CultureInfo.InvariantCulture, out long absolute))
            throw UnexpectedReply();

        body = body[(first + 1)..];
        int second = body.IndexOf((byte)'\n');

        if (second <= 0 || !long.TryParse(body[..second], NumberStyles.None, CultureInfo.InvariantCulture, out long sliding))
            throw UnexpectedReply();

        return (absolute, sliding, magic.Length + first + 1 + second + 1);
    }

    /// <summary><c>RDC1\n{absolute}\n{sliding}\n</c> followed by the payload, in one allocation.</summary>
    private static byte[] Envelope(long absolute, long sliding, byte[] value)
    {
        Span<byte> header = stackalloc byte[5 + 20 + 1 + 20 + 1];
        ReadOnlySpan<byte> magic = "RDC1\n"u8;

        magic.CopyTo(header);
        int written = magic.Length;

        Utf8Formatter.TryFormat(absolute, header[written..], out int digits);
        written += digits;
        header[written++] = (byte)'\n';
        Utf8Formatter.TryFormat(sliding, header[written..], out digits);
        written += digits;
        header[written++] = (byte)'\n';

        byte[] record = new byte[written + value.Length];

        header[..written].CopyTo(record);
        value.CopyTo(record, written);

        return record;
    }

    /// <summary>PX when the sliding window is sooner, otherwise PXAT for an absolute deadline.</summary>
    private static (string Kind, long When)? Expiry(long now, long absolute, long sliding)
    {
        if (sliding > 0 && (absolute == 0 || now + sliding < absolute))
            return ("PX", sliding);

        if (absolute > 0)
            return ("PXAT", absolute);

        return null;
    }

    private static void CheckReply(RespValue reply)
    {
        if (reply.Kind == RespKind.Error)
            throw new RuvioException(reply.Text ?? "Cache command failed.");
    }

    private async Task<RespValue> Execute(IReadOnlyList<ReadOnlyMemory<byte>> arguments, CancellationToken token)
    {
        RespValue reply = await _client.ExecuteBinaryAsync(arguments, token).ConfigureAwait(false);

        if (reply.Kind == RespKind.Error)
            throw new RuvioException(reply.Text ?? "Cache command failed.");

        return reply;
    }

    private byte[] Key(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        // The prefix ends in ASCII, so encoding the parts separately equals encoding the concatenation.
        int keyBytes = RuvioCacheOptions.Utf8.GetByteCount(key);

        if (_prefix.Length + keyBytes > _maxKeyBytes)
            throw new ArgumentException("Namespaced cache key exceeds MaxKeyBytes.", nameof(key));

        byte[] encoded = new byte[_prefix.Length + keyBytes];

        _prefix.CopyTo(encoded, 0);
        RuvioCacheOptions.Utf8.GetBytes(key, encoded.AsSpan(_prefix.Length));

        return encoded;
    }

    private static long Milliseconds(TimeSpan? duration)
    {
        if (!duration.HasValue)
            return 0;

        long value = duration.Value.Ticks / TimeSpan.TicksPerMillisecond;

        if (value <= 0 || value > DateTimeOffset.MaxValue.ToUnixTimeMilliseconds())
            throw new ArgumentOutOfRangeException(nameof(duration), "Expiration must be at least one millisecond and within the supported date range.");

        return value;
    }

    private static byte[] Number(long value)
    {
        Span<byte> digits = stackalloc byte[20];
        Utf8Formatter.TryFormat(value, digits, out int written);

        return digits[..written].ToArray();
    }

    private static RuvioException UnexpectedReply() => new("Unexpected cache command response.");
}
