using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Ruvio.Client;

/// <summary>
/// A distributed lock held on one string key: acquired with <c>SET key token NX PX ttl</c>, renewed with
/// <c>SET key token IFEQ token PX ttl</c>, released with <c>DELEX key IFEQ token</c>. Release and renew only touch
/// the key while it still holds this owner's token, so a lock that expired and was taken by someone else is never
/// deleted. <c>await using</c> releases it.
/// </summary>
public sealed class RuvioLock : IAsyncDisposable
{
    private readonly IRuvioClient _client;
    private int _released;

    internal RuvioLock(IRuvioClient client, string key, string token, TimeSpan ttl)
    {
        _client = client;
        Key = key;
        Token = token;
        Ttl = ttl;
    }

    /// <summary>The lock key.</summary>
    public string Key { get; }

    /// <summary>This owner's random token, the value stored at <see cref="Key"/>.</summary>
    public string Token { get; }

    /// <summary>The lease length used by <see cref="RenewAsync"/> when none is given.</summary>
    public TimeSpan Ttl { get; }

    /// <summary>True after <see cref="ReleaseAsync"/> or <see cref="DisposeAsync"/> ran.</summary>
    public bool IsReleased => Volatile.Read(ref _released) != 0;

    /// <summary>Extends the lock to <paramref name="ttl"/> (default <see cref="Ttl"/>) from now. False when the lock was lost.</summary>
    public ValueTask<bool> RenewAsync(TimeSpan? ttl = null, CancellationToken cancellationToken = default) =>
        IsReleased
            ? new ValueTask<bool>(false)
            : _client.SetIfEqualAsync(Key, Token, Token, ttl ?? Ttl, cancellationToken);

    /// <summary>Deletes the key only while it holds this token. False when the lock had already expired or moved on.</summary>
    public ValueTask<bool> ReleaseAsync(CancellationToken cancellationToken = default) =>
        Interlocked.Exchange(ref _released, 1) != 0
            ? new ValueTask<bool>(false)
            : _client.DeleteIfEqualAsync(Key, Token, cancellationToken);

    /// <summary>Releases the lock (once). Errors are not swallowed.</summary>
    public async ValueTask DisposeAsync() => _ = await ReleaseAsync().ConfigureAwait(false);
}

/// <summary>
/// A token from <c>LEASE</c> (a fencing token: it only grows for one key) or from <c>SEMAPHORE</c> (one slot).
/// <c>await using</c> sends <c>RELEASE key token</c>, which frees it only while this token still holds it.
/// </summary>
public sealed class RuvioLease : IAsyncDisposable
{
    private readonly IRuvioClient client;
    private int released;

    internal RuvioLease(IRuvioClient client, string key, long token)
    {
        this.client = client;
        Key = key;
        Token = token;
    }

    /// <summary>The lease or semaphore key.</summary>
    public string Key { get; }

    /// <summary>The token. For <c>LEASE</c> it is a fencing token: pass it to the guarded resource and reject smaller ones.</summary>
    public long Token { get; }

    /// <summary>True after <see cref="ReleaseAsync"/> or <see cref="DisposeAsync"/> ran.</summary>
    public bool IsReleased => Volatile.Read(ref released) != 0;

    /// <summary>Sends <c>RELEASE key token</c> once. False when the token no longer held it.</summary>
    public async ValueTask<bool> ReleaseAsync(CancellationToken cancellationToken = default) =>
        Interlocked.Exchange(ref released, 1) == 0
        && await client.ReleaseAsync(Key, Token, cancellationToken).ConfigureAwait(false) > 0;

    /// <summary>Releases the token (once).</summary>
    public async ValueTask DisposeAsync() => _ = await ReleaseAsync().ConfigureAwait(false);
}

/// <summary>
/// Higher-level patterns built on Ruvio's native commands: locks, leases, semaphores, rate limits,
/// cache-aside with stampede protection, scan iterators, recent-items lists and "seen before" filters.
/// They work on any <see cref="IRuvioClient"/>.
/// </summary>
public static class RuvioClientHelpers
{
    private const int MaxBackoffMilliseconds = 25;

    // ---- Locks, leases, semaphores -------------------------------------------------------------

    /// <summary>
    /// Tries once to take the lock at <paramref name="key"/> for <paramref name="ttl"/>. Null when another owner holds it.
    /// <code>await using RuvioLock? held = await db.TryAcquireLockAsync("lock:job", TimeSpan.FromSeconds(10));</code>
    /// </summary>
    public static async ValueTask<RuvioLock?> TryAcquireLockAsync(
        this IRuvioClient client,
        string key,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        if (ttl <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(ttl), "The lock TTL must be positive.");
        }

        string token = NewToken();

        return await client.SetAsync(key, token, ttl, SetMode.IfNotExists, cancellationToken).ConfigureAwait(false)
            ? new RuvioLock(client, key, token, ttl)
            : null;
    }

    /// <summary>
    /// Takes the lock at <paramref name="key"/>, retrying with a short backoff (2 to 25 ms) for up to <paramref name="wait"/>.
    /// Null when it is still held after that.
    /// </summary>
    public static async ValueTask<RuvioLock?> AcquireLockAsync(
        this IRuvioClient client,
        string key,
        TimeSpan ttl,
        TimeSpan wait,
        CancellationToken cancellationToken = default)
    {
        long started = Stopwatch.GetTimestamp();
        int delay = 2;

        while (true)
        {
            if (await client.TryAcquireLockAsync(key, ttl, cancellationToken).ConfigureAwait(false) is { } held)
            {
                return held;
            }

            if (Stopwatch.GetElapsedTime(started) >= wait)
            {
                return null;
            }

            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            delay = Math.Min(delay * 2, MaxBackoffMilliseconds);
        }
    }

    /// <summary>Sends <c>LEASE key ttl</c>. Null while another holder's lease is live; otherwise a fenced lease.</summary>
    public static async ValueTask<RuvioLease?> TryAcquireLeaseAsync(
        this IRuvioClient client,
        string key,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long? token = await client.LeaseAsync(key, RespRead.Milliseconds(ttl), cancellationToken).ConfigureAwait(false);

        return token is { } value ? new RuvioLease(client, key, value) : null;
    }

    /// <summary>Sends <c>SEMAPHORE key max ttl</c>. Null when <paramref name="max"/> holders are live; otherwise one slot.</summary>
    public static async ValueTask<RuvioLease?> TryAcquireSemaphoreAsync(
        this IRuvioClient client,
        string key,
        long max,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long? token = await client.AcquireSemaphoreAsync(key, max, RespRead.Milliseconds(ttl), cancellationToken).ConfigureAwait(false);

        return token is { } value ? new RuvioLease(client, key, value) : null;
    }

    // ---- Rate limit, idempotency -----------------------------------------------------------------

    /// <summary>
    /// One fixed-window rate-limit decision with <c>LIMIT key max window</c>. A refused call does not count.
    /// </summary>
    public static async ValueTask<RateLimitResult> RateLimitAsync(
        this IRuvioClient client,
        string key,
        long max,
        TimeSpan window,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long? remaining = await client.LimitAsync(key, max, RespRead.Milliseconds(window), cancellationToken).ConfigureAwait(false);

        return remaining is { } left ? new RateLimitResult(true, left) : new RateLimitResult(false, 0);
    }

    // ---- Cache-aside --------------------------------------------------------------------------------

    /// <summary>
    /// Returns the cached string or builds it with <paramref name="factory"/>, letting one caller across every process
    /// build it while the others wait (<c>FILLGET</c>/<c>FILLSET</c> stampede protection). Waiters poll plain
    /// <c>FILLGET</c> with a 2 to 25 ms backoff instead of <c>FILLGET ... WAIT</c>, because a server-side wait parks
    /// the whole shared connection. A waiter that outlasts <see cref="GetOrCreateOptions.WaitTimeout"/> builds and
    /// stores the value itself. With <see cref="GetOrCreateOptions.StaleWindow"/>, callers keep getting the stale
    /// value while the one caller holding the refresh token rebuilds it.
    /// </summary>
    public static async ValueTask<string> GetOrCreateAsync(
        this IRuvioClient client,
        string key,
        TimeSpan ttl,
        Func<CancellationToken, ValueTask<string>> factory,
        GetOrCreateOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(factory);

        if (ttl <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(ttl), "The cache TTL must be positive.");
        }

        options ??= GetOrCreateOptions.Default;
        long started = Stopwatch.GetTimestamp();
        int delay = 2;
        FillResult fill;

        while (true)
        {
            fill = await client.FillGetAsync(key, options.FillTimeout, wait: null, cancellationToken).ConfigureAwait(false);

            if (fill.State != FillState.Busy || Stopwatch.GetElapsedTime(started) >= options.WaitTimeout)
            {
                break;
            }

            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            delay = Math.Min(delay * 2, MaxBackoffMilliseconds);
        }

        switch (fill.State)
        {
            case FillState.Hit:
            case FillState.Stale when fill.Token is null:
                return Encoding.UTF8.GetString(fill.Value!);
        }

        string value = await factory(cancellationToken).ConfigureAwait(false);

        if (fill.Token is { } token)
        {
            // False means the marker lapsed while the factory ran; the value is still right for this caller.
            _ = await client.FillSetAsync(key, value, ttl, token, options.StaleWindow, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // Waited out the builder: store it plainly so the next caller hits.
            _ = await client.SetAsync(key, value, ttl, SetMode.Always, cancellationToken).ConfigureAwait(false);
        }

        return value;
    }

    /// <summary>
    /// <see cref="GetOrCreateAsync(IRuvioClient, string, TimeSpan, Func{CancellationToken, ValueTask{string}}, GetOrCreateOptions?, CancellationToken)"/>
    /// for a typed value stored as JSON with <paramref name="typeInfo"/> (trimming and AOT safe).
    /// </summary>
    public static async ValueTask<T> GetOrCreateAsync<T>(
        this IRuvioClient client,
        string key,
        TimeSpan ttl,
        Func<CancellationToken, ValueTask<T>> factory,
        JsonTypeInfo<T> typeInfo,
        GetOrCreateOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(typeInfo);
        T? built = default;
        bool ran = false;

        string json = await client.GetOrCreateAsync(
            key,
            ttl,
            async token =>
            {
                built = await factory(token).ConfigureAwait(false);
                ran = true;

                return JsonSerializer.Serialize(built, typeInfo);
            },
            options,
            cancellationToken).ConfigureAwait(false);

        return ran ? built! : JsonSerializer.Deserialize(json, typeInfo)!;
    }

    // ---- Scan iterators ---------------------------------------------------------------------------

    /// <summary>Lazily walks <c>SCAN</c> pages. Keys added or removed during the walk may or may not appear.</summary>
    public static async IAsyncEnumerable<string> ScanKeysAsync(
        this IRuvioClient client,
        string? pattern = null,
        int? count = null,
        string? type = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long cursor = 0;

        do
        {
            ScanPage page = await client.ScanAsync(cursor, pattern, count, type, cancellationToken).ConfigureAwait(false);

            foreach (string key in page.Keys)
            {
                yield return key;
            }

            cursor = page.Cursor;
        }
        while (cursor != 0);
    }

    /// <summary>Lazily walks <c>HSCAN</c> pages of one hash.</summary>
    public static async IAsyncEnumerable<KeyValuePair<string, string>> ScanHashEntriesAsync(
        this IRuvioClient client,
        string key,
        string? pattern = null,
        int? count = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long cursor = 0;

        do
        {
            HashScanPage page = await client.ScanHashAsync(key, cursor, pattern, count, cancellationToken).ConfigureAwait(false);

            foreach (KeyValuePair<string, string> field in page.Fields)
            {
                yield return field;
            }

            cursor = page.Cursor;
        }
        while (cursor != 0);
    }

    /// <summary>Lazily walks <c>SSCAN</c> pages of one set.</summary>
    public static async IAsyncEnumerable<string> ScanSetMembersAsync(
        this IRuvioClient client,
        string key,
        string? pattern = null,
        int? count = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long cursor = 0;

        do
        {
            SetScanPage page = await client.ScanSetAsync(key, cursor, pattern, count, cancellationToken).ConfigureAwait(false);

            foreach (string member in page.Members)
            {
                yield return member;
            }

            cursor = page.Cursor;
        }
        while (cursor != 0);
    }

    /// <summary>Lazily walks <c>ZSCAN</c> pages of one sorted set.</summary>
    public static async IAsyncEnumerable<SortedSetEntry> ScanSortedSetEntriesAsync(
        this IRuvioClient client,
        string key,
        string? pattern = null,
        int? count = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long cursor = 0;

        do
        {
            SortedSetScanPage page = await client.ScanSortedSetAsync(key, cursor, pattern, count, cancellationToken).ConfigureAwait(false);

            foreach (SortedSetEntry entry in page.Entries)
            {
                yield return entry;
            }

            cursor = page.Cursor;
        }
        while (cursor != 0);
    }

    // ---- Recent items, seen-before --------------------------------------------------------------

    /// <summary>Pushes <paramref name="value"/> to the head of a "recent items" list kept at most <paramref name="cap"/> long (<c>LPUSHCAP</c>).</summary>
    public static ValueTask<long> PushRecentAsync(this IRuvioClient client, string key, string value, long cap)
    {
        ArgumentNullException.ThrowIfNull(client);

        return client.PushListLeftCappedAsync(key, cap, value);
    }

    /// <summary>The newest <paramref name="count"/> items of a list written by <see cref="PushRecentAsync"/>, newest first.</summary>
    public static ValueTask<IReadOnlyList<string>> GetRecentAsync(
        this IRuvioClient client,
        string key,
        long count,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        return count <= 0
            ? new ValueTask<IReadOnlyList<string>>(Array.Empty<string>())
            : client.GetListRangeAsync(key, 0, count - 1, cancellationToken);
    }

    /// <summary>
    /// Records <paramref name="items"/> in a Bloom filter (<c>BF.MADD</c>) and returns, per item, whether it was
    /// (probably) seen before. A false answer is certain; a true answer can be a false positive at the filter's error rate.
    /// </summary>
    public static async ValueTask<IReadOnlyList<bool>> SeenBeforeAsync(this IRuvioClient client, string key, params string[] items)
    {
        ArgumentNullException.ThrowIfNull(client);
        IReadOnlyList<bool> added = await client.AddBloomFilterManyAsync(key, items).ConfigureAwait(false);
        var seen = new bool[added.Count];

        for (int index = 0; index < seen.Length; index++)
        {
            seen[index] = !added[index];
        }

        return seen;
    }

    /// <summary>Checks <paramref name="items"/> against a Bloom filter without recording them (<c>BF.MEXISTS</c>).</summary>
    public static ValueTask<IReadOnlyList<bool>> MaybeSeenAsync(this IRuvioClient client, string key, params string[] items)
    {
        ArgumentNullException.ThrowIfNull(client);

        return client.BloomFilterContainsManyAsync(key, items);
    }

    private static string NewToken()
    {
        Span<byte> bytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(bytes);

        return Convert.ToHexString(bytes);
    }
}
