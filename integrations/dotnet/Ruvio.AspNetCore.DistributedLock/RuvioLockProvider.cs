using System.Globalization;
using Ruvio.Client;

namespace Ruvio.AspNetCore.DistributedLock;

/// <summary>
/// Acquires distributed locks on one Ruvio client. A lock is a key holding a random owner token:
/// <c>SET key token NX PX lease</c> takes it, <c>SET key token IFEQ token PX lease</c> renews it and
/// <c>DELEX key IFEQ token</c> releases it, each in one command and only for the owner. The provider
/// neither owns nor disposes the client.
/// </summary>
/// <remarks>
/// Needs a Ruvio server with <c>SET … IFEQ</c> and <c>DELEX</c>. The lock lives on one server, so a
/// failover that loses the key can let two owners overlap; guard writes that must never overlap with
/// the resource's own versioning as well.
/// </remarks>
public sealed class RuvioLockProvider
{
    private readonly IRuvioClient client;
    private readonly RuvioLockOptions options;

    internal RuvioLockOptions Options => options;

    /// <summary>Creates a provider.</summary>
    public RuvioLockProvider(IRuvioClient client, RuvioLockOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        this.client = client;
        this.options = options ?? new RuvioLockOptions();
        this.options.Validate();
    }

    /// <summary>
    /// Takes the lock for <paramref name="lease"/>, retrying for up to <paramref name="wait"/>.
    /// Returns null when it stayed taken. A zero wait tries exactly once.
    /// </summary>
    public async ValueTask<IRuvioLock?> TryAcquireAsync(
        string resource,
        TimeSpan lease,
        TimeSpan wait = default,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(resource);
        string leaseText = LeaseMilliseconds(lease).ToString(CultureInfo.InvariantCulture);
        string key = options.KeyPrefix + resource;
        // Random per acquisition: IFEQ and DELEX match on it, so a holder whose lease expired cannot
        // renew or release a lock that another owner now holds.
        string token = Guid.NewGuid().ToString("N");
        IReadOnlyList<string> command = ["SET", key, token, "NX", "PX", leaseText];
        long start = options.TimeProvider.GetTimestamp();
        TimeSpan pause = options.RetryMin;

        while (true)
        {
            RespValue reply = await client.ExecuteAsync(command, cancellationToken).ConfigureAwait(false);

            Check(reply);

            if (reply.Kind != RespKind.Null)
            {
                return new RuvioLock(client, options, resource, key, token, lease);
            }

            TimeSpan remaining = wait - options.TimeProvider.GetElapsedTime(start);

            if (remaining <= TimeSpan.Zero)
            {
                return null;
            }

            double jitter = 0.5 + Random.Shared.NextDouble();
            TimeSpan delay = TimeSpan.FromTicks((long)(pause.Ticks * jitter));

            await Task.Delay(delay < remaining ? delay : remaining, options.TimeProvider, cancellationToken).ConfigureAwait(false);
            pause = pause + pause > options.RetryMax ? options.RetryMax : pause + pause;
        }
    }

    /// <summary>Like <see cref="TryAcquireAsync"/>, but throws <see cref="TimeoutException"/> when the lock stayed taken.</summary>
    public async ValueTask<IRuvioLock> AcquireAsync(
        string resource,
        TimeSpan lease,
        TimeSpan wait,
        CancellationToken cancellationToken = default)
    {
        return await TryAcquireAsync(resource, lease, wait, cancellationToken).ConfigureAwait(false)
            ?? throw new TimeoutException($"The lock '{resource}' stayed taken for {wait}.");
    }

    internal static long LeaseMilliseconds(TimeSpan lease)
    {
        long milliseconds = (long)lease.TotalMilliseconds;

        if (milliseconds < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(lease), "The lease must be at least one millisecond.");
        }

        return milliseconds;
    }

    internal static void Check(RespValue reply)
    {
        if (reply.Kind == RespKind.Error)
        {
            throw new RuvioException(reply.Text ?? "The server rejected the lock command.", isServerReply: true);
        }
    }
}
