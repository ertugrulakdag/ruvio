using Microsoft.AspNetCore.Http;

namespace Ruvio.AspNetCore.DistributedLock;

/// <summary>
/// Makes an endpoint run under a distributed lock: at most one request per resource, across all of
/// your application instances, executes at a time.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why use it.</b> Some endpoints must never run twice at once for the same thing: charging an
/// order, generating an invoice number, importing a file, rebuilding a report, closing a day. An
/// in-process <c>lock</c> or <c>SemaphoreSlim</c> only protects one instance; as soon as you run two
/// pods behind a load balancer, or a user double-clicks and the second click lands on another pod,
/// both requests run. This attribute moves that guard into Ruvio, so it holds for every instance,
/// and it removes the boilerplate (acquire, try/finally release, renew, map failure to HTTP).
/// </para>
/// <para>
/// <b>What happens.</b> Before your endpoint runs, the middleware takes the lock named by
/// <see cref="Resource"/>. If another request holds it, the middleware waits up to
/// <see cref="WaitMilliseconds"/> (default 0: do not wait) and then answers
/// <see cref="RejectStatusCode"/> (default 409 Conflict) with a <c>Retry-After</c> hint, without
/// running your code. If it gets the lock, your endpoint runs; the lease is renewed automatically
/// while the request is in flight, and the lock is released when the response finishes, even when the
/// endpoint throws.
/// </para>
/// <para>
/// <b>Safety net.</b> While the endpoint runs, <c>HttpContext.RequestAborted</c> is replaced by a token
/// that is also cancelled when the lock is lost (for example the Ruvio server was unreachable for
/// longer than the lease, so another instance may now hold the lock). Anything you pass
/// <c>RequestAborted</c> to (EF Core, <c>HttpClient</c>, your own code) stops instead of running
/// alongside the new owner. It still fires when the client disconnects, as before.
/// </para>
/// <para>
/// Use it as metadata on a minimal API (<c>.WithMetadata(new RuvioLockAttribute("import"))</c>) or as
/// an attribute on a controller or action. Add <c>app.UseRuvioLock()</c> after routing and
/// authentication so the endpoint and the user are known. Endpoints without the attribute pay
/// nothing.
/// </para>
/// </remarks>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class RuvioLockAttribute : Attribute
{
    /// <summary>
    /// Creates the metadata.
    /// </summary>
    /// <param name="resource">
    /// The lock name. A fixed name such as <c>"nightly-import"</c> serialises the whole endpoint. A
    /// template such as <c>"order:{id}"</c> or <c>"wallet:{user}"</c> serialises per value: each
    /// <c>{name}</c> is replaced by the route value, then the query value, of that name, and
    /// <c>{user}</c> by the authenticated user name. A placeholder that cannot be resolved answers
    /// 400 Bad Request instead of silently locking the wrong thing.
    /// </param>
    /// <param name="leaseSeconds">
    /// How long the lock lives without renewal. If your process dies the lock frees itself after this
    /// time; while it is alive the lease is renewed automatically. Default 30 seconds.
    /// </param>
    public RuvioLockAttribute(string resource, int leaseSeconds = 30)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resource);
        ArgumentOutOfRangeException.ThrowIfLessThan(leaseSeconds, 1);
        Resource = resource;
        LeaseSeconds = leaseSeconds;
    }

    /// <summary>The lock name or template, without the key prefix.</summary>
    public string Resource { get; }

    /// <summary>The lease in seconds; renewed automatically while the request runs.</summary>
    public int LeaseSeconds { get; }

    /// <summary>
    /// How long to wait for a taken lock before rejecting. 0 (default) answers immediately, which is
    /// right for double-submit protection; use a few seconds to queue short critical sections.
    /// </summary>
    public int WaitMilliseconds { get; init; }

    /// <summary>Status code when the lock stays taken. Default 409 Conflict; use 429 if clients should back off.</summary>
    public int RejectStatusCode { get; init; } = StatusCodes.Status409Conflict;
}
