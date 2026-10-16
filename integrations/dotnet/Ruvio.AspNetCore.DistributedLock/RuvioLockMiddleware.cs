using System.Diagnostics;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Ruvio.AspNetCore.DistributedLock;

/// <summary>
/// Runs endpoints marked with <see cref="RuvioLockAttribute"/> under a Ruvio distributed lock. See the
/// attribute for the behaviour; place it after routing and authentication.
/// </summary>
public sealed class RuvioLockMiddleware(RequestDelegate next)
{
    private readonly RequestDelegate next = next ?? throw new ArgumentNullException(nameof(next));

    /// <summary>Handles one request.</summary>
    public Task InvokeAsync(HttpContext context)
    {
        // Endpoints without the attribute return the downstream task itself: no state machine, no
        // allocation, so enabling the middleware costs nothing for them.
        RuvioLockAttribute? metadata = context.GetEndpoint()?.Metadata.GetMetadata<RuvioLockAttribute>();

        return metadata is null ? next(context) : InvokeLockedAsync(context, metadata);
    }

    private async Task InvokeLockedAsync(HttpContext context, RuvioLockAttribute metadata)
    {
        string? resource = ResolveResource(context, metadata.Resource);

        if (resource is null)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;

            return;
        }

        var provider = context.RequestServices.GetService<RuvioLockProvider>()!;
        // A templated name is never a metric tag: one tag value per order id would explode cardinality.
        bool fixedName = metadata.Resource.IndexOf('{') < 0;
        var tags = new TagList { { "resource", fixedName ? resource : "(templated)" } };
        using Activity? activity = RuvioLockTelemetry.Source.StartActivity("ruvio.lock");
        long start = Stopwatch.GetTimestamp();
        IRuvioLock? held;

        try
        {
            held = await provider.TryAcquireAsync(
                resource,
                TimeSpan.FromSeconds(metadata.LeaseSeconds),
                TimeSpan.FromMilliseconds(metadata.WaitMilliseconds),
                context.RequestAborted).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            return; // The client left while we waited; nothing to answer.
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // Fail closed: when we cannot tell whether someone else holds the lock, running could break
            // the exclusion the endpoint asked for.
            RuvioLockTelemetry.Errors.Add(1, tags);
            activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;

            return;
        }

        RuvioLockTelemetry.WaitDuration.Record(Stopwatch.GetElapsedTime(start).TotalMilliseconds, tags);

        if (held is null)
        {
            RuvioLockTelemetry.Rejected.Add(1, tags);
            activity?.SetTag("ruvio.lock.outcome", "rejected");
            context.Response.StatusCode = metadata.RejectStatusCode;
            // Tell well-behaved clients when it is worth trying again; the lease is the longest it can last.
            context.Response.Headers.RetryAfter = Math.Max(1, metadata.LeaseSeconds).ToString(System.Globalization.CultureInfo.InvariantCulture);

            return;
        }

        RuvioLockTelemetry.Acquired.Add(1, tags);
        activity?.SetTag("ruvio.lock.outcome", "acquired");
        CancellationToken original = context.RequestAborted;

        // One token that fires when the client disconnects OR the lock is lost. Downstream code that
        // already honours RequestAborted (EF Core, HttpClient, minimal-API handlers) is protected
        // without knowing about locks.
        using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(original, held.LockLost);

        context.RequestAborted = linked.Token;

        try
        {
            await next(context).ConfigureAwait(false);
        }
        finally
        {
            context.RequestAborted = original;

            if (held.LockLost.IsCancellationRequested)
            {
                RuvioLockTelemetry.Lost.Add(1, tags);
                activity?.SetTag("ruvio.lock.outcome", "lost");
            }

            try
            {
                // Do not use the request token here: a disconnected client must still free the lock.
                await held.ReleaseAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                // The lease frees the lock anyway; a failed release must not turn a finished request into an error.
                RuvioLockTelemetry.Errors.Add(1, tags);
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
            }

            await held.DisposeAsync().ConfigureAwait(false);
        }
    }

    // Fixed names are returned as they are. Templates replace {name} by route value, query value, or
    // the user name for {user}; any placeholder left unresolved returns null (400).
    private static string? ResolveResource(HttpContext context, string template)
    {
        // span uses SIMD for some functions.
        var templateSpan = template.AsSpan();

        if (templateSpan.IndexOf('{') < 0)
        {
            return template;
        }

        var builder = new StringBuilder(templateSpan.Length + 16);
        int index = 0;

        while (index < templateSpan.Length)
        {
            int open = templateSpan[index..].IndexOf('{');
            if (open >= 0)
                open += index;

            if (open < 0)
            {
                builder.Append(template, index, templateSpan.Length - index);
                break;
            }

            int close = template.IndexOf('}', open + 1);

            if (close < 0)
            {
                return null;
            }

            builder.Append(template, index, open - index);
            
            string name = template.Substring(open + 1, close - open - 1);
            string? value = name == "user"
                ? context.User.Identity?.Name
                : context.Request.RouteValues.TryGetValue(name, out object? route) ? route?.ToString() : context.Request.Query[name].ToString();

            if (string.IsNullOrEmpty(value))
            {
                return null;
            }

            builder.Append(value);
            index = close + 1;
        }

        return builder.ToString();
    }
}
