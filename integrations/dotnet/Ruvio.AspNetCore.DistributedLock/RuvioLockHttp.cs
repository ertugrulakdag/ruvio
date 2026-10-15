using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Ruvio.AspNetCore.DistributedLock;

/// <summary>
/// The shared core behind <see cref="RuvioLockEndpointExtensions.WithDistributedLock{TBuilder}(TBuilder, string, TimeSpan?, TimeSpan?, int?)"/>
/// and <see cref="RuvioLockAttribute"/>: take the lock for the request, tell the caller when it stayed taken,
/// release it when the request ends, however it ends.
/// </summary>
internal static class RuvioLockHttp
{
    private const string ItemKey = "Ruvio.AspNetCore.DistributedLock.Lock";

    internal static async ValueTask<IRuvioLock?> TryTakeAsync(
        HttpContext context, string resource, TimeSpan? lease, TimeSpan? wait)
    {
        var provider = context.RequestServices.GetRequiredService<RuvioLockProvider>();
        RuvioLockOptions options = provider.Options;
        
        IRuvioLock? held = await provider.TryAcquireAsync(
            resource, lease ?? options.DefaultLease, wait ?? options.DefaultWait, context.RequestAborted)
            .ConfigureAwait(false);

        if (held is not null)
        {
            context.Items[ItemKey] = held;
        }

        return held;
    }

    internal static IRuvioLock? Get(HttpContext context) =>
        context.Items.TryGetValue(ItemKey, out object? held) ? held as IRuvioLock : null;

    internal static int ConflictStatus(HttpContext context, int? statusCode) =>
        statusCode ?? context.RequestServices.GetRequiredService<RuvioLockProvider>().Options.ConflictStatusCode;

    // The resource name is deliberately not echoed back: it can contain ids the caller should not learn.
    internal const string ConflictTitle = "The resource is busy.";

    internal const string ConflictDetail = "Another request holds the lock for this operation. Try again shortly.";

    internal static IResult Conflict(HttpContext context, int? statusCode) =>
        Results.Problem(statusCode: ConflictStatus(context, statusCode), title: ConflictTitle, detail: ConflictDetail);

    /// <summary>
    /// Turns <c>"pay:{orderId}"</c> into <c>"pay:42"</c> from the route values. Parsed once when the endpoint is built,
    /// so a request only concatenates strings.
    /// </summary>
    internal sealed class Template
    {
        private readonly (string Literal, string? Name)[] parts;

        internal Template(string template)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(template);
            var list = new List<(string, string?)>();
            int position = 0;

            while (position < template.Length)
            {
                int open = template.IndexOf('{', position);

                if (open < 0)
                {
                    list.Add((template[position..], null));
                    break;
                }

                int close = template.IndexOf('}', open);

                if (close < 0 || close == open + 1)
                {
                    throw new ArgumentException($"The lock resource template '{template}' has an unmatched or empty brace.", nameof(template));
                }

                list.Add((template[position..open], template[(open + 1)..close]));
                position = close + 1;
            }

            parts = [.. list];
        }

        internal string Resolve(HttpContext context)
        {
            if (parts.Length == 1 && parts[0].Name is null)
            {
                return parts[0].Literal;
            }

            var builder = new System.Text.StringBuilder();

            foreach ((string literal, string? name) in parts)
            {
                builder.Append(literal);

                if (name is null)
                {
                    continue;
                }

                // A missing value is a configuration mistake (wrong placeholder), not a client error: fail loudly
                // rather than silently locking one shared name for every caller.
                object? value = context.Request.RouteValues.TryGetValue(name, out object? route) ? route : null;

                builder.Append(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)
                    is { Length: > 0 } text ? text : throw new InvalidOperationException(
                        $"The lock resource placeholder '{{{name}}}' has no route value on this request."));
            }

            return builder.ToString();
        }
    }
}
