using Microsoft.AspNetCore.Builder;

namespace Ruvio.AspNetCore.DistributedLock;

/// <summary>Pipeline registration.</summary>
public static class RuvioLockApplicationBuilderExtensions
{
    /// <summary>
    /// Adds the middleware that honours <see cref="RuvioLockAttribute"/>. Call it after
    /// <c>UseRouting</c> and <c>UseAuthentication</c>, and register the services with
    /// <c>AddRuvioDistributedLock</c> and a Ruvio client first.
    /// </summary>
    public static IApplicationBuilder UseRuvioLock(this IApplicationBuilder app)
    {
        ArgumentNullException.ThrowIfNull(app);

        return app.UseMiddleware<RuvioLockMiddleware>();
    }
}
