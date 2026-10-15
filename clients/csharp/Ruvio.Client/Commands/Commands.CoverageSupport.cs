using System.Runtime.CompilerServices;

namespace Ruvio.Client;

public sealed partial class RuvioClient
{
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<ScanPage> ScanPageOf(ValueTask<RespValue> pending) =>
        ParseScan(await pending.ConfigureAwait(false));

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<IReadOnlyList<RespValue>> ItemsOf(ValueTask<RespValue> pending) =>
        (await pending.ConfigureAwait(false)).Items ?? [];
}
