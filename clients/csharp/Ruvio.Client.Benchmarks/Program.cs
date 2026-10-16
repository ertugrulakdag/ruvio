using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;

if (args.Length > 0 && args[0] == "--paired-socket-load")
{
    await PairedSocketLoad.RunAsync(args[1..]);

    return;
}

if (args.Length > 0 && args[0] == "--allocation-profile")
{
    await AllocationProfile.RunAsync(args[1..]);

    return;
}

if (args.Length > 0 && args[0] == "--socket-load")
{
    await SocketLoad.RunAsync(args[1..]);

    return;
}

if (args.Length > 0 && args[0] == "--cluster-cost")
{
    await ClusterCost.RunAsync(args[1..]);

    return;
}

if (args.Length > 0 && args[0] == "--quick")
{
    await QuickBench.RunAsync(args[1..]);

    return;
}

if (args.Length > 0 && args[0] == "--socket-phases")
{
    await SocketPhases.RunAsync(args[1..]);

    return;
}

IConfig config = DefaultConfig.Instance.WithArtifactsPath(
    Path.Combine(AppContext.BaseDirectory, "BenchmarkDotNet.Artifacts"));

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
