using System.Reflection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class InterfaceTests
{
    [Fact]
    public void ConnectionOwnsTheLifecycleAndClientExposesCommands()
    {
        Assert.Contains(typeof(IRuvioClient), typeof(RuvioClient).GetInterfaces());
        Assert.DoesNotContain(typeof(IRuvioConnection), typeof(RuvioClient).GetInterfaces());
        Assert.Contains(typeof(IRuvioConnection), typeof(RuvioConnection).GetInterfaces());
    }

    [Fact]
    public void EveryPublicCommandIsOnIRuvioClientAndLifecycleIsOnIRuvioConnection()
    {
        const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly;
        List<string> missing = [];

        foreach (MethodInfo method in typeof(RuvioClient).GetMethods(flags))
        {
            if (method.IsSpecialName || method.Name.Contains('.'))
            {
                continue;
            }

            MethodInfo? match = Find(typeof(IRuvioClient), method);

            if (match is null)
            {
                missing.Add(method.Name);
            }
        }

        foreach (PropertyInfo property in typeof(RuvioClient).GetProperties(flags))
        {
            if (typeof(IRuvioClient).GetProperty(property.Name) is null)
            {
                missing.Add(property.Name);
            }
        }

        foreach (EventInfo evt in typeof(RuvioClient).GetEvents(flags))
        {
            if (typeof(IRuvioClient).GetEvent(evt.Name) is null)
            {
                missing.Add(evt.Name);
            }
        }

        Assert.True(missing.Count == 0, "Missing from the interfaces: " + string.Join(", ", missing));
    }

    [Fact]
    public async Task ACommandSubstituteAnswersWithoutASocket()
    {
        IRuvioClient db = DispatchProxy.Create<IRuvioClient, CannedClient>();
        var stock = new StockReader(db);

        Assert.Equal("4", await stock.ReadAsync());
    }

    private static MethodInfo? Find(Type surface, MethodInfo method)
    {
        foreach (MethodInfo candidate in surface.GetMethods().Concat(surface.GetInterfaces().SelectMany(parent => parent.GetMethods())))
        {
            if (candidate.Name != method.Name || candidate.IsGenericMethod != method.IsGenericMethod)
            {
                continue;
            }

            if (Signature(candidate) == Signature(method))
            {
                return candidate;
            }
        }

        return null;
    }

    private static string Signature(MethodInfo method)
    {
        string parameters = string.Join(
            ",",
            method.GetParameters().Select(parameter => parameter.ParameterType.ToString()));
        string returned = method.ReturnType.ToString();

        return returned + "|" + parameters;
    }

    private sealed class StockReader(IRuvioClient db)
    {
        public ValueTask<string?> ReadAsync()
        {
            return db.GetStringAsync("stock");
        }
    }

    private class CannedClient : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod?.Name == nameof(IRuvioClient.GetStringAsync))
            {
                return ValueTask.FromResult<string?>("4");
            }

            throw new NotSupportedException(targetMethod?.Name);
        }
    }
}
