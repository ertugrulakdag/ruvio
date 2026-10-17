using System.Reflection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>Drives every default interface member of <see cref="IRuvioClient"/> through an implementer that only has ExecuteAsync.</summary>
public class InterfaceDefaultsTests
{
    private static object? MakeArgument(Type type)
    {
        Type? inner = Nullable.GetUnderlyingType(type);

        if (inner is not null)
        {
            return MakeArgument(inner);
        }

        if (type == typeof(string))
        {
            return "a";
        }

        if (type == typeof(bool))
        {
            return true;
        }

        if (type == typeof(double))
        {
            return 1.5;
        }

        if (type == typeof(long))
        {
            return 1L;
        }

        if (type == typeof(int))
        {
            return 1;
        }

        if (type == typeof(TimeSpan))
        {
            return TimeSpan.FromSeconds(2);
        }

        if (type == typeof(DateTimeOffset))
        {
            return DateTimeOffset.UtcNow.AddMinutes(5);
        }

        if (type == typeof(byte[]))
        {
            return new byte[] { 1, 2 };
        }

        if (type == typeof(BitFieldStep))
        {
            return BitFieldStep.Get("u8", 0);
        }

        if (type == typeof(StreamTrim))
        {
            return StreamTrim.MaxLen(5);
        }

        if (type.IsEnum)
        {
            return Enum.GetValues(type).GetValue(0);
        }

        if (type.IsArray)
        {
            Type element = type.GetElementType()!;
            Array array = Array.CreateInstance(element, 2);

            for (int i = 0; i < 2; i++)
            {
                array.SetValue(MakeArgument(element), i);
            }

            return array;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() is { } definition
            && (definition == typeof(IReadOnlyList<>) || definition == typeof(IEnumerable<>) || definition == typeof(IReadOnlyCollection<>)))
        {
            return MakeArgument(type.GetGenericArguments()[0].MakeArrayType());
        }

        if (type.IsGenericType && type.FullName!.StartsWith("System.ValueTuple", StringComparison.Ordinal))
        {
            return Activator.CreateInstance(type, type.GetGenericArguments().Select(MakeArgument).ToArray());
        }

        if (type == typeof(CancellationToken))
        {
            return CancellationToken.None;
        }

        return type.IsValueType ? Activator.CreateInstance(type) : null;
    }

    private static async Task Await(object? result)
    {
        if (result is null)
        {
            return;
        }

        MethodInfo? asTask = result.GetType().GetMethod("AsTask", Type.EmptyTypes);

        if (asTask is not null)
        {
            await (Task)asTask.Invoke(result, null)!;
        }
    }

    [Fact(Timeout = 60000)]
    public async Task EveryDefaultMemberSendsThroughExecuteAsyncForAnyReplyShape()
    {
        IRuvioClient db = DispatchProxy.Create<IRuvioClient, CommandSurfaceTests.RecordingClient>();
        var recorder = (CommandSurfaceTests.RecordingClient)(object)db;
        RespValue[] replies =
        [
            RespValue.Null(),
            RespValue.FromInteger(1),
            RespValue.Simple("OK"),
            RespValue.Bulk("1"u8.ToArray()),
            RespValue.Array([]),
            RespValue.Array([RespValue.Bulk("a"u8.ToArray()), RespValue.Bulk("1"u8.ToArray())]),
            RespValue.Array([RespValue.Bulk("a"u8.ToArray()), RespValue.Array([RespValue.Bulk("b"u8.ToArray())])]),
            RespValue.Array([RespValue.FromInteger(1), RespValue.FromInteger(2)]),
            RespValue.Error("ERR nope"),
        ];
        MethodInfo[] methods = [.. typeof(IRuvioClient).GetMethods()
            .Where(method => !method.IsAbstract && !method.IsGenericMethodDefinition && !method.IsSpecialName)];
        var neverSent = new List<string>();
        int invoked = 0;

        Assert.True(methods.Length > 50);

        foreach (MethodInfo method in methods)
        {
            object?[] arguments = [.. method.GetParameters().Select(p => MakeArgument(p.ParameterType))];
            bool sent = false;

            foreach (RespValue reply in replies)
            {
                recorder.Reply = reply;
                recorder.Clear();

                try
                {
                    await Await(method.Invoke(db, arguments));
                }
                catch (Exception)
                {
                    // Wrong reply shapes, rejected arguments and unsupported members are all expected here.
                }

                invoked++;
                sent |= recorder.Last.Count > 0;
            }

            if (!sent)
            {
                neverSent.Add(method.Name);
            }
        }

        Assert.True(invoked >= methods.Length * replies.Length);

        // Members that build on other abstract members (subscriptions) or throw by design never reach ExecuteAsync.
        Assert.All(neverSent, name => Assert.True(
            name.StartsWith("Subscribe", StringComparison.Ordinal) || name.Contains("Binary", StringComparison.Ordinal) || name.StartsWith("Listen", StringComparison.Ordinal) || name.StartsWith("Json", StringComparison.Ordinal) || name.StartsWith("Pattern", StringComparison.Ordinal),
            $"{name} never reached ExecuteAsync"));
    }

    [Fact]
    public async Task TheDefaultBinaryMemberRefusesWhenTheImplementerHasNoBinarySupport()
    {
        IRuvioClient db = DispatchProxy.Create<IRuvioClient, CommandSurfaceTests.RecordingClient>();

        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await db.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[] { "GET"u8.ToArray() }));
    }
}
