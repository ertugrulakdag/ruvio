using System.Threading.Channels;

namespace Ruvio.Client;

/// <summary>One dedicated Pub/Sub TCP socket and its inbound pump.</summary>
internal sealed class PubSubLink : IDisposable
{
    private readonly Channel<RespValue> _control = Channel.CreateUnbounded<RespValue>();
    private readonly Channel<PubSubMessage> _messages = Channel.CreateBounded<PubSubMessage>(
        new BoundedChannelOptions(1024)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = false,
            SingleWriter = true,
        });

    private readonly CancellationTokenSource _pump = new();
    private readonly Task _running;

    public PubSubLink(RuvioSocket connection, Action<PubSubMessage> onMessage, Action<Exception>? onFailed)
    {
        Connection = connection;
        connection.EnableIndependentIo();
        _running = Task.Run(() => RunAsync(onMessage, onFailed));
    }

    public RuvioSocket Connection { get; }

    public ChannelReader<PubSubMessage> Messages => _messages.Reader;

    public async Task<IReadOnlyList<RespValue>> SendAsync(
        IReadOnlyList<string> arguments,
        int replies,
        CancellationToken cancellationToken)
    {
        using (PooledFrame frame = RespCodec.EncodePooled(arguments))
        {
            await Connection.WriteAsync(frame.Buffer.AsMemory(0, frame.Length), cancellationToken);
        }

        var values = new RespValue[replies];

        for (int index = 0; index < replies; index++)
        {
            values[index] = await _control.Reader.ReadAsync(cancellationToken);

            if (values[index].Kind is RespKind.Error)
            {
                throw new RuvioException(values[index].Text ?? "ERR", isServerReply: true);
            }
        }

        return values;
    }

    public void Dispose()
    {
        _pump.Cancel();

        try
        {
            _running.Wait(TimeSpan.FromSeconds(2));
        }

        catch (AggregateException)
        {
        }

        Connection.Dispose();
        _pump.Dispose();
    }

    private async Task RunAsync(Action<PubSubMessage> onMessage, Action<Exception>? onFailed)
    {
        try
        {
            while (!_pump.IsCancellationRequested)
            {
                RespValue reply = await Connection.ReadAsync(_pump.Token);

                if (reply.Kind is RespKind.Error)
                {
                    await _control.Writer.WriteAsync(reply, _pump.Token);
                    continue;
                }

                if (PubSubMessage.TryParse(reply, out PubSubMessage message))
                {
                    onMessage(message);
                    _messages.Writer.TryWrite(message);

                    continue;
                }

                await _control.Writer.WriteAsync(reply, _pump.Token);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception exception)
        {
            _messages.Writer.TryComplete(exception);
            onFailed?.Invoke(exception);
        }
    }
}
