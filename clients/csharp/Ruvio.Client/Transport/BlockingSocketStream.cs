using System.Net.Sockets;

namespace Ruvio.Client;

/// <summary>
/// A stream over a socket that only ever uses blocking calls, so the socket stays in blocking mode
/// (one async call would switch it to non-blocking for good, after which blocking calls are emulated
/// through the async engine). The async members run synchronously and return completed tasks; they
/// exist so the shared reader and writer code can use either stream.
/// </summary>
internal sealed class BlockingSocketStream : Stream
{
    private readonly Socket _socket;

    internal BlockingSocketStream(Socket socket)
    {
        _socket = socket;
    }

    public override bool CanRead => true;

    public override bool CanWrite => true;

    public override bool CanSeek => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        try
        {
            return _socket.Receive(buffer, SocketFlags.None);
        }
        catch (SocketException error)
        {
            throw new IOException(error.Message, error);
        }
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        new(Read(buffer.Span));

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        Task.FromResult(Read(buffer.AsSpan(offset, count)));

    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        try
        {
            while (!buffer.IsEmpty)
            {
                buffer = buffer[_socket.Send(buffer, SocketFlags.None)..];
            }
        }
        catch (SocketException error)
        {
            throw new IOException(error.Message, error);
        }
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        Write(buffer.Span);

        return ValueTask.CompletedTask;
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        Write(buffer.AsSpan(offset, count));

        return Task.CompletedTask;
    }

    public override void Flush()
    {
    }

    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            // Wakes a reader blocked in Receive.
            try
            {
                _socket.Shutdown(SocketShutdown.Both);
            }
            catch (Exception)
            {
                // Already closed or never connected.
            }

            _socket.Dispose();
        }

        base.Dispose(disposing);
    }
}
