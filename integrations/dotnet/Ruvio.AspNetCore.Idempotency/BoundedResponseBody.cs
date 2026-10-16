using System.IO.Pipelines;
using Microsoft.AspNetCore.Http.Features;

namespace Ruvio.AspNetCore.Idempotency;

// A fixed-capacity buffer also bounds BodyWriter.GetMemory; overflow is sticky even if a handler catches it.
internal sealed class BoundedBuffer(int limit) : Stream
{
    private readonly byte[] bytes = new byte[limit];
    private int length;

    public bool Failed { get; private set; }
    public ReadOnlyMemory<byte> Written => bytes.AsMemory(0, length);
    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => length;

    public override long Position { get => length; set => throw new NotSupportedException(); }

    internal Memory<byte> Reserve(int sizeHint)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(sizeHint);

        Check(Math.Max(1, sizeHint));

        return bytes.AsMemory(length);
    }

    internal void Advance(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);

        Check(count);
        length += count;
    }

    private void Check(int count)
    {
        if (Failed || count > bytes.Length - length)
        {
            Failed = true;
            throw new IOException("The idempotency response body limit was exceeded.");
        }
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        Check(buffer.Length);
        buffer.CopyTo(bytes.AsSpan(length));
        length += buffer.Length;
    }

    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));
    public override void WriteByte(byte value)
    {
        Check(1);
        bytes[length++] = value;
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Write(buffer.Span);

        return ValueTask.CompletedTask;
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    public override void Flush() { }
    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return Task.CompletedTask;
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}

internal sealed class BoundedResponseBody(int limit) : IHttpResponseBodyFeature
{
    internal BoundedBuffer Buffer { get; } = new(limit);

    private PipeWriter? writer;
    public Stream Stream => Buffer;
    public PipeWriter Writer => writer ??= new BufferWriter(Buffer);

    public void DisableBuffering() => throw new NotSupportedException("Streaming is not supported on idempotent endpoints.");
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return Task.CompletedTask;
    }

    public Task CompleteAsync() => Task.CompletedTask;

    public async Task SendFileAsync(string path, long offset, long? count, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);

        if (offset < 0 || offset > file.Length || count < 0) 
            throw new ArgumentOutOfRangeException(nameof(offset));
        
        var remaining = count ?? file.Length - offset;

        // Reject the entire operation before reading or writing any of it.

        if (remaining > limit - Buffer.Length)
            Buffer.Reserve(limit + 1);

        file.Position = offset;

        var scratch = new byte[4096];

        while (remaining > 0)
        {
            int read = await file.ReadAsync(scratch.AsMemory(0, (int)Math.Min(remaining, scratch.Length)), cancellationToken);

            if (read == 0) 
                throw new EndOfStreamException();
            
            await Buffer.WriteAsync(scratch.AsMemory(0, read), cancellationToken);
            remaining -= read;
        }
    }

    private sealed class BufferWriter(BoundedBuffer buffer) : PipeWriter
    {
        private bool canceled;

        public override void Advance(int bytes) => buffer.Advance(bytes);
        public override Memory<byte> GetMemory(int sizeHint = 0) => buffer.Reserve(sizeHint);
        public override Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;
        public override void CancelPendingFlush() => canceled = true;
        public override void Complete(Exception? exception = null)
        {
            if (exception is not null)
                throw new IOException("Response writer failed.", exception);
        }

        public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var result = new FlushResult(canceled, false);

            canceled = false;

            return ValueTask.FromResult(result);
        }
    }
}
