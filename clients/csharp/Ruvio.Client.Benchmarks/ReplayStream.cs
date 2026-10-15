namespace Ruvio.Client.Benchmarks;

/// <summary>Returns the same payload from the start after <see cref="Rewind"/>.</summary>
internal sealed class ReplayStream : Stream
{
    private readonly byte[] _payload;
    private int _offset;

    internal ReplayStream(byte[] payload)
    {
        _payload = payload;
    }

    internal void Rewind()
    {
        _offset = 0;
    }

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => _payload.Length;

    public override long Position
    {
        get => _offset;
        set => throw new NotSupportedException();
    }

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return CopyPayload(buffer.AsSpan(offset, count));
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return new ValueTask<int>(CopyPayload(buffer.Span));
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    private int CopyPayload(Span<byte> buffer)
    {
        if (_offset >= _payload.Length)
        {
            return 0;
        }

        int count = Math.Min(buffer.Length, _payload.Length - _offset);

        _payload.AsSpan(_offset, count).CopyTo(buffer);
        _offset += count;

        return count;
    }
}
