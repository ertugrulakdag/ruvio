using Microsoft.AspNetCore.Http;

namespace Ruvio.AspNetCore.Idempotency;

internal static class RequestBuffer
{
    internal static async Task<(byte[] Bytes, int Length, int? RejectionStatus)> ReadAsync(
        Stream input, long? contentLength, int limit, CancellationToken cancellation)
    {
        if (contentLength < 0)
            return ([], 0, StatusCodes.Status400BadRequest);

        if (contentLength > limit)
            return ([], 0, StatusCodes.Status413PayloadTooLarge);

        int capacity = contentLength is long known ? (int)known : Math.Min(4096, limit);
        byte[] bytes = capacity == 0 ? [] : new byte[capacity];
        byte[]? probe = null;
        int length = 0;

        while (true)
        {
            if (length == bytes.Length)
            {
                probe ??= new byte[1];

                if (await input.ReadAsync(probe, cancellation) == 0)
                    break;

                if (length == limit)
                    return (bytes, length, StatusCodes.Status413PayloadTooLarge);

                if (contentLength.HasValue)
                    return (bytes, length, StatusCodes.Status400BadRequest);

                Array.Resize(ref bytes, Math.Min(limit, Math.Max(1, bytes.Length * 2)));
                bytes[length++] = probe[0];

                continue;
            }

            int read = await input.ReadAsync(bytes.AsMemory(length), cancellation);

            if (read == 0) break;
            length += read;
        }

        if (contentLength.HasValue && length != contentLength.Value)
            return (bytes, length, StatusCodes.Status400BadRequest);

        return (bytes, length, null);
    }
}
