using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Xunit;

namespace Ruvio.AspNetCore.Idempotency.Tests;

public sealed class HashFormatTests
{
    private static string Reference(IEnumerable<string> parts, byte[] body)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var size = new byte[4];

        foreach (string part in parts)
        {
            byte[] encoded = Encoding.UTF8.GetBytes(part);
            BinaryPrimitives.WriteInt32BigEndian(size, encoded.Length);
            hash.AppendData(size);
            hash.AppendData(encoded);
        }

        BinaryPrimitives.WriteInt32BigEndian(size, body.Length);
        hash.AppendData(size);
        hash.AppendData(body);

        return Convert.ToHexString(hash.GetHashAndReset());
    }

    public static TheoryData<string> Parts => new() { "orders", "sipariş-🚀", new string('ç', 4000) };

    [Theory]
    [MemberData(nameof(Parts))]
    public void StorageKeyAndFingerprintKeepTheDocumentedFormat(string part)
    {
        string[] identity = ["ns", "authenticated", "Bearer", "issuer", part, "scope"];
        string[] request = ["POST", "", "/orders", "?a=1", "application/json"];
        byte[] body = Encoding.UTF8.GetBytes("{\"x\":" + part.Length + "}");

        Assert.Equal("ruvio:http-idempotency:v1:{" + Reference([.. identity, "key-1"], []) + "}",
            IdempotencyMiddleware.StorageKey(identity, "key-1"));
        Assert.Equal(Reference([.. identity, .. request], body),
            IdempotencyMiddleware.Fingerprint(identity, request, body));
    }
}
