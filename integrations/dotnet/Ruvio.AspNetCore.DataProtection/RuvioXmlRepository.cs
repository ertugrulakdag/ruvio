using System.Security.Cryptography;
using System.Xml;
using System.Xml.Linq;
using Microsoft.AspNetCore.DataProtection.Repositories;
using Microsoft.Extensions.Options;
using Ruvio.Client;

namespace Ruvio.AspNetCore.DataProtection;

/// <summary>
/// Provides an XML repository implementation for storing ASP.NET Core Data Protection keys in a Ruvio data store.
/// </summary>
public sealed class RuvioXmlRepository : IXmlRepository
{
    private readonly IRuvioClient client;
    private readonly string keyName;
    private readonly int maxElementBytes;
    private readonly int maxElements;
    private readonly TimeSpan timeout;
    private readonly bool requireEncryption;

    /// <summary>
    /// Initializes a new instance of the <see cref="RuvioXmlRepository"/> class.
    /// </summary>
    /// <param name="client">The Ruvio client used to interact with the underlying data store.</param>
    /// <param name="options">The options used to configure the repository.</param>
    public RuvioXmlRepository(IRuvioClient client, IOptions<RuvioDataProtectionOptions> options)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(options);
        var settings = options.Value;

        settings.Validate();
        this.client = client;

        // The strict encoder rejects invalid UTF-16 here, once, instead of on every call.
        keyName = RuvioDataProtectionOptions.Utf8.GetString(Bytes(settings.Key));
        maxElementBytes = settings.MaxElementBytes;
        maxElements = settings.MaxElements;
        timeout = settings.OperationTimeout;
        requireEncryption = settings.RequireEncryption;
    }

    /// <inheritdoc />
    public IReadOnlyCollection<XElement> GetAllElements()
    {
        string name = KeyName();
        long ttl = Run(token => client.GetTimeToLiveMillisecondsAsync(name, token));

        // Key material must not expire: a TTL would let the server silently delete the ring.
        if (ttl >= 0)
            throw new RuvioException("Data Protection key ring must not expire");

        IReadOnlyDictionary<string, string> fields = Run(token => client.GetHashAllAsync(name, token));

        if (fields.Count > maxElements)
            throw new RuvioException("Data Protection key ring exceeds MaxElements");

        var elements = new List<XElement>(fields.Count);

        foreach (string xml in fields.Values)
        {
            if (RuvioDataProtectionOptions.Utf8.GetByteCount(xml) > maxElementBytes)
                throw new RuvioException("Data Protection XML exceeds MaxElementBytes");

            using var reader = XmlReader.Create(new StringReader(xml), new XmlReaderSettings
            {
                DtdProcessing = DtdProcessing.Prohibit,
                XmlResolver = null,
                MaxCharactersInDocument = maxElementBytes,
            });

            elements.Add(XElement.Load(reader));
        }

        return elements.AsReadOnly();
    }

    /// <inheritdoc />
    public void StoreElement(XElement element, string friendlyName)
    {
        ArgumentNullException.ThrowIfNull(element);

        if (requireEncryption && element.DescendantsAndSelf().Attributes().Any(
            a => a.Name.LocalName == "requiresEncryption" && a.Value.Equals("true", StringComparison.OrdinalIgnoreCase)))
            throw new InvalidOperationException("Refusing to persist an unencrypted Data Protection secret.");

        string xml = element.ToString(SaveOptions.DisableFormatting);

        if (RuvioDataProtectionOptions.Utf8.GetByteCount(xml) > maxElementBytes)
            throw new ArgumentException("Data Protection XML exceeds MaxElementBytes.", nameof(element));

        string value = xml;
        // Content addressing preserves every distinct key/revocation and makes identical writes idempotent.
        string field = Convert.ToHexString(SHA256.HashData(RuvioDataProtectionOptions.Utf8.GetBytes(value)));
        string name = KeyName();
        long ttl = Run(token => client.GetTimeToLiveMillisecondsAsync(name, token));

        if (ttl >= 0)
            throw new RuvioException("Data Protection key ring must not expire");

        if (Run(token => client.HashFieldExistsAsync(name, field, token)))
            return;

        // Fail at the limit rather than evicting old keys: dropping one could make payloads it protected unreadable.
        if (Run(token => client.GetHashLengthAsync(name, token)) >= maxElements)
            throw new RuvioException("Data Protection key ring reached MaxElements");

        long added = Run(token => client.SetHashFieldsAsync(name, (field, value)));

        if (added is < 0 or > 1)
            throw Unexpected();
    }

    private string KeyName() => keyName;

    private T Run<T>(Func<CancellationToken, ValueTask<T>> call)
    {
        using var deadline = new CancellationTokenSource(timeout);

        return Task.Run(() => call(deadline.Token).AsTask()).GetAwaiter().GetResult();
    }

    private static byte[] Bytes(string value) => RuvioDataProtectionOptions.Utf8.GetBytes(value);
    private static RuvioException Unexpected() => new("Invalid Data Protection repository response.");
}
