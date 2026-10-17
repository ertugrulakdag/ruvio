using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Ruvio.Client.AspNetCore;

/// <summary>Registers an owning <see cref="RuvioConnection"/> and its shared database client.</summary>
/// <remarks>
/// Every overload registers the same singletons: <see cref="RuvioConnection"/>, <see cref="IRuvioConnection"/>,
/// <see cref="IRuvioClient"/> for <see cref="RuvioClientOptions.Database"/>, and <see cref="RuvioClient"/>.
/// The connection opens on first use of any of them, blocking that first resolve, and the container disposes it at shutdown.
/// </remarks>
public static class RuvioServiceCollectionExtensions
{
    /// <summary>
    /// Registers one owning connection and its configured database-scoped client.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Sets the connection options: host, port, credentials, database, timeouts and reconnect policy.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="configure"/> is null.</exception>
    /// <example>
    /// <code>
    /// builder.Services.AddRuvioClient(o =>
    /// {
    ///     o.Host = "ruvio.internal";
    ///     o.Password = builder.Configuration["Ruvio:Password"];
    /// });
    /// </code>
    /// </example>
    public static IServiceCollection AddRuvioClient(
        this IServiceCollection services,
        Action<RuvioClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        services.AddOptions<RuvioClientOptions>().Configure(configure);
        RegisterClient(services);

        return services;
    }

    /// <summary>
    /// Registers a singleton connection and runs <paramref name="configureConnection"/>
    /// after it opens. Use it for connection lifecycle events. Reconnect timing stays on
    /// <see cref="RuvioClientOptions"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Sets the connection options.</param>
    /// <param name="configureConnection">Runs once with the open connection, for example to subscribe to <see cref="RuvioConnection.ConnectionLost"/>. If it throws, the connection is disposed and the exception reaches the caller that resolved the service.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configureConnection"/> is null.</exception>
    /// <example>
    /// <code>
    /// builder.Services.AddRuvioClient(
    ///     o => o.Host = "ruvio.internal",
    ///     connection => connection.ConnectionLost += (_, notice) => logger.LogWarning("Ruvio socket lost: {Notice}", notice));
    /// </code>
    /// </example>
    public static IServiceCollection AddRuvioClient(
        this IServiceCollection services,
        Action<RuvioClientOptions> configure,
        Action<RuvioConnection> configureConnection)
    {
        ArgumentNullException.ThrowIfNull(configureConnection);
        services.AddSingleton(new RuvioConnectionSetup(configureConnection));

        return services.AddRuvioClient(configure);
    }

    /// <summary>
    /// Registers a singleton <see cref="RuvioClient"/> for <paramref name="host"/>:<paramref name="port"/>,
    /// bound to <paramref name="database"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="host">The server host name or address.</param>
    /// <param name="port">The RESP port, usually 6379.</param>
    /// <param name="database">The logical database to <c>SELECT</c>. Default 0. Cluster mode supports 0 only.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddRuvioClient("localhost", 6379);
    /// </code>
    /// </example>
    public static IServiceCollection AddRuvioClient(
        this IServiceCollection services,
        string host,
        int port,
        int database = 0)
    {
        return services.AddRuvioClient(options =>
        {
            options.Host = host;
            options.Port = port;
            options.Database = database;
        });
    }

    /// <summary>
    /// Binds <see cref="RuvioClientOptions"/> from configuration and registers a singleton <see cref="RuvioClient"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">A configuration section whose keys are option names: <c>Host</c>, <c>Port</c>, <c>Username</c>, <c>Password</c>, <c>Database</c>, <c>ConnectTimeout</c>, <c>MaxInFlightRequests</c>, <c>DedicatedReaderThread</c>, <c>DiscoverCluster</c>, <c>Reconnect</c>, <c>MaxReconnectAttempts</c>, <c>ReconnectBaseDelay</c> and <c>ReconnectMaxDelay</c>. Missing keys keep the defaults. Time spans use the <c>hh:mm:ss</c> form.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="configuration"/> is null.</exception>
    /// <exception cref="FormatException">A numeric, boolean or time value in the section cannot be parsed (raised when the options are first read).</exception>
    /// <example>
    /// <code>
    /// builder.Services.AddRuvioClient(builder.Configuration.GetSection("Ruvio"));
    /// </code>
    /// </example>
    public static IServiceCollection AddRuvioClient(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddOptions<RuvioClientOptions>().Configure(options => BindOptions(configuration, options));
        RegisterClient(services);

        return services;
    }

    // Reads each setting by hand: ConfigurationBinder reflects over the options type, which breaks trimming and NativeAOT.
    private static void BindOptions(IConfiguration configuration, RuvioClientOptions options)
    {
        if (configuration["Host"] is { Length: > 0 } host)
            options.Host = host;

        if (Number(configuration, "Port") is { } port)
            options.Port = port;

        if (configuration["Username"] is { } username)
            options.Username = username;

        if (configuration["Password"] is { } password)
            options.Password = password;

        if (Number(configuration, "Database") is { } database)
            options.Database = database;

        if (Span(configuration, "ConnectTimeout") is { } connectTimeout)
            options.ConnectTimeout = connectTimeout;

        if (Number(configuration, "MaxInFlightRequests") is { } inFlight)
            options.MaxInFlightRequests = inFlight;

        if (Flag(configuration, "DedicatedReaderThread") is { } dedicatedReader)
            options.DedicatedReaderThread = dedicatedReader;

        if (Flag(configuration, "DiscoverCluster") is { } discover)
            options.DiscoverCluster = discover;

        if (Flag(configuration, "Reconnect") is { } reconnect)
            options.Reconnect = reconnect;

        if (Number(configuration, "MaxReconnectAttempts") is { } attempts)
            options.MaxReconnectAttempts = attempts;

        if (Span(configuration, "ReconnectBaseDelay") is { } baseDelay)
            options.ReconnectBaseDelay = baseDelay;

        if (Span(configuration, "ReconnectMaxDelay") is { } maxDelay)
            options.ReconnectMaxDelay = maxDelay;
    }

    private static int? Number(IConfiguration configuration, string key)
        => configuration[key] is { } text
            ? int.Parse(text, System.Globalization.CultureInfo.InvariantCulture)
            : null;

    private static bool? Flag(IConfiguration configuration, string key)
        => configuration[key] is { } text ? bool.Parse(text) : null;

    private static TimeSpan? Span(IConfiguration configuration, string key)
        => configuration[key] is { } text
            ? TimeSpan.Parse(text, System.Globalization.CultureInfo.InvariantCulture)
            : null;

    /// <summary>
    /// The connection owns the socket resources and the database-scoped clients.
    /// </summary>
    private static void RegisterClient(IServiceCollection services)
    {
        services.AddSingleton(CreateConnection);
        services.AddSingleton<IRuvioConnection>(static provider => provider.GetRequiredService<RuvioConnection>());
        services.AddSingleton<IRuvioClient>(static provider =>
        {
            RuvioClientOptions options = provider.GetRequiredService<IOptions<RuvioClientOptions>>().Value;

            return provider.GetRequiredService<RuvioConnection>()
                .GetDatabaseAsync(options.Database).GetAwaiter().GetResult();
        });
        
        services.AddSingleton(static provider => (RuvioClient)provider.GetRequiredService<IRuvioClient>());
    }

    private static RuvioConnection CreateConnection(IServiceProvider serviceProvider)
    {
        RuvioClientOptions options = serviceProvider.GetRequiredService<IOptions<RuvioClientOptions>>().Value;
        RuvioConnection connection = RuvioConnection.ConnectAsync(options).GetAwaiter().GetResult();

        try
        {
            foreach (RuvioConnectionSetup setup in serviceProvider.GetServices<RuvioConnectionSetup>())
            {
                setup.Configure(connection);
            }

            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }
}

internal sealed class RuvioConnectionSetup(Action<RuvioConnection> configure)
{
    public Action<RuvioConnection> Configure { get; } = configure;
}
