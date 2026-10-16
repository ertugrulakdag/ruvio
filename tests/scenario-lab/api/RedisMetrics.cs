using System.Globalization;
using System.Text;
using StackExchange.Redis;

internal static class RedisMetrics
{
    private static readonly HashSet<string> NumericMetrics =
    [
        "used_memory",
        "used_memory_rss",
        "used_memory_peak",
        "total_system_memory",
        "instantaneous_ops_per_sec",
        "total_commands_processed",
        "total_connections_received",
        "total_net_input_bytes",
        "total_net_output_bytes",
        "rejected_connections",
        "expired_keys",
        "evicted_keys",
        "keyspace_hits",
        "keyspace_misses",
        "master_repl_offset",
        "aof_pending_bio_fsync",
        "aof_delayed_fsync",
        "rdb_last_cow_size",
        "aof_last_cow_size",
        "used_cpu_sys",
        "used_cpu_user",
        "used_cpu_sys_children",
        "used_cpu_user_children"
    ];

    public static async Task<string> RenderAsync(IConnectionMultiplexer multiplexer)
    {
        RedisResult[] responses =
        [
            await multiplexer.GetDatabase().ExecuteAsync("INFO").ConfigureAwait(false),
            await multiplexer.GetDatabase().ExecuteAsync("INFO", "commandstats").ConfigureAwait(false)
        ];
        var output = new StringBuilder();
        var declared = new HashSet<string>(StringComparer.Ordinal);

        foreach (string raw in responses
                     .SelectMany(response => (response.ToString() ?? "")
                         .Split('\n', StringSplitOptions.RemoveEmptyEntries)))
        {
            string line = raw.TrimEnd('\r');

            if (line.Length == 0 || line[0] == '#')
                continue;

            int colon = line.IndexOf(':');

            if (colon <= 0)
                continue;

            string name = line[..colon];
            string value = line[(colon + 1)..];

            if (NumericMetrics.Contains(name) &&
                double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out double number))
            {
                Write(output, declared, "scenario_redis_" + name, number, counter: name.EndsWith("_processed") ||
                    name.EndsWith("_received") || name.EndsWith("_bytes") || name is "expired_keys" or "evicted_keys" or
                    "rejected_connections" or "keyspace_hits" or "keyspace_misses" ||
                    name.StartsWith("used_cpu_", StringComparison.Ordinal));
            }

            if (name.StartsWith("cmdstat_", StringComparison.Ordinal))
            {
                string command = name["cmdstat_".Length..];

                foreach (string field in value.Split(','))
                {
                    int equals = field.IndexOf('=');

                    if (equals <= 0 ||
                        !double.TryParse(field[(equals + 1)..], NumberStyles.Float, CultureInfo.InvariantCulture, out number))
                        continue;

                    string metric = field[..equals] switch
                    {
                        "calls" => "scenario_redis_command_calls_total",
                        "usec" => "scenario_redis_command_cpu_microseconds_total",
                        "usec_per_call" => "scenario_redis_command_cpu_microseconds_per_call",
                        _ => ""
                    };

                    if (metric.Length > 0)
                        Write(output, declared, metric, number, counter: metric.EndsWith("_total", StringComparison.Ordinal),
                            ("command", command));
                }
            }
        }

        return output.ToString();
    }

    private static void Write(
        StringBuilder output,
        HashSet<string> declared,
        string name,
        double value,
        bool counter,
        (string Key, string Value)? label = null)
    {
        if (declared.Add(name))
            output.Append("# TYPE ").Append(name).Append(counter ? " counter\n" : " gauge\n");

        output.Append(name);

        if (label is { } item)
            output.Append("{").Append(item.Key).Append("=\"").Append(item.Value).Append("\"}");

        output.Append(' ').Append(value.ToString("R", CultureInfo.InvariantCulture)).Append('\n');
    }
}
