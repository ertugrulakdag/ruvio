use std::io::{self, Write};

use std::net::SocketAddr;

/// Sampled latency of one command (one command in 16 is timed).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandLatency {
    pub command: &'static str,
    pub samples: u64,
    pub sum_nanos: u128,
    /// `(upper bound in ns, cumulative samples)` for each non-empty bucket.
    pub buckets: Vec<(u64, u64)>,
    pub p50_nanos: u64,
    pub p99_nanos: u64,
    pub p999_nanos: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub backend: &'static str,
    pub durability: String,
    pub persistence_state: &'static str,
    pub recovered_wal_records: u64,
    pub wal_last_sequence: u64,
    pub wal_unsynced_bytes: u64,
    pub shard_id: u64,
    pub shard_count: u64,
    pub allocator: &'static str,
    pub connections: u64,
    pub active_connections: u64,
    pub rejected_connections: u64,
    pub rate_limited_commands: u64,
    pub commands: u64,
    pub gets: u64,
    pub sets: u64,
    pub increments: u64,
    pub other_commands: u64,
    pub get_command_duration_nanos: u128,
    pub set_command_duration_nanos: u128,
    pub increment_command_duration_nanos: u128,
    pub other_command_duration_nanos: u128,
    pub wal_sync_count: u64,
    pub wal_sync_duration_nanos: u128,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub protocol_errors: u64,
    pub charged_memory_bytes: u64,
    pub key_memory_bytes: u64,
    pub value_memory_bytes: u64,
    pub collection_memory_bytes: u64,
    pub index_memory_bytes: u64,
    pub pooled_value_bytes: u64,
    pub process_resident_memory_bytes: Option<u64>,
    pub keys: u64,
    pub expired_keys_passive: u64,
    pub expired_keys_active: u64,
    pub keys_with_ttl: u64,
    pub evicted_keys: u64,
    pub eviction_scans: u64,
    pub eviction_slots_inspected: u64,
    pub allocation_events: u64,
    pub allocation_bytes: u64,
    pub command_latency: Vec<CommandLatency>,
    pub up: bool,
    pub ready: bool,
}

pub const HTTP_NOT_FOUND: &[u8] =
    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

pub const HTTP_UNAUTHORIZED: &[u8] = b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\nWWW-Authenticate: Bearer\r\n\r\n";

pub fn bearer_matches(request: &[u8], token: &[u8]) -> bool {
    if token.is_empty() {
        return true;
    }

    crate::security::constant_time_eq(bearer_token(request).unwrap_or(b""), token)
}

fn bearer_token(request: &[u8]) -> Option<&[u8]> {
    for line in request.split(|&byte| byte == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some((name, value)) = split_once_byte(line, b':') else {
            continue;
        };

        if !name.eq_ignore_ascii_case(b"authorization") {
            continue;
        }

        let value = trim_ascii(value);
        let Some((scheme, rest)) = split_once_byte(value, b' ') else {
            continue;
        };

        if scheme.eq_ignore_ascii_case(b"bearer") {
            return Some(trim_ascii(rest));
        }
    }

    None
}

fn split_once_byte(bytes: &[u8], separator: u8) -> Option<(&[u8], &[u8])> {
    let index = bytes.iter().position(|byte| *byte == separator)?;

    Some((&bytes[..index], &bytes[index + 1..]))
}

fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map(|index| index + 1)
        .unwrap_or(start);

    &bytes[start..end]
}

pub fn shard_metrics_addr(base: SocketAddr, shard_id: u32) -> Option<SocketAddr> {
    Some(SocketAddr::new(
        base.ip(),
        base.port().checked_add(u16::try_from(shard_id).ok()?)?,
    ))
}

pub fn is_metrics_get(request: &[u8]) -> bool {
    let line = request
        .split(|&byte| byte == b'\n')
        .next()
        .unwrap_or(request);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let mut parts = line.split(|&byte| byte == b' ');
    let method = parts.next().unwrap_or(b"");
    let path = parts.next().unwrap_or(b"");

    method.eq_ignore_ascii_case(b"GET") && (path == b"/metrics" || path == b"/metrics/")
}

pub fn http_ok(body: &[u8]) -> Vec<u8> {
    let mut response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();

    response.extend_from_slice(body);
    response
}

pub fn encode_prometheus(snapshot: &MetricsSnapshot) -> Vec<u8> {
    let mut body = Vec::with_capacity(2048);

    write_prometheus(&mut body, snapshot).expect("writing Prometheus text into memory cannot fail");
    body
}

fn write_command_latency(
    output: &mut impl Write,
    snapshot: &MetricsSnapshot,
    labels: &str,
) -> io::Result<()> {
    if snapshot.command_latency.is_empty() {
        return Ok(());
    }

    writeln!(
        output,
        "# HELP ruvio_command_duration_seconds Sampled command processing time (one command in 16), excluding socket I/O."
    )?;
    writeln!(output, "# TYPE ruvio_command_duration_seconds histogram")?;

    for latency in &snapshot.command_latency {
        let command = latency.command;

        for (bound, cumulative) in &latency.buckets {
            writeln!(
                output,
                "ruvio_command_duration_seconds_bucket{{{labels},command=\"{command}\",le=\"{}\"}} {cumulative}",
                *bound as f64 / 1_000_000_000.0
            )?;
        }

        writeln!(
            output,
            "ruvio_command_duration_seconds_bucket{{{labels},command=\"{command}\",le=\"+Inf\"}} {}",
            latency.samples
        )?;
        writeln!(
            output,
            "ruvio_command_duration_seconds_sum{{{labels},command=\"{command}\"}} {}",
            latency.sum_nanos as f64 / 1_000_000_000.0
        )?;
        writeln!(
            output,
            "ruvio_command_duration_seconds_count{{{labels},command=\"{command}\"}} {}",
            latency.samples
        )?;
    }

    Ok(())
}

fn write_prometheus(output: &mut impl Write, snapshot: &MetricsSnapshot) -> io::Result<()> {
    let labels = format!(
        "backend=\"{}\",shard=\"{}\"",
        snapshot.backend, snapshot.shard_id
    );

    write_command_latency(output, snapshot, &labels)?;

    writeln!(
        output,
        "# HELP ruvio_up 1 when this shard process is running."
    )?;
    writeln!(output, "# TYPE ruvio_up gauge")?;
    writeln!(output, "ruvio_up{{{labels}}} {}", u64::from(snapshot.up))?;
    writeln!(
        output,
        "# HELP ruvio_ready 1 when this shard is serving clients."
    )?;
    writeln!(output, "# TYPE ruvio_ready gauge")?;
    writeln!(
        output,
        "ruvio_ready{{{labels}}} {}",
        u64::from(snapshot.ready)
    )?;
    write_gauge(
        output,
        "ruvio_connected_clients",
        "Currently active client connections.",
        &labels,
        snapshot.active_connections,
    )?;
    write_counter(
        output,
        "ruvio_commands_total",
        "Commands processed by this shard.",
        &labels,
        snapshot.commands,
    )?;
    write_counter(
        output,
        "ruvio_get_commands_total",
        "GET commands processed by this shard.",
        &labels,
        snapshot.gets,
    )?;
    write_counter(
        output,
        "ruvio_set_commands_total",
        "SET commands processed by this shard.",
        &labels,
        snapshot.sets,
    )?;
    write_counter(
        output,
        "ruvio_increment_commands_total",
        "INCR/INCRBY/DECR/DECRBY commands processed by this shard.",
        &labels,
        snapshot.increments,
    )?;
    write_counter(
        output,
        "ruvio_other_commands_total",
        "Non-hot-path commands processed by this shard.",
        &labels,
        snapshot.other_commands,
    )?;
    write_float_counter(
        output,
        "ruvio_command_processing_seconds_sum",
        "Estimated cumulative time spent processing commands on this shard, excluding socket I/O; one command in 16 is timed and scaled.",
        &labels,
        (snapshot.get_command_duration_nanos
            + snapshot.set_command_duration_nanos
            + snapshot.increment_command_duration_nanos
            + snapshot.other_command_duration_nanos) as f64
            / 1_000_000_000.0,
    )?;
    write_float_counter(
        output,
        "ruvio_get_command_processing_seconds_sum",
        "Cumulative time spent processing GET commands on this shard.",
        &labels,
        snapshot.get_command_duration_nanos as f64 / 1_000_000_000.0,
    )?;
    write_float_counter(
        output,
        "ruvio_set_command_processing_seconds_sum",
        "Cumulative time spent processing SET commands on this shard.",
        &labels,
        snapshot.set_command_duration_nanos as f64 / 1_000_000_000.0,
    )?;
    write_float_counter(
        output,
        "ruvio_increment_command_processing_seconds_sum",
        "Cumulative time spent processing increment commands on this shard.",
        &labels,
        snapshot.increment_command_duration_nanos as f64 / 1_000_000_000.0,
    )?;
    write_float_counter(
        output,
        "ruvio_other_command_processing_seconds_sum",
        "Cumulative time spent processing other commands on this shard.",
        &labels,
        snapshot.other_command_duration_nanos as f64 / 1_000_000_000.0,
    )?;
    write_counter(
        output,
        "ruvio_wal_syncs_total",
        "WAL synchronization attempts with dirty bytes.",
        &labels,
        snapshot.wal_sync_count,
    )?;
    write_float_counter(
        output,
        "ruvio_wal_sync_duration_seconds_sum",
        "Cumulative time waiting for and performing dirty WAL synchronization.",
        &labels,
        snapshot.wal_sync_duration_nanos as f64 / 1_000_000_000.0,
    )?;

    write_counter(
        output,
        "ruvio_net_input_bytes_total",
        "Bytes read from this shard's client connections.",
        &labels,
        snapshot.input_bytes,
    )?;
    write_counter(
        output,
        "ruvio_net_output_bytes_total",
        "Bytes written to this shard's client connections.",
        &labels,
        snapshot.output_bytes,
    )?;
    // Admission counters are process-wide, so only shard 0 reports them.
    if snapshot.shard_id == 0 {
        write_counter(
            output,
            "ruvio_connections_total",
            "Accepted client connections. Process-wide, on shard 0 only.",
            &labels,
            snapshot.connections,
        )?;
        write_counter(
            output,
            "ruvio_rejected_connections_total",
            "Connections rejected by admission limits. Process-wide, on shard 0 only.",
            &labels,
            snapshot.rejected_connections,
        )?;
        write_counter(
            output,
            "ruvio_rate_limited_commands_total",
            "Commands rejected by the per-connection command rate limit. Process-wide, on shard 0 only.",
            &labels,
            snapshot.rate_limited_commands,
        )?;
        write_counter(
            output,
            "ruvio_protocol_errors_total",
            "Frames the parser could not read, which close the connection. Process-wide, on shard 0 only.",
            &labels,
            snapshot.protocol_errors,
        )?;
        write_counter(
            output,
            "ruvio_log_lines_dropped_total",
            "Log lines dropped because the bounded writer was full. Process-wide and reported only on shard 0.",
            &labels,
            crate::log::dropped_log_lines(),
        )?;
    }

    write_gauge(
        output,
        "ruvio_charged_memory_bytes",
        "Deterministic charged shard memory.",
        &labels,
        snapshot.charged_memory_bytes,
    )?;
    write_gauge(
        output,
        "ruvio_key_memory_bytes",
        "Charged key bytes.",
        &labels,
        snapshot.key_memory_bytes,
    )?;
    write_gauge(
        output,
        "ruvio_value_memory_bytes",
        "Charged value bytes.",
        &labels,
        snapshot.value_memory_bytes,
    )?;
    write_gauge(
        output,
        "ruvio_collection_memory_bytes",
        "Charged collection bytes.",
        &labels,
        snapshot.collection_memory_bytes,
    )?;
    write_gauge(
        output,
        "ruvio_index_memory_bytes",
        "Charged primary-index bytes.",
        &labels,
        snapshot.index_memory_bytes,
    )?;
    write_gauge(
        output,
        "ruvio_pooled_value_bytes",
        "Bytes retained in the shard value pool.",
        &labels,
        snapshot.pooled_value_bytes,
    )?;
    if let Some(bytes) = snapshot.process_resident_memory_bytes {
        write_gauge(
            output,
            "ruvio_process_resident_memory_bytes",
            "Resident memory of the process; repeated across shard labels.",
            &labels,
            bytes,
        )?;
    }
    write_gauge(
        output,
        "ruvio_keys",
        "Live keys on this shard.",
        &labels,
        snapshot.keys,
    )?;
    writeln!(
        output,
        "# HELP ruvio_expired_keys_total Keys removed after their deadline."
    )?;
    writeln!(output, "# TYPE ruvio_expired_keys_total counter")?;
    writeln!(
        output,
        "ruvio_expired_keys_total{{{labels},reason=\"passive\"}} {}",
        snapshot.expired_keys_passive
    )?;
    writeln!(
        output,
        "ruvio_expired_keys_total{{{labels},reason=\"active\"}} {}",
        snapshot.expired_keys_active
    )?;
    write_gauge(
        output,
        "ruvio_keys_with_ttl",
        "Stored keys that currently carry an expiration deadline.",
        &labels,
        snapshot.keys_with_ttl,
    )?;
    write_counter(
        output,
        "ruvio_evicted_keys_total",
        "Keys removed to stay under the shard memory budget.",
        &labels,
        snapshot.evicted_keys,
    )?;
    write_counter(
        output,
        "ruvio_eviction_scans_total",
        "Eviction victim searches started under memory pressure.",
        &labels,
        snapshot.eviction_scans,
    )?;
    write_counter(
        output,
        "ruvio_eviction_slots_inspected_total",
        "Index slots, including empty hash buckets, inspected by eviction victim searches.",
        &labels,
        snapshot.eviction_slots_inspected,
    )?;
    write_counter(
        output,
        "ruvio_allocation_events_total",
        "Mutations that increased charged shard memory.",
        &labels,
        snapshot.allocation_events,
    )?;
    write_counter(
        output,
        "ruvio_allocation_bytes_total",
        "Charged bytes added by memory-increasing mutations.",
        &labels,
        snapshot.allocation_bytes,
    )?;
    write_gauge(
        output,
        "ruvio_recovered_wal_records",
        "WAL records replayed at startup.",
        &labels,
        snapshot.recovered_wal_records,
    )?;
    write_gauge(
        output,
        "ruvio_wal_last_sequence",
        "Last assigned WAL sequence on this shard.",
        &labels,
        snapshot.wal_last_sequence,
    )?;
    write_gauge(
        output,
        "ruvio_wal_unsynced_bytes",
        "Dirty WAL bytes not yet synchronized.",
        &labels,
        snapshot.wal_unsynced_bytes,
    )?;
    write_gauge(
        output,
        "ruvio_shard_count",
        "Configured shard count for this process.",
        &labels,
        snapshot.shard_count,
    )?;
    writeln!(
        output,
        "# HELP ruvio_build_info Build and configuration labels."
    )?;
    writeln!(output, "# TYPE ruvio_build_info gauge")?;
    writeln!(
        output,
        "ruvio_build_info{{backend=\"{}\",shard=\"{}\",version=\"{}\",durability=\"{}\",persistence_state=\"{}\",allocator=\"{}\"}} 1",
        snapshot.backend,
        snapshot.shard_id,
        env!("CARGO_PKG_VERSION"),
        snapshot.durability,
        snapshot.persistence_state,
        snapshot.allocator
    )?;
    Ok(())
}

fn write_counter(
    output: &mut impl Write,
    name: &str,
    help: &str,
    labels: &str,
    value: u64,
) -> io::Result<()> {
    writeln!(output, "# HELP {name} {help}")?;
    writeln!(output, "# TYPE {name} counter")?;
    writeln!(output, "{name}{{{labels}}} {value}")
}

fn write_float_counter(
    output: &mut impl Write,
    name: &str,
    help: &str,
    labels: &str,
    value: f64,
) -> io::Result<()> {
    writeln!(output, "# HELP {name} {help}")?;
    writeln!(output, "# TYPE {name} counter")?;
    writeln!(output, "{name}{{{labels}}} {value}")
}

fn write_gauge(
    output: &mut impl Write,
    name: &str,
    help: &str,
    labels: &str,
    value: u64,
) -> io::Result<()> {
    writeln!(output, "# HELP {name} {help}")?;
    writeln!(output, "# TYPE {name} gauge")?;
    writeln!(output, "{name}{{{labels}}} {value}")
}

pub fn process_resident_memory_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;

        for line in status.lines() {
            if let Some(value) = line.strip_prefix("VmRSS:") {
                let kibibytes = value.split_whitespace().next()?.parse::<u64>().ok()?;
                return kibibytes.checked_mul(1024);
            }
        }

        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> MetricsSnapshot {
        MetricsSnapshot {
            backend: "portable",
            durability: "always".into(),
            persistence_state: "ok",
            recovered_wal_records: 2,
            wal_last_sequence: 4,
            wal_unsynced_bytes: 0,
            shard_id: 0,
            shard_count: 1,
            allocator: "system",
            connections: 3,
            active_connections: 1,
            rejected_connections: 0,
            rate_limited_commands: 0,
            commands: 8,
            gets: 5,
            sets: 2,
            increments: 1,
            other_commands: 0,
            get_command_duration_nanos: 5_000_000,
            set_command_duration_nanos: 2_000_000,
            increment_command_duration_nanos: 1_000_000,
            other_command_duration_nanos: 0,
            wal_sync_count: 2,
            wal_sync_duration_nanos: 3_000_000,
            input_bytes: 40,
            output_bytes: 20,
            protocol_errors: 0,
            charged_memory_bytes: 128,
            key_memory_bytes: 8,
            value_memory_bytes: 16,
            collection_memory_bytes: 0,
            index_memory_bytes: 64,
            pooled_value_bytes: 0,
            process_resident_memory_bytes: Some(4096),
            keys: 2,
            expired_keys_passive: 3,
            expired_keys_active: 1,
            keys_with_ttl: 4,
            evicted_keys: 2,
            eviction_scans: 3,
            eviction_slots_inspected: 40,
            allocation_events: 6,
            allocation_bytes: 96,
            command_latency: Vec::new(),
            up: true,
            ready: true,
        }
    }

    #[test]
    fn prometheus_text_is_labeled_per_shard() {
        let text = String::from_utf8(encode_prometheus(&sample())).unwrap();

        assert!(text.contains("# TYPE ruvio_commands_total counter"));
        assert!(text.contains("ruvio_commands_total{backend=\"portable\",shard=\"0\"} 8"));
        assert!(text.contains(
            "ruvio_command_processing_seconds_sum{backend=\"portable\",shard=\"0\"} 0.008"
        ));
        assert!(text.contains(
            "ruvio_get_command_processing_seconds_sum{backend=\"portable\",shard=\"0\"} 0.005"
        ));
        assert!(text.contains("ruvio_wal_syncs_total{backend=\"portable\",shard=\"0\"} 2"));
        assert!(text.contains(
            "ruvio_wal_sync_duration_seconds_sum{backend=\"portable\",shard=\"0\"} 0.003"
        ));
        assert!(text.contains("ruvio_keys{backend=\"portable\",shard=\"0\"} 2"));
        assert!(text.contains(
            "ruvio_expired_keys_total{backend=\"portable\",shard=\"0\",reason=\"passive\"} 3"
        ));
        assert!(text.contains(
            "ruvio_expired_keys_total{backend=\"portable\",shard=\"0\",reason=\"active\"} 1"
        ));
        assert!(text.contains("ruvio_keys_with_ttl{backend=\"portable\",shard=\"0\"} 4"));
        assert!(text.contains("ruvio_evicted_keys_total{backend=\"portable\",shard=\"0\"} 2"));
        assert!(text.contains("ruvio_eviction_scans_total{backend=\"portable\",shard=\"0\"} 3"));
        assert!(
            text.contains(
                "ruvio_eviction_slots_inspected_total{backend=\"portable\",shard=\"0\"} 40"
            )
        );
        assert!(text.contains("durability=\"always\""));
        assert!(text.contains("ruvio_up{backend=\"portable\",shard=\"0\"} 1"));
        assert!(text.contains(
            "ruvio_process_resident_memory_bytes{backend=\"portable\",shard=\"0\"} 4096"
        ));
        assert!(text.contains("ruvio_log_lines_dropped_total{backend=\"portable\",shard=\"0\"} 0"));
    }

    #[test]
    fn log_drop_counter_is_reported_only_on_shard_zero() {
        let mut snapshot = sample();

        snapshot.shard_id = 1;
        let text = String::from_utf8(encode_prometheus(&snapshot)).unwrap();

        assert!(!text.contains("ruvio_log_lines_dropped_total"));
    }

    #[test]
    fn recognizes_prometheus_scrape_paths() {
        assert!(is_metrics_get(b"GET /metrics HTTP/1.1\r\n"));
        assert!(is_metrics_get(b"GET /metrics/ HTTP/1.0\r\n"));
        assert!(!is_metrics_get(b"POST /metrics HTTP/1.1\r\n"));
        assert!(!is_metrics_get(b"GET /health HTTP/1.1\r\n"));
        assert!(bearer_matches(b"GET /metrics HTTP/1.1\r\n\r\n", b""));
        assert!(bearer_matches(
            b"GET /metrics HTTP/1.1\r\nAuthorization: Bearer scrape\r\n\r\n",
            b"scrape"
        ));
        assert!(!bearer_matches(
            b"GET /metrics HTTP/1.1\r\nAuthorization: Bearer nope\r\n\r\n",
            b"scrape"
        ));
        let response = http_ok(b"ok\n");

        assert!(response.starts_with(b"HTTP/1.1 200 OK"));
        assert!(
            response
                .windows(b"ok\n".len())
                .any(|window| window == b"ok\n")
        );
    }

    #[test]
    fn process_rss_is_available_on_linux_only() {
        #[cfg(target_os = "linux")]
        assert!(process_resident_memory_bytes().is_some_and(|bytes| bytes > 0));

        #[cfg(not(target_os = "linux"))]
        assert_eq!(process_resident_memory_bytes(), None);
    }
}
