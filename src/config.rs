use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::Deserialize;

use crate::{
    persistence::{DurabilityProfile, PersistenceConfig, SyncWorkerMode},
    protocol::ProtocolLimits,
};

pub use crate::security::SecurityConfig;

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationRole {
    #[default]
    Off,
    Primary,
    Replica,
}

impl std::str::FromStr for ReplicationRole {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "off" => Ok(Self::Off),
            "primary" => Ok(Self::Primary),
            "replica" => Ok(Self::Replica),
            _ => Err("replication role must be off, primary, or replica"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationConfig {
    pub role: ReplicationRole,
    pub listen: Option<SocketAddr>,
    pub primary: Option<SocketAddr>,
    pub primary_name: Option<(String, u16)>,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            role: ReplicationRole::Off,
            listen: None,
            primary: None,
            primary_name: None,
        }
    }
}

/// How `shards > 1` exposes its listeners.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PortMode {
    /// One port per shard (`port + i`), `MOVED` redirects and cluster discovery.
    PerShard,
    /// One port; connections are spread over shards and commands are forwarded
    /// to the shard that owns the key.
    #[default]
    Single,
}

impl PortMode {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.to_ascii_lowercase().as_str() {
            "per-shard" | "per_shard" => Ok(Self::PerShard),
            "single" => Ok(Self::Single),
            _ => Err("port_mode must be \"single\" or \"per-shard\"".into()),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PerShard => "per-shard",
            Self::Single => "single",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppConfig {
    pub address: String,
    pub shards: usize,
    pub port_mode: PortMode,
    pub advertised_host: String,
    /// Port announced for shard 0 in `CLUSTER SLOTS`, `CLUSTER NODES`, and `MOVED`.
    /// Shard `i` is announced on this port plus `i`. `None` announces the bind port.
    pub advertised_port: Option<u16>,
    pub cpu_set: Option<String>,
    pub shutdown_grace: Duration,
    /// Per-command latency histograms (`INFO latencystats`, `/metrics`). Off = one branch per command.
    pub latency_histograms: bool,
    /// `SLOWLOG` threshold in microseconds; negative turns it off, 0 logs every timed command.
    pub slowlog_threshold_micros: i64,
    /// Entries `SLOWLOG` keeps per shard.
    pub slowlog_max_len: usize,
    /// Sampled hot keys and recent big keys (`RUVIO HOTKEYS`, `RUVIO BIGKEYS`).
    pub key_stats: bool,
    pub protocol: ProtocolLimits,
    pub timeouts: ConnectionTimeouts,
    pub connections: ConnectionLimits,
    pub requests: RequestLimits,
    pub persistence: PersistenceConfig,
    pub security: SecurityConfig,
    pub memory: MemoryLimits,
    pub metrics_address: Option<String>,
    pub scripts: ScriptLimits,
    pub hot_keys: Vec<Vec<u8>>,
    /// Original `notify-keyspace-events` letters. Empty disables the feature.
    pub notify_keyspace_events: String,
    pub replication: ReplicationConfig,
}

impl AppConfig {
    /// The limits one shard enforces: a process-wide `maxmemory` with no explicit shard limit
    /// is split evenly across the shards.
    pub fn shard_memory_limits(&self) -> MemoryLimits {
        let mut limits = self.memory;

        if limits.max_process_bytes > 0 && limits.max_shard_bytes == 0 {
            let shards = self.shards.max(1);
            let share = limits.max_process_bytes / shards;

            // The headroom is reserved once for the process, so each shard keeps its part of
            // it; subtracting all of it from every share left several shards almost no budget.
            limits.reserved_headroom_bytes /= shards;
            limits.max_shard_bytes = share.max(limits.reserved_headroom_bytes.saturating_add(1));
        }

        limits
    }

    /// Refuses a memory limit that leaves a shard too little to store anything once the
    /// reserved headroom is taken out.
    fn validate_shard_memory_budget(&self) -> Result<(), String> {
        const MIN_SHARD_BUDGET_BYTES: usize = 64 * 1024;
        let limits = self.shard_memory_limits();

        match limits.shard_admission_budget() {
            Some(budget) if budget < MIN_SHARD_BUDGET_BYTES => Err(format!(
                "memory limit leaves {budget} bytes per shard ({} shard(s), {} bytes reserved headroom per shard); \
                 each shard needs at least {MIN_SHARD_BUDGET_BYTES} bytes: raise maxmemory or lower the memory headroom",
                self.shards.max(1),
                limits.reserved_headroom_bytes,
            )),
            _ => Ok(()),
        }
    }

    pub fn load(path: Option<&Path>) -> Result<Self, String> {
        let mut config = Self::default();

        if let Some(path) = path {
            let contents = fs::read_to_string(path)
                .map_err(|error| format!("failed to read config {}: {error}", path.display()))?;

            config.apply_toml(&contents)?;
        }

        config.apply_environment(|name| env::var(name).ok())?;
        Ok(config)
    }

    pub fn apply_toml(&mut self, contents: &str) -> Result<(), String> {
        let file: FileConfig =
            toml::from_str(contents).map_err(|error| format!("invalid config file: {error}"))?;

        file.apply(self)?;
        Ok(())
    }

    pub fn apply_environment(
        &mut self,
        get: impl Fn(&str) -> Option<String>,
    ) -> Result<(), String> {
        apply_string(&get, "RUVIO_ADDR", &mut self.address);
        apply_usize(&get, "RUVIO_SHARDS", &mut self.shards)?;

        if let Some(value) = get("RUVIO_PORT_MODE") {
            self.port_mode = PortMode::parse(&value)?;
        }
        apply_string(&get, "RUVIO_ADVERTISE_HOST", &mut self.advertised_host);

        if let Some(value) = get("RUVIO_ADVERTISE_PORT") {
            self.advertised_port = Some(parse_advertised_port(&value)?);
        }

        if let Some(value) = get("RUVIO_CPUSET") {
            self.cpu_set = Some(value);
        } else if let Some(value) = get("RUVIO_CORE") {
            self.cpu_set = Some(value);
        }

        apply_duration(&get, "RUVIO_SHUTDOWN_GRACE_MS", &mut self.shutdown_grace)?;
        if let Some(value) = get("RUVIO_SLOWLOG_THRESHOLD_MICROS") {
            self.slowlog_threshold_micros = value
                .parse()
                .map_err(|_| "RUVIO_SLOWLOG_THRESHOLD_MICROS must be an integer".to_owned())?;
        }
        apply_usize(&get, "RUVIO_SLOWLOG_MAX_LEN", &mut self.slowlog_max_len)?;
        if let Some(value) = get("RUVIO_LATENCY_HISTOGRAMS") {
            self.latency_histograms = parse_bool(&value, "RUVIO_LATENCY_HISTOGRAMS")?;
        }
        if let Some(value) = get("RUVIO_KEY_STATS") {
            self.key_stats = parse_bool(&value, "RUVIO_KEY_STATS")?;
        }

        apply_usize(
            &get,
            "RUVIO_MAX_ARGUMENTS",
            &mut self.protocol.max_arguments,
        )?;
        apply_usize(
            &get,
            "RUVIO_MAX_KEY_BYTES",
            &mut self.protocol.max_key_bytes,
        )?;
        apply_usize(
            &get,
            "RUVIO_MAX_VALUE_BYTES",
            &mut self.protocol.max_value_bytes,
        )?;
        apply_usize(
            &get,
            "RUVIO_MAX_COMMAND_BYTES",
            &mut self.protocol.max_command_bytes,
        )?;
        apply_usize(
            &get,
            "RUVIO_MAX_PIPELINE_COMMANDS",
            &mut self.protocol.max_pipeline_commands,
        )?;
        apply_usize(
            &get,
            "RUVIO_MAX_CONNECTION_BUFFER_BYTES",
            &mut self.protocol.max_connection_buffer_bytes,
        )?;
        apply_usize(&get, "RUVIO_DATABASES", &mut self.protocol.databases)?;
        apply_duration(&get, "RUVIO_IDLE_TIMEOUT_MS", &mut self.timeouts.idle)?;
        apply_duration(&get, "RUVIO_READ_TIMEOUT_MS", &mut self.timeouts.read)?;
        apply_duration(&get, "RUVIO_WRITE_TIMEOUT_MS", &mut self.timeouts.write)?;
        apply_usize(
            &get,
            "RUVIO_MAX_CONNECTIONS",
            &mut self.connections.max_global,
        )?;
        apply_usize(
            &get,
            "RUVIO_MAX_CONNECTIONS_PER_IP",
            &mut self.connections.max_per_ip,
        )?;
        apply_usize(
            &get,
            "RUVIO_MAX_CONNECTIONS_PER_USER",
            &mut self.connections.max_per_user,
        )?;
        apply_u64(
            &get,
            "RUVIO_MAX_COMMANDS_PER_SECOND",
            &mut self.connections.max_commands_per_second,
        )?;
        apply_usize(
            &get,
            "RUVIO_MAX_QUEUED_REQUESTS",
            &mut self.requests.max_queued_requests,
        )?;
        apply_usize(
            &get,
            "RUVIO_MAX_OUTPUT_BUFFER_BYTES",
            &mut self.requests.max_output_buffer_bytes,
        )?;

        if let Some(value) = get("RUVIO_DURABILITY") {
            self.persistence.profile = value.parse().map_err(str::to_owned)?;
        }

        if let Some(value) = get("RUVIO_DATA_DIR") {
            self.persistence.data_dir = value.into();
        }

        apply_u64(
            &get,
            "RUVIO_WAL_SEGMENT_BYTES",
            &mut self.persistence.wal_segment_bytes,
        )?;
        apply_duration(
            &get,
            "RUVIO_WAL_SYNC_INTERVAL_MS",
            &mut self.persistence.sync_interval,
        )?;
        if let Some(value) = get("RUVIO_EXPERIMENTAL_SYNC_WORKER") {
            self.persistence.sync_worker = if parse_bool(&value, "RUVIO_EXPERIMENTAL_SYNC_WORKER")?
            {
                SyncWorkerMode::On
            } else {
                SyncWorkerMode::Off
            };
        }
        if let Some(value) = get("RUVIO_SYNC_WORKER") {
            self.persistence.sync_worker = value.parse().map_err(str::to_owned)?;
        }
        apply_u64(
            &get,
            "RUVIO_SNAPSHOT_MAX_BYTES",
            &mut self.persistence.snapshot_max_bytes,
        )?;
        apply_usize(
            &get,
            "RUVIO_SNAPSHOT_JOURNAL_BYTES",
            &mut self.persistence.snapshot_journal_bytes,
        )?;
        apply_duration(
            &get,
            "RUVIO_SNAPSHOT_INTERVAL_MS",
            &mut self.persistence.snapshot_interval,
        )?;
        apply_u64(
            &get,
            "RUVIO_SNAPSHOT_MIN_MUTATIONS",
            &mut self.persistence.snapshot_min_mutations,
        )?;
        apply_u64(
            &get,
            "RUVIO_CDC_RETAIN_BYTES",
            &mut self.persistence.cdc_retain_bytes,
        )?;
        apply_u64(
            &get,
            "RUVIO_REPLICA_RETAIN_MAX_BYTES",
            &mut self.persistence.replica_retain_max_bytes,
        )?;
        apply_string(&get, "RUVIO_REQUIREPASS", &mut self.security.requirepass);

        if let Some(value) = get("RUVIO_TLS_CERT") {
            self.security.tls_cert = value.into();
        }

        if let Some(value) = get("RUVIO_TLS_KEY") {
            self.security.tls_key = value.into();
        }

        if let Some(value) = get("RUVIO_TLS_CLIENT_CA") {
            self.security.tls_client_ca = value.into();
        }

        if let Some(value) = get("RUVIO_TLS_CLIENT_AUTH") {
            self.security.tls_client_auth = parse_bool(&value, "RUVIO_TLS_CLIENT_AUTH")?;
        }

        if let Some(value) = get("RUVIO_ACL_FILE") {
            self.security.acl_file = value.into();
        }

        if let Some(value) = get("RUVIO_PROTECTED_MODE") {
            self.security.protected_mode = parse_bool(&value, "RUVIO_PROTECTED_MODE")?;
        }

        if let Some(value) = get("RUVIO_ALLOW_UNAUTHENTICATED") {
            self.security.allow_unauthenticated_public =
                parse_bool(&value, "RUVIO_ALLOW_UNAUTHENTICATED")?;
        }

        if let Some(value) = get("RUVIO_METRICS_TOKEN") {
            self.security.metrics_token = value;
        }

        if let Some(value) = get("RUVIO_REPLICATION_PASSWORD") {
            self.security.replication_password = value;
        }

        apply_usize(&get, "RUVIO_MAXMEMORY", &mut self.memory.max_process_bytes)?;
        apply_usize(
            &get,
            "RUVIO_MAX_SHARD_BYTES",
            &mut self.memory.max_shard_bytes,
        )?;
        apply_usize(
            &get,
            "RUVIO_MEMORY_HEADROOM_BYTES",
            &mut self.memory.reserved_headroom_bytes,
        )?;

        if let Some(value) = get("RUVIO_MAXMEMORY_POLICY") {
            self.memory.eviction_policy = value.parse()?;
        }

        apply_usize(
            &get,
            "RUVIO_MAXMEMORY_SAMPLES",
            &mut self.memory.maxmemory_samples,
        )?;
        apply_usize(
            &get,
            "RUVIO_ACTIVE_EXPIRE_KEYS",
            &mut self.memory.active_expire_keys,
        )?;
        apply_usize(
            &get,
            "RUVIO_ACTIVE_EXPIRE_MAINTENANCE_KEYS",
            &mut self.memory.active_expire_maintenance_keys,
        )?;
        apply_usize(
            &get,
            "RUVIO_HASH_MAX_COMPACT_ENTRIES",
            &mut self.memory.hash_max_compact_entries,
        )?;
        apply_usize(
            &get,
            "RUVIO_HASH_MAX_COMPACT_VALUE_BYTES",
            &mut self.memory.hash_max_compact_value_bytes,
        )?;
        apply_usize(
            &get,
            "RUVIO_SET_MAX_INTSET_ENTRIES",
            &mut self.memory.set_max_intset_entries,
        )?;
        apply_usize(
            &get,
            "RUVIO_LIST_MAX_COMPACT_ENTRIES",
            &mut self.memory.list_max_compact_entries,
        )?;
        apply_usize(
            &get,
            "RUVIO_LIST_MAX_COMPACT_VALUE_BYTES",
            &mut self.memory.list_max_compact_value_bytes,
        )?;

        if let Some(value) = get("RUVIO_NOTIFY_KEYSPACE_EVENTS") {
            self.set_notify_keyspace_events(value)?;
        }

        if let Some(value) = get("RUVIO_METRICS_ADDR") {
            self.metrics_address = if value.is_empty() { None } else { Some(value) };
        }

        apply_usize(&get, "RUVIO_SCRIPT_MAX_BYTES", &mut self.scripts.max_bytes)?;
        apply_usize(
            &get,
            "RUVIO_SCRIPT_MAX_CACHED",
            &mut self.scripts.max_cached,
        )?;
        apply_usize(&get, "RUVIO_SCRIPT_MAX_CALLS", &mut self.scripts.max_calls)?;
        apply_usize(
            &get,
            "RUVIO_SCRIPT_MAX_INSTRUCTIONS",
            &mut self.scripts.max_instructions,
        )?;
        apply_usize(
            &get,
            "RUVIO_SCRIPT_MAX_TIME_MS",
            &mut self.scripts.max_time_ms,
        )?;

        if let Some(value) = get("RUVIO_HOT_KEYS") {
            self.hot_keys = parse_hot_keys(&value)?;
        }

        if let Some(value) = get("RUVIO_REPLICATION_ROLE") {
            self.replication.role = value.parse().map_err(str::to_owned)?;
        }

        if let Some(value) = get("RUVIO_REPLICATION_LISTEN") {
            self.replication.listen = Some(parse_numeric_socket(&value, "replication.listen")?);
        }

        if let Some(value) = get("RUVIO_REPLICATION_PRIMARY") {
            self.set_replication_primary(&value)?;
        }

        Ok(())
    }

    fn set_replication_primary(&mut self, value: &str) -> Result<(), String> {
        let parsed = parse_replication_primary(value)?;

        self.replication.primary = parsed.socket;
        self.replication.primary_name = parsed.name;
        Ok(())
    }

    pub fn apply_overrides(&mut self, overrides: ConfigOverrides) {
        if let Some(value) = overrides.address {
            self.address = value;
        }

        if let Some(value) = overrides.shards {
            self.shards = value;
        }

        if let Some(value) = overrides.advertised_host {
            self.advertised_host = value;
        }

        if let Some(value) = overrides.advertised_port {
            self.advertised_port = Some(value);
        }

        if let Some(value) = overrides.cpu_set {
            self.cpu_set = Some(value);
        }

        if let Some(value) = overrides.shutdown_grace {
            self.shutdown_grace = value;
        }
    }

    fn set_notify_keyspace_events(&mut self, value: String) -> Result<(), String> {
        self.memory.notify_mask = crate::notify::parse_mask(&value)?;
        self.notify_keyspace_events = value;
        Ok(())
    }

    /// `hot_keys` share keys between shards. With one shard every key is already on it, and
    /// its checkpoint has no hot partition to restart from, so the setting is dropped there.
    /// True when it was.
    pub(crate) fn drop_hot_keys_on_one_shard(&mut self) -> bool {
        let dropped = self.shards <= 1 && !self.hot_keys.is_empty();

        if dropped {
            self.hot_keys.clear();
        }
        dropped
    }

    pub(crate) fn checkpoint_partition_count(&self) -> usize {
        if self.hot_keys.is_empty() {
            self.shards
        } else {
            self.shards.saturating_add(1)
        }
    }

    /// `hot_keys` stay unsupported by the sync worker.
    pub(crate) fn sync_worker_supported(&self) -> bool {
        self.hot_keys.is_empty()
    }

    /// Settles `persistence.sync_worker_active` from the mode: `auto` is on only for Linux,
    /// `always` durability, one shard and no `hot_keys` (the measured, gated envelope;
    /// multi-shard throughput is not yet measured).
    pub fn resolve_sync_worker(&mut self) {
        self.persistence.sync_worker_active = match self.persistence.sync_worker {
            SyncWorkerMode::On => true,
            SyncWorkerMode::Off => false,
            SyncWorkerMode::Auto => {
                cfg!(target_os = "linux")
                    // Multi-shard `always` is fsync-latency bound: fsync batching collapses
                    // across shards, so `auto` stays single-shard.
                    && self.shards == 1
                    && self.persistence.profile == DurabilityProfile::Always
                    && self.sync_worker_supported()
            }
        };
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.security.requirepass.len() > usize::from(u16::MAX) {
            return Err("requirepass must be at most 65535 bytes".into());
        }

        self.protocol.validate().map_err(str::to_owned)?;
        self.timeouts.validate().map_err(str::to_owned)?;
        self.connections.validate().map_err(str::to_owned)?;
        self.requests.validate().map_err(str::to_owned)?;
        self.persistence.validate().map_err(str::to_owned)?;
        if self.persistence.sync_worker == SyncWorkerMode::On && !self.sync_worker_supported() {
            return Err("sync_worker = on does not yet support hot_keys".into());
        }
        self.security.validate().map_err(str::to_owned)?;
        self.memory.validate().map_err(str::to_owned)?;
        self.validate_shard_memory_budget()?;
        self.scripts.validate().map_err(str::to_owned)?;

        if self.address.parse::<std::net::SocketAddr>().is_err() {
            return Err("address must be a numeric socket address".into());
        }

        if !crate::bind_is_loopback(&self.address)
            && !self.security.protected_mode
            && !self.security.authentication_enabled()
            && !self.security.allow_unauthenticated_public
        {
            return Err(
                "refusing a public listener with protected mode off and no authentication. Set requirepass, enable protected mode, or set RUVIO_ALLOW_UNAUTHENTICATED=true only on a network you already trust"
                    .into(),
            );
        }

        if let Some(metrics) = &self.metrics_address {
            if metrics.parse::<std::net::SocketAddr>().is_err() {
                return Err("metrics address must be a numeric socket address".into());
            }

            if !crate::bind_is_loopback(metrics) && self.security.metrics_token.is_empty() {
                return Err(
                    "a public metrics listener requires metrics_token or RUVIO_METRICS_TOKEN"
                        .into(),
                );
            }
        }

        if self.replication.role == ReplicationRole::Primary
            && let Some(listen) = self.replication.listen
            && !listen.ip().is_loopback()
            && self.security.replication_secret().is_empty()
        {
            return Err(
                "a public replication listener requires replication_password or requirepass".into(),
            );
        }

        if self.shards == 0 {
            return Err("shard count must be greater than zero".into());
        }

        if self.shards > 64 {
            return Err("shard count must be at most 64".into());
        }

        if self.shards > 1 && self.cpu_set.as_deref() == Some("") {
            return Err("CPU set cannot be empty".into());
        }

        if self.hot_keys.iter().any(|pattern| pattern.is_empty()) {
            return Err("hot_keys entries must be non-empty globs".into());
        }

        if self.shutdown_grace.is_zero() {
            return Err("shutdown grace period must be greater than zero".into());
        }

        if let Some(metrics) = &self.metrics_address {
            let metrics: std::net::SocketAddr = metrics
                .parse()
                .map_err(|_| "metrics address must be a numeric socket address")?;
            let resp: std::net::SocketAddr = self
                .address
                .parse()
                .map_err(|_| "address must be a numeric socket address")?;
            let resp_end =
                u32::from(resp.port()) + u32::try_from(self.shards).unwrap_or(u32::MAX) - 1;
            let metrics_end =
                u32::from(metrics.port()) + u32::try_from(self.shards).unwrap_or(u32::MAX) - 1;

            if resp.ip() == metrics.ip()
                && u32::from(resp.port()) <= metrics_end
                && u32::from(metrics.port()) <= resp_end
            {
                return Err("metrics port range overlaps the RESP listener range".into());
            }
        }

        if self.replication.role != ReplicationRole::Off && !self.persistence.profile.uses_wal() {
            return Err("replication requires a WAL-backed durability profile".into());
        }

        let Some(base) = replication_base(&self.replication)? else {
            return Ok(());
        };

        let resp: SocketAddr = self
            .address
            .parse()
            .map_err(|_| "address must be a numeric socket address")?;

        if listener_ranges_overlap(base, resp, self.shards) {
            return Err("replication port range overlaps the RESP listener range".into());
        }

        let bus_port = u32::from(resp.port()) + 10_000;

        if bus_port <= u32::from(u16::MAX) {
            let bus = SocketAddr::new(base.ip(), bus_port as u16);

            if listener_ranges_overlap(base, bus, self.shards) {
                return Err("replication port range overlaps the cluster bus port range".into());
            }
        }

        if let Some(metrics) = &self.metrics_address {
            let metrics: SocketAddr = metrics
                .parse()
                .map_err(|_| "metrics address must be a numeric socket address")?;

            if listener_ranges_overlap(base, metrics, self.shards) {
                return Err("replication port range overlaps the metrics listener range".into());
            }
        }

        Ok(())
    }

    pub fn metrics_socket_addr(&self) -> Result<Option<std::net::SocketAddr>, String> {
        self.metrics_address
            .as_deref()
            .map(|address| {
                address
                    .parse()
                    .map_err(|_| "metrics address must be a numeric socket address".into())
            })
            .transpose()
    }

    pub fn introspect(&self) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> = vec![
            ("address".into(), self.address.clone()),
            ("advertised-host".into(), self.advertised_host.clone()),
            (
                "advertised-port".into(),
                self.advertised_port
                    .map(|port| port.to_string())
                    .unwrap_or_default(),
            ),
            ("appendonly".into(), "no".into()),
            ("cpu-set".into(), self.cpu_set.clone().unwrap_or_default()),
            (
                "data-dir".into(),
                self.persistence.data_dir.display().to_string(),
            ),
            (
                "dir".into(),
                self.persistence.data_dir.display().to_string(),
            ),
            ("databases".into(), self.protocol.databases.to_string()),
            ("durability".into(), self.persistence.profile.to_string()),
            ("idle-timeout-ms".into(), millis(self.timeouts.idle)),
            (
                "max-arguments".into(),
                self.protocol.max_arguments.to_string(),
            ),
            (
                "max-command-bytes".into(),
                self.protocol.max_command_bytes.to_string(),
            ),
            (
                "max-connection-buffer-bytes".into(),
                self.protocol.max_connection_buffer_bytes.to_string(),
            ),
            (
                "max-connections".into(),
                self.connections.max_global.to_string(),
            ),
            (
                "max-connections-per-ip".into(),
                self.connections.max_per_ip.to_string(),
            ),
            (
                "max-connections-per-user".into(),
                self.connections.max_per_user.to_string(),
            ),
            (
                "max-commands-per-second".into(),
                self.connections.max_commands_per_second.to_string(),
            ),
            (
                "max-key-bytes".into(),
                self.protocol.max_key_bytes.to_string(),
            ),
            (
                "max-output-buffer-bytes".into(),
                self.requests.max_output_buffer_bytes.to_string(),
            ),
            (
                "max-pipeline-commands".into(),
                self.protocol.max_pipeline_commands.to_string(),
            ),
            (
                "max-queued-requests".into(),
                self.requests.max_queued_requests.to_string(),
            ),
            (
                "max-shard-bytes".into(),
                self.memory.max_shard_bytes.to_string(),
            ),
            (
                "max-value-bytes".into(),
                self.protocol.max_value_bytes.to_string(),
            ),
            (
                "maxmemory".into(),
                self.memory.max_process_bytes.to_string(),
            ),
            (
                "maxmemory-policy".into(),
                self.memory.eviction_policy.as_str().into(),
            ),
            (
                "maxmemory-samples".into(),
                self.memory.maxmemory_samples.to_string(),
            ),
            (
                "active-expire-keys".into(),
                self.memory.active_expire_keys.to_string(),
            ),
            (
                "active-expire-maintenance-keys".into(),
                self.memory.active_expire_maintenance_keys.to_string(),
            ),
            (
                "notify-keyspace-events".into(),
                self.notify_keyspace_events.clone(),
            ),
            (
                "hash-max-compact-entries".into(),
                self.memory.hash_max_compact_entries.to_string(),
            ),
            (
                "hash-max-compact-value-bytes".into(),
                self.memory.hash_max_compact_value_bytes.to_string(),
            ),
            (
                "set-max-intset-entries".into(),
                self.memory.set_max_intset_entries.to_string(),
            ),
            (
                "list-max-compact-entries".into(),
                self.memory.list_max_compact_entries.to_string(),
            ),
            (
                "list-max-compact-value-bytes".into(),
                self.memory.list_max_compact_value_bytes.to_string(),
            ),
            (
                "memory-headroom-bytes".into(),
                self.memory.reserved_headroom_bytes.to_string(),
            ),
            (
                "metrics-address".into(),
                self.metrics_address.clone().unwrap_or_default(),
            ),
            ("metrics-token".into(), redact(&self.security.metrics_token)),
            (
                "protected-mode".into(),
                yes_no(self.security.protected_mode),
            ),
            ("read-timeout-ms".into(), millis(self.timeouts.read)),
            (
                "replication-password".into(),
                redact(&self.security.replication_password),
            ),
            (
                "requirepass".into(),
                self.security.redacted_requirepass().into(),
            ),
            (
                "acl-file".into(),
                self.security.acl_file.display().to_string(),
            ),
            (
                "acl-users".into(),
                self.security
                    .effective_users()
                    .into_iter()
                    .map(|user| String::from_utf8_lossy(&user.name).into_owned())
                    .collect::<Vec<_>>()
                    .join(" "),
            ),
            (
                "tls-cert".into(),
                self.security.tls_cert.display().to_string(),
            ),
            (
                "tls-key".into(),
                self.security.tls_key.display().to_string(),
            ),
            (
                "tls-client-ca".into(),
                self.security.tls_client_ca.display().to_string(),
            ),
            (
                "tls-client-auth".into(),
                yes_no(self.security.tls_client_auth),
            ),
            ("save".into(), String::new()),
            ("port-mode".into(), self.port_mode.as_str().into()),
            ("shards".into(), self.shards.to_string()),
            ("shutdown-grace-ms".into(), millis(self.shutdown_grace)),
            (
                "snapshot-interval-ms".into(),
                millis(self.persistence.snapshot_interval),
            ),
            (
                "snapshot-journal-bytes".into(),
                self.persistence.snapshot_journal_bytes.to_string(),
            ),
            (
                "snapshot-max-bytes".into(),
                self.persistence.snapshot_max_bytes.to_string(),
            ),
            (
                "snapshot-min-mutations".into(),
                self.persistence.snapshot_min_mutations.to_string(),
            ),
            (
                "cdc-retain-bytes".into(),
                self.persistence.cdc_retain_bytes.to_string(),
            ),
            (
                "replica-retain-max-bytes".into(),
                self.persistence.replica_retain_max_bytes.to_string(),
            ),
            (
                "wal-segment-bytes".into(),
                self.persistence.wal_segment_bytes.to_string(),
            ),
            (
                "wal-sync-interval-ms".into(),
                millis(self.persistence.sync_interval),
            ),
            (
                "sync-worker".into(),
                self.persistence.sync_worker.to_string(),
            ),
            (
                "sync-worker-active".into(),
                yes_no(self.persistence.sync_worker_active),
            ),
            ("write-timeout-ms".into(), millis(self.timeouts.write)),
            (
                "script-max-bytes".into(),
                self.scripts.max_bytes.to_string(),
            ),
            (
                "script-max-cached".into(),
                self.scripts.max_cached.to_string(),
            ),
            (
                "script-max-calls".into(),
                self.scripts.max_calls.to_string(),
            ),
            (
                "script-max-instructions".into(),
                self.scripts.max_instructions.to_string(),
            ),
            (
                "script-max-time-ms".into(),
                self.scripts.max_time_ms.to_string(),
            ),
            (
                "allow-unauthenticated-public".into(),
                yes_no(self.security.allow_unauthenticated_public),
            ),
            (
                "hot-keys".into(),
                self.hot_keys
                    .iter()
                    .map(|pattern| String::from_utf8_lossy(pattern).into_owned())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            ("latency-histograms".into(), yes_no(self.latency_histograms)),
            ("key-stats".into(), yes_no(self.key_stats)),
            (
                "slowlog-log-slower-than".into(),
                self.slowlog_threshold_micros.to_string(),
            ),
            ("slowlog-max-len".into(), self.slowlog_max_len.to_string()),
            (
                "replication-role".into(),
                match self.replication.role {
                    ReplicationRole::Off => "off",
                    ReplicationRole::Primary => "primary",
                    ReplicationRole::Replica => "replica",
                }
                .into(),
            ),
            (
                "replication-listen".into(),
                self.replication
                    .listen
                    .map(|address| address.to_string())
                    .unwrap_or_default(),
            ),
            (
                "replication-primary".into(),
                match (&self.replication.primary_name, self.replication.primary) {
                    (Some((host, port)), _) => format!("{host}:{port}"),
                    (None, Some(address)) => address.to_string(),
                    (None, None) => String::new(),
                },
            ),
        ];

        pairs.sort_by(|left, right| left.0.cmp(&right.0));
        pairs
    }

    pub fn config_get(&self, pattern: &[u8]) -> Vec<(String, String)> {
        let pattern: Vec<u8> = pattern.iter().map(u8::to_ascii_lowercase).collect();

        self.introspect()
            .into_iter()
            .filter(|(name, _)| crate::protocol::glob_match(&pattern, name.as_bytes()))
            .collect()
    }
}

fn millis(duration: Duration) -> String {
    duration.as_millis().to_string()
}

fn parse_advertised_port(value: &str) -> Result<u16, String> {
    match value.parse::<u16>() {
        Ok(port) if port > 0 => Ok(port),
        _ => Err("RUVIO_ADVERTISE_PORT must be a port from 1 to 65535".into()),
    }
}

fn yes_no(value: bool) -> String {
    if value { "yes" } else { "no" }.into()
}

fn redact(secret: &str) -> String {
    if secret.is_empty() {
        String::new()
    } else {
        "***".into()
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:6379".into(),
            shards: 1,
            port_mode: PortMode::default(),
            advertised_host: "127.0.0.1".into(),
            advertised_port: None,
            cpu_set: None,
            shutdown_grace: Duration::from_secs(30),
            latency_histograms: true,
            slowlog_threshold_micros: 10_000,
            slowlog_max_len: 128,
            key_stats: true,
            protocol: ProtocolLimits::default(),
            timeouts: ConnectionTimeouts::default(),
            connections: ConnectionLimits::default(),
            requests: RequestLimits::default(),
            persistence: PersistenceConfig::default(),
            security: SecurityConfig::default(),
            memory: MemoryLimits::default(),
            metrics_address: None,
            scripts: ScriptLimits::default(),
            hot_keys: Vec::new(),
            notify_keyspace_events: String::new(),
            replication: ReplicationConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfigOverrides {
    pub address: Option<String>,
    pub shards: Option<usize>,
    pub advertised_host: Option<String>,
    pub advertised_port: Option<u16>,
    pub cpu_set: Option<String>,
    pub shutdown_grace: Option<Duration>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    address: Option<String>,
    shards: Option<usize>,
    port_mode: Option<String>,
    advertised_host: Option<String>,
    advertised_port: Option<u16>,
    cpu_set: Option<String>,
    shutdown_grace_ms: Option<u64>,
    latency_histograms: Option<bool>,
    slowlog_threshold_micros: Option<i64>,
    slowlog_max_len: Option<usize>,
    key_stats: Option<bool>,
    protocol: ProtocolFileConfig,
    timeouts: TimeoutFileConfig,
    connections: ConnectionFileConfig,
    requests: RequestFileConfig,
    persistence: PersistenceFileConfig,
    security: SecurityFileConfig,
    memory: MemoryFileConfig,
    metrics_address: Option<String>,
    scripts: ScriptFileConfig,
    hot_keys: Option<Vec<String>>,
    notify_keyspace_events: Option<String>,
    replication: ReplicationFileConfig,
}

impl FileConfig {
    fn apply(self, config: &mut AppConfig) -> Result<(), String> {
        config.apply_overrides(ConfigOverrides {
            address: self.address,
            shards: self.shards,
            advertised_host: self.advertised_host,
            advertised_port: self.advertised_port,
            cpu_set: self.cpu_set,
            shutdown_grace: self.shutdown_grace_ms.map(Duration::from_millis),
        });

        if let Some(value) = self.latency_histograms {
            config.latency_histograms = value;
        }

        if let Some(value) = self.slowlog_threshold_micros {
            config.slowlog_threshold_micros = value;
        }

        if let Some(value) = self.slowlog_max_len {
            config.slowlog_max_len = value;
        }

        if let Some(value) = self.key_stats {
            config.key_stats = value;
        }

        if let Some(mode) = self.port_mode {
            config.port_mode = PortMode::parse(&mode)?;
        }

        self.protocol.apply(&mut config.protocol);
        self.timeouts.apply(&mut config.timeouts);
        self.connections.apply(&mut config.connections);
        self.requests.apply(&mut config.requests);
        self.persistence.apply(&mut config.persistence);
        self.security.apply(&mut config.security)?;
        self.memory.apply(&mut config.memory)?;
        self.scripts.apply(&mut config.scripts);

        if let Some(value) = self.metrics_address {
            config.metrics_address = if value.is_empty() { None } else { Some(value) };
        }

        if let Some(value) = self.hot_keys {
            config.hot_keys = value
                .into_iter()
                .map(|pattern| pattern.into_bytes())
                .collect();
        }

        if let Some(value) = self.notify_keyspace_events {
            config.set_notify_keyspace_events(value)?;
        }

        self.replication.apply(&mut config.replication)?;
        Ok(())
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProtocolFileConfig {
    max_arguments: Option<usize>,
    max_key_bytes: Option<usize>,
    max_value_bytes: Option<usize>,
    max_command_bytes: Option<usize>,
    max_pipeline_commands: Option<usize>,
    max_connection_buffer_bytes: Option<usize>,
    databases: Option<usize>,
}

impl ProtocolFileConfig {
    fn apply(self, limits: &mut ProtocolLimits) {
        assign(&mut limits.max_arguments, self.max_arguments);
        assign(&mut limits.max_key_bytes, self.max_key_bytes);
        assign(&mut limits.max_value_bytes, self.max_value_bytes);
        assign(&mut limits.max_command_bytes, self.max_command_bytes);
        assign(
            &mut limits.max_pipeline_commands,
            self.max_pipeline_commands,
        );
        assign(
            &mut limits.max_connection_buffer_bytes,
            self.max_connection_buffer_bytes,
        );
        assign(&mut limits.databases, self.databases);
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TimeoutFileConfig {
    idle_ms: Option<u64>,
    read_ms: Option<u64>,
    write_ms: Option<u64>,
}

impl TimeoutFileConfig {
    fn apply(self, timeouts: &mut ConnectionTimeouts) {
        assign_duration(&mut timeouts.idle, self.idle_ms);
        assign_duration(&mut timeouts.read, self.read_ms);
        assign_duration(&mut timeouts.write, self.write_ms);
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ConnectionFileConfig {
    max_global: Option<usize>,
    max_per_ip: Option<usize>,
    max_per_user: Option<usize>,
    max_commands_per_second: Option<u64>,
}

impl ConnectionFileConfig {
    fn apply(self, limits: &mut ConnectionLimits) {
        assign(&mut limits.max_global, self.max_global);
        assign(&mut limits.max_per_ip, self.max_per_ip);
        assign(&mut limits.max_per_user, self.max_per_user);
        assign(
            &mut limits.max_commands_per_second,
            self.max_commands_per_second,
        );
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RequestFileConfig {
    max_queued_requests: Option<usize>,
    max_output_buffer_bytes: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct PersistenceFileConfig {
    profile: Option<DurabilityProfile>,
    data_dir: Option<PathBuf>,
    wal_segment_bytes: Option<u64>,
    sync_interval_ms: Option<u64>,
    sync_worker: Option<SyncWorkerMode>,
    /// Legacy spelling of `sync_worker = "on" | "off"`.
    experimental_sync_worker: Option<bool>,
    snapshot_max_bytes: Option<u64>,
    snapshot_journal_bytes: Option<usize>,
    snapshot_interval_ms: Option<u64>,
    snapshot_min_mutations: Option<u64>,
    cdc_retain_bytes: Option<u64>,
    replica_retain_max_bytes: Option<u64>,
}

impl PersistenceFileConfig {
    fn apply(self, config: &mut PersistenceConfig) {
        assign(&mut config.profile, self.profile);
        assign(&mut config.data_dir, self.data_dir);
        assign(&mut config.wal_segment_bytes, self.wal_segment_bytes);
        assign_duration(&mut config.sync_interval, self.sync_interval_ms);
        if let Some(legacy) = self.experimental_sync_worker {
            config.sync_worker = if legacy {
                SyncWorkerMode::On
            } else {
                SyncWorkerMode::Off
            };
        }
        assign(&mut config.sync_worker, self.sync_worker);
        assign(&mut config.snapshot_max_bytes, self.snapshot_max_bytes);
        assign(
            &mut config.snapshot_journal_bytes,
            self.snapshot_journal_bytes,
        );
        assign_duration(&mut config.snapshot_interval, self.snapshot_interval_ms);
        assign(
            &mut config.snapshot_min_mutations,
            self.snapshot_min_mutations,
        );
        assign(&mut config.cdc_retain_bytes, self.cdc_retain_bytes);
        assign(
            &mut config.replica_retain_max_bytes,
            self.replica_retain_max_bytes,
        );
    }
}

impl RequestFileConfig {
    fn apply(self, limits: &mut RequestLimits) {
        assign(&mut limits.max_queued_requests, self.max_queued_requests);
        assign(
            &mut limits.max_output_buffer_bytes,
            self.max_output_buffer_bytes,
        );
    }
}

fn assign<T>(target: &mut T, value: Option<T>) {
    if let Some(value) = value {
        *target = value;
    }
}

fn assign_duration(target: &mut Duration, millis: Option<u64>) {
    if let Some(millis) = millis {
        *target = Duration::from_millis(millis);
    }
}

fn apply_string(get: &impl Fn(&str) -> Option<String>, name: &str, target: &mut String) {
    if let Some(value) = get(name) {
        *target = value;
    }
}

fn apply_usize(
    get: &impl Fn(&str) -> Option<String>,
    name: &str,
    target: &mut usize,
) -> Result<(), String> {
    if let Some(value) = get(name) {
        *target = value
            .parse()
            .map_err(|_| format!("{name} must be an unsigned integer"))?;
    }

    Ok(())
}

fn apply_u64(
    get: &impl Fn(&str) -> Option<String>,
    name: &str,
    target: &mut u64,
) -> Result<(), String> {
    if let Some(value) = get(name) {
        *target = value
            .parse()
            .map_err(|_| format!("{name} must be an unsigned integer"))?;
    }

    Ok(())
}

fn apply_duration(
    get: &impl Fn(&str) -> Option<String>,
    name: &str,
    target: &mut Duration,
) -> Result<(), String> {
    if let Some(value) = get(name) {
        let millis = value
            .parse()
            .map_err(|_| format!("{name} must be an unsigned integer"))?;

        *target = Duration::from_millis(millis);
    }

    Ok(())
}

fn replication_base(replication: &ReplicationConfig) -> Result<Option<SocketAddr>, String> {
    match replication.role {
        ReplicationRole::Off => Ok(None),
        ReplicationRole::Primary => match replication.listen {
            Some(address) => Ok(Some(address)),
            None => Err("replication primary requires replication.listen".into()),
        },
        ReplicationRole::Replica => match (replication.primary, replication.primary_name.is_some())
        {
            (Some(address), _) => Ok(Some(address)),
            (None, true) => Ok(None),
            (None, false) => Err("replication replica requires replication.primary".into()),
        },
    }
}

fn listener_ranges_overlap(left: SocketAddr, right: SocketAddr, shards: usize) -> bool {
    if left.ip() != right.ip() {
        return false;
    }

    let left_end = u32::from(left.port()) + u32::try_from(shards).unwrap_or(u32::MAX) - 1;
    let right_end = u32::from(right.port()) + u32::try_from(shards).unwrap_or(u32::MAX) - 1;

    u32::from(left.port()) <= right_end && u32::from(right.port()) <= left_end
}

struct ParsedPrimary {
    socket: Option<SocketAddr>,
    name: Option<(String, u16)>,
}

fn parse_replication_primary(value: &str) -> Result<ParsedPrimary, String> {
    if let Ok(address) = value.parse::<SocketAddr>() {
        return Ok(ParsedPrimary {
            socket: Some(address),
            name: None,
        });
    }

    let Some((host, port)) = value.rsplit_once(':') else {
        return Err("replication.primary must be host:port".into());
    };

    if host.is_empty() {
        return Err("replication.primary must be host:port".into());
    }

    let port = port
        .parse::<u16>()
        .map_err(|_| "replication.primary must be host:port".to_string())?;

    Ok(ParsedPrimary {
        socket: None,
        name: Some((host.to_string(), port)),
    })
}

fn parse_numeric_socket(value: &str, field: &str) -> Result<SocketAddr, String> {
    value
        .parse()
        .map_err(|_| format!("{field} must be a numeric socket address"))
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ReplicationFileConfig {
    role: Option<ReplicationRole>,
    listen: Option<String>,
    primary: Option<String>,
}

impl ReplicationFileConfig {
    fn apply(self, replication: &mut ReplicationConfig) -> Result<(), String> {
        let listen = match self.listen {
            Some(value) => Some(parse_numeric_socket(&value, "replication.listen")?),
            None => None,
        };

        let primary = match self.primary {
            Some(value) => Some(parse_replication_primary(&value)?),
            None => None,
        };

        if let Some(role) = self.role {
            replication.role = role;
        }

        if let Some(listen) = listen {
            replication.listen = Some(listen);
        }

        if let Some(parsed) = primary {
            replication.primary = parsed.socket;
            replication.primary_name = parsed.name;
        }

        Ok(())
    }
}

fn parse_hot_keys(value: &str) -> Result<Vec<Vec<u8>>, String> {
    let mut patterns = Vec::new();

    for part in value.split(',') {
        let pattern = part.trim();

        if pattern.is_empty() {
            return Err("hot_keys entries must be non-empty globs".into());
        }

        patterns.push(pattern.as_bytes().to_vec());
    }

    Ok(patterns)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestLimits {
    pub max_queued_requests: usize,
    pub max_output_buffer_bytes: usize,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ScriptFileConfig {
    max_bytes: Option<usize>,
    max_cached: Option<usize>,
    max_calls: Option<usize>,
    max_instructions: Option<usize>,
    max_time_ms: Option<usize>,
}

impl ScriptFileConfig {
    fn apply(self, limits: &mut ScriptLimits) {
        assign(&mut limits.max_bytes, self.max_bytes);
        assign(&mut limits.max_cached, self.max_cached);
        assign(&mut limits.max_calls, self.max_calls);
        assign(&mut limits.max_instructions, self.max_instructions);
        assign(&mut limits.max_time_ms, self.max_time_ms);
    }
}

/// Longest `scripts.max_time_ms`: one hour.
const MAX_SCRIPT_TIME_MS: usize = 3_600_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScriptLimits {
    pub max_bytes: usize,
    pub max_cached: usize,
    pub max_calls: usize,
    pub max_instructions: usize,
    /// Wall-clock time one script may run, in milliseconds.
    pub max_time_ms: usize,
}

impl ScriptLimits {
    pub fn validate(self) -> Result<Self, &'static str> {
        if self.max_bytes == 0 {
            return Err("script byte limit must be greater than zero");
        }

        if self.max_cached == 0 {
            return Err("script cache limit must be greater than zero");
        }

        if self.max_calls == 0 {
            return Err("script call limit must be greater than zero");
        }

        if self.max_instructions == 0 {
            return Err("script instruction limit must be greater than zero");
        }

        if self.max_time_ms == 0 {
            return Err("script time limit must be greater than zero");
        }

        if self.max_time_ms > MAX_SCRIPT_TIME_MS {
            return Err("script time limit must be at most 3600000 ms");
        }

        Ok(self)
    }
}

impl Default for ScriptLimits {
    fn default() -> Self {
        Self {
            max_bytes: 16 * 1024,
            max_cached: 1_024,
            max_calls: 1_024,
            max_instructions: 1_000_000,
            max_time_ms: 1_000,
        }
    }
}

impl RequestLimits {
    pub fn validate(self) -> Result<Self, &'static str> {
        if self.max_queued_requests == 0 {
            return Err("request queue limit must be greater than zero");
        }

        if self.max_output_buffer_bytes == 0 {
            return Err("output buffer limit must be greater than zero");
        }

        Ok(self)
    }
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self {
            max_queued_requests: 1_024,
            // Replies are sent as a pipeline produces them, so this bounds what a client has
            // not read yet, not the size of a pipeline's answer.
            max_output_buffer_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionLimits {
    pub max_global: usize,
    pub max_per_ip: usize,
    pub max_per_user: usize,
    pub max_commands_per_second: u64,
}

impl ConnectionLimits {
    pub fn validate(self) -> Result<Self, &'static str> {
        if self.max_global == 0 {
            return Err("global connection limit must be greater than zero");
        }

        if self.max_per_ip == 0 {
            return Err("per-IP connection limit must be greater than zero");
        }

        Ok(self)
    }

    /// Lowers a nested cap that sits above the global cap. A global limit of 8
    /// with the default per-IP limit of 256 used to refuse startup.
    pub fn clamp_to_global(&mut self) -> Vec<String> {
        let mut notes = Vec::new();

        if self.max_per_ip > self.max_global {
            notes.push(format!(
                "per-IP connection limit {} is above the global limit {}; using {}",
                self.max_per_ip, self.max_global, self.max_global
            ));
            self.max_per_ip = self.max_global;
        }

        if self.max_per_user > self.max_global {
            notes.push(format!(
                "per-user connection limit {} is above the global limit {}; using {}",
                self.max_per_user, self.max_global, self.max_global
            ));
            self.max_per_user = self.max_global;
        }

        notes
    }
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        Self {
            max_global: 10_000,
            max_per_ip: 256,
            max_per_user: 0,
            max_commands_per_second: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionTimeouts {
    pub idle: Duration,
    pub read: Duration,
    pub write: Duration,
}

impl ConnectionTimeouts {
    pub fn validate(self) -> Result<Self, &'static str> {
        if self.idle.is_zero() {
            return Err("idle timeout must be greater than zero");
        }

        if self.read.is_zero() {
            return Err("read timeout must be greater than zero");
        }

        if self.write.is_zero() {
            return Err("write timeout must be greater than zero");
        }

        Ok(self)
    }
}

impl Default for ConnectionTimeouts {
    fn default() -> Self {
        Self {
            idle: Duration::from_secs(300),
            read: Duration::from_secs(30),
            write: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EvictionPolicy {
    #[default]
    NoEviction,
    AllKeysLru,
    AllKeysLfu,
    VolatileLru,
    VolatileTtl,
}

impl EvictionPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllKeysLru => "allkeys-lru",
            Self::AllKeysLfu => "allkeys-lfu",
            Self::VolatileLru => "volatile-lru",
            Self::VolatileTtl => "volatile-ttl",
        }
    }

    pub const fn evicts(self) -> bool {
        !matches!(self, Self::NoEviction)
    }

    pub const fn tracks_recency(self) -> bool {
        matches!(
            self,
            Self::AllKeysLru | Self::AllKeysLfu | Self::VolatileLru
        )
    }

    pub const fn volatile_only(self) -> bool {
        matches!(self, Self::VolatileLru | Self::VolatileTtl)
    }
}

impl std::str::FromStr for EvictionPolicy {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "noeviction" => Ok(Self::NoEviction),
            "allkeys-lru" => Ok(Self::AllKeysLru),
            "allkeys-lfu" => Ok(Self::AllKeysLfu),
            "volatile-lru" => Ok(Self::VolatileLru),
            "volatile-ttl" => Ok(Self::VolatileTtl),
            _ => Err(format!(
                "unknown maxmemory-policy {value:?}; expected noeviction, allkeys-lru, allkeys-lfu, volatile-lru, or volatile-ttl"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryLimits {
    pub max_shard_bytes: usize,
    pub max_process_bytes: usize,
    pub reserved_headroom_bytes: usize,
    pub eviction_policy: EvictionPolicy,
    pub maxmemory_samples: usize,
    pub active_expire_keys: usize,
    pub active_expire_maintenance_keys: usize,
    pub hash_max_compact_entries: usize,
    pub hash_max_compact_value_bytes: usize,
    /// Sets of canonical integers up to this many members stay a sorted integer array.
    pub set_max_intset_entries: usize,
    /// Lists up to this many items, each at most `list_max_compact_value_bytes`, stay in
    /// one packed buffer; past that they become a chain of packed nodes.
    pub list_max_compact_entries: usize,
    pub list_max_compact_value_bytes: usize,
    pub notify_mask: u32,
}

impl MemoryLimits {
    pub fn validate(self) -> Result<Self, &'static str> {
        if self.max_shard_bytes > 0 && self.reserved_headroom_bytes >= self.max_shard_bytes {
            return Err("reserved memory headroom must be smaller than the shard memory limit");
        }

        if self.max_process_bytes > 0
            && self.max_shard_bytes > 0
            && self.max_shard_bytes > self.max_process_bytes
        {
            return Err("shard memory limit cannot exceed process memory limit");
        }

        if !(1..=64).contains(&self.maxmemory_samples) {
            return Err("maxmemory-samples must be between 1 and 64");
        }

        if !(1..=1_024).contains(&self.active_expire_keys) {
            return Err("active-expire-keys must be between 1 and 1024");
        }

        if !(1..=1_024).contains(&self.active_expire_maintenance_keys) {
            return Err("active-expire-maintenance-keys must be between 1 and 1024");
        }

        if !(1..=4_096).contains(&self.hash_max_compact_entries) {
            return Err("hash-max-compact-entries must be between 1 and 4096");
        }

        if !(1..=65_536).contains(&self.hash_max_compact_value_bytes) {
            return Err("hash-max-compact-value-bytes must be between 1 and 65536");
        }

        if self.set_max_intset_entries > 4_096 {
            return Err("set-max-intset-entries must be between 0 and 4096");
        }

        if !(1..=4_096).contains(&self.list_max_compact_entries) {
            return Err("list-max-compact-entries must be between 1 and 4096");
        }

        if !(1..=65_536).contains(&self.list_max_compact_value_bytes) {
            return Err("list-max-compact-value-bytes must be between 1 and 65536");
        }

        Ok(self)
    }

    pub fn shard_admission_budget(self) -> Option<usize> {
        (self.max_shard_bytes > 0).then_some(
            self.max_shard_bytes
                .saturating_sub(self.reserved_headroom_bytes),
        )
    }
}

impl Default for MemoryLimits {
    fn default() -> Self {
        Self {
            max_shard_bytes: 0,
            max_process_bytes: 0,
            reserved_headroom_bytes: 16 * 1024 * 1024,
            eviction_policy: EvictionPolicy::NoEviction,
            maxmemory_samples: 16,
            active_expire_keys: 16,
            active_expire_maintenance_keys: 16,
            hash_max_compact_entries: 64,
            hash_max_compact_value_bytes: 64,
            set_max_intset_entries: 512,
            list_max_compact_entries: 128,
            list_max_compact_value_bytes: 64,
            notify_mask: 0,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SecurityFileConfig {
    requirepass: Option<String>,
    protected_mode: Option<bool>,
    allow_unauthenticated_public: Option<bool>,
    metrics_token: Option<String>,
    replication_password: Option<String>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_client_ca: Option<PathBuf>,
    tls_client_auth: Option<bool>,
    acl_file: Option<PathBuf>,
    users: Vec<AclUserFile>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AclUserFile {
    name: String,
    rules: String,
}

impl SecurityFileConfig {
    fn apply(self, security: &mut SecurityConfig) -> Result<(), String> {
        if let Some(value) = self.requirepass {
            security.requirepass = value;
        }

        if let Some(value) = self.protected_mode {
            security.protected_mode = value;
        }

        if let Some(value) = self.allow_unauthenticated_public {
            security.allow_unauthenticated_public = value;
        }

        if let Some(value) = self.metrics_token {
            security.metrics_token = value;
        }

        if let Some(value) = self.replication_password {
            security.replication_password = value;
        }

        assign(&mut security.tls_cert, self.tls_cert);
        assign(&mut security.tls_key, self.tls_key);
        assign(&mut security.tls_client_ca, self.tls_client_ca);

        if let Some(value) = self.tls_client_auth {
            security.tls_client_auth = value;
        }

        assign(&mut security.acl_file, self.acl_file);

        if !self.users.is_empty() {
            security.users = self
                .users
                .into_iter()
                .map(|user| crate::acl::AclUser::parse(&user.name, &user.rules))
                .collect::<Result<Vec<_>, _>>()?;
        }

        Ok(())
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct MemoryFileConfig {
    max_shard_bytes: Option<usize>,
    max_process_bytes: Option<usize>,
    reserved_headroom_bytes: Option<usize>,
    maxmemory_policy: Option<String>,
    maxmemory_samples: Option<usize>,
    active_expire_keys: Option<usize>,
    active_expire_maintenance_keys: Option<usize>,
    hash_max_compact_entries: Option<usize>,
    hash_max_compact_value_bytes: Option<usize>,
    set_max_intset_entries: Option<usize>,
    list_max_compact_entries: Option<usize>,
    list_max_compact_value_bytes: Option<usize>,
}

impl MemoryFileConfig {
    fn apply(self, memory: &mut MemoryLimits) -> Result<(), String> {
        assign(&mut memory.max_shard_bytes, self.max_shard_bytes);
        assign(&mut memory.max_process_bytes, self.max_process_bytes);
        assign(
            &mut memory.reserved_headroom_bytes,
            self.reserved_headroom_bytes,
        );

        if let Some(value) = self.maxmemory_policy {
            memory.eviction_policy = value.parse()?;
        }

        assign(&mut memory.maxmemory_samples, self.maxmemory_samples);
        assign(&mut memory.active_expire_keys, self.active_expire_keys);
        assign(
            &mut memory.active_expire_maintenance_keys,
            self.active_expire_maintenance_keys,
        );
        assign(
            &mut memory.hash_max_compact_entries,
            self.hash_max_compact_entries,
        );
        assign(
            &mut memory.hash_max_compact_value_bytes,
            self.hash_max_compact_value_bytes,
        );
        assign(
            &mut memory.set_max_intset_entries,
            self.set_max_intset_entries,
        );
        assign(
            &mut memory.list_max_compact_entries,
            self.list_max_compact_entries,
        );
        assign(
            &mut memory.list_max_compact_value_bytes,
            self.list_max_compact_value_bytes,
        );
        Ok(())
    }
}

const CONFIG_HEADER: &str = "\
# Effective settings. Ruvio reads this file once, at process start.
# Editing it while the process is running does not change the live server.
# Restart to pick up edits.
# RUVIO_* environment variables and CLI flags override this file; they are
# written back here only when the server starts with --write-config.

";

pub fn resolve_config_path(explicit: Option<&Path>) -> PathBuf {
    if let Some(path) = explicit {
        return path.to_path_buf();
    }

    if let Some(path) = env::var("RUVIO_CONFIG")
        .ok()
        .filter(|value| !value.is_empty())
    {
        return PathBuf::from(path);
    }

    if let Ok(executable) = env::current_exe()
        && let Some(directory) = executable.parent()
    {
        let beside = directory.join("ruvio.toml");

        if beside.exists() {
            return beside;
        }
    }

    let system = PathBuf::from("/etc/ruvio/ruvio.toml");

    if system.exists() {
        return system;
    }

    PathBuf::from("ruvio.toml")
}

/// The file written when no config exists. It must hold the code defaults: a
/// leftover from a local run here would ship to every new install and image.
const DEFAULT_CONFIG: &str = include_str!("../ruvio.example.toml");

pub fn ensure_config_file(path: &Path) -> Result<(), String> {
    if path.exists() {
        return Ok(());
    }

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }

    fs::write(path, DEFAULT_CONFIG)
        .map_err(|error| format!("failed to write config {}: {error}", path.display()))
}

impl AppConfig {
    /// Writes settings that differ from the file that was loaded. The process
    /// does not watch the file after this; a later edit applies on the next start.
    pub fn write_overrides(&self, path: &Path, loaded: &Self) -> Result<(), String> {
        if self == loaded {
            return Ok(());
        }

        let current = match fs::read_to_string(path) {
            Ok(current) => current,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => {
                return Err(format!("failed to read config {}: {error}", path.display()));
            }
        };
        // Rewriting a file that does not parse would drop every setting in it.
        let mut table: toml::Table = toml::from_str(&current).map_err(|error| {
            format!(
                "refusing to rewrite config {}: it does not parse: {error}",
                path.display()
            )
        })?;

        overlay_config(&mut table, loaded, self);

        let body = toml::to_string_pretty(&table)
            .map_err(|error| format!("failed to render config: {error}"))?;

        // Owner-only: the file can hold requirepass, metrics_token and the replication
        // password, as the ACL file does.
        crate::persistence::write_file_atomically(
            path,
            format!("{CONFIG_HEADER}{body}").as_bytes(),
            true,
        )
        .map_err(|error| format!("failed to write config {}: {error}", path.display()))
    }
}

fn overlay_config(table: &mut toml::Table, loaded: &AppConfig, effective: &AppConfig) {
    if loaded.address != effective.address {
        set_toml(table, &["address"], text(&effective.address));
    }

    if loaded.shards != effective.shards {
        set_toml(table, &["shards"], number(effective.shards as u64));
    }

    if loaded.advertised_host != effective.advertised_host {
        set_toml(
            table,
            &["advertised_host"],
            text(&effective.advertised_host),
        );
    }

    sync_optional_number(
        table,
        &["advertised_port"],
        loaded.advertised_port.map(u64::from),
        effective.advertised_port.map(u64::from),
    );
    sync_optional_text(table, &["cpu_set"], &loaded.cpu_set, &effective.cpu_set);

    if loaded.shutdown_grace != effective.shutdown_grace {
        set_toml(
            table,
            &["shutdown_grace_ms"],
            number(effective.shutdown_grace.as_millis() as u64),
        );
    }

    if loaded.slowlog_threshold_micros != effective.slowlog_threshold_micros {
        set_toml(
            table,
            &["slowlog_threshold_micros"],
            toml::Value::Integer(effective.slowlog_threshold_micros),
        );
    }

    if loaded.slowlog_max_len != effective.slowlog_max_len {
        set_toml(
            table,
            &["slowlog_max_len"],
            number(effective.slowlog_max_len as u64),
        );
    }

    if loaded.latency_histograms != effective.latency_histograms {
        set_toml(
            table,
            &["latency_histograms"],
            toml::Value::Boolean(effective.latency_histograms),
        );
    }

    if loaded.key_stats != effective.key_stats {
        set_toml(
            table,
            &["key_stats"],
            toml::Value::Boolean(effective.key_stats),
        );
    }

    sync_usize(
        table,
        &["protocol", "max_arguments"],
        loaded.protocol.max_arguments,
        effective.protocol.max_arguments,
    );
    sync_usize(
        table,
        &["protocol", "max_key_bytes"],
        loaded.protocol.max_key_bytes,
        effective.protocol.max_key_bytes,
    );
    sync_usize(
        table,
        &["protocol", "max_value_bytes"],
        loaded.protocol.max_value_bytes,
        effective.protocol.max_value_bytes,
    );
    sync_usize(
        table,
        &["protocol", "max_command_bytes"],
        loaded.protocol.max_command_bytes,
        effective.protocol.max_command_bytes,
    );
    sync_usize(
        table,
        &["protocol", "max_pipeline_commands"],
        loaded.protocol.max_pipeline_commands,
        effective.protocol.max_pipeline_commands,
    );
    sync_usize(
        table,
        &["protocol", "max_connection_buffer_bytes"],
        loaded.protocol.max_connection_buffer_bytes,
        effective.protocol.max_connection_buffer_bytes,
    );
    sync_usize(
        table,
        &["protocol", "databases"],
        loaded.protocol.databases,
        effective.protocol.databases,
    );
    sync_millis(
        table,
        &["timeouts", "idle_ms"],
        loaded.timeouts.idle,
        effective.timeouts.idle,
    );
    sync_millis(
        table,
        &["timeouts", "read_ms"],
        loaded.timeouts.read,
        effective.timeouts.read,
    );
    sync_millis(
        table,
        &["timeouts", "write_ms"],
        loaded.timeouts.write,
        effective.timeouts.write,
    );
    sync_usize(
        table,
        &["connections", "max_global"],
        loaded.connections.max_global,
        effective.connections.max_global,
    );
    sync_usize(
        table,
        &["connections", "max_per_ip"],
        loaded.connections.max_per_ip,
        effective.connections.max_per_ip,
    );
    sync_usize(
        table,
        &["connections", "max_per_user"],
        loaded.connections.max_per_user,
        effective.connections.max_per_user,
    );
    sync_u64(
        table,
        &["connections", "max_commands_per_second"],
        loaded.connections.max_commands_per_second,
        effective.connections.max_commands_per_second,
    );
    sync_usize(
        table,
        &["requests", "max_queued_requests"],
        loaded.requests.max_queued_requests,
        effective.requests.max_queued_requests,
    );
    sync_usize(
        table,
        &["requests", "max_output_buffer_bytes"],
        loaded.requests.max_output_buffer_bytes,
        effective.requests.max_output_buffer_bytes,
    );

    if loaded.persistence.profile != effective.persistence.profile {
        set_toml(
            table,
            &["persistence", "profile"],
            text(&effective.persistence.profile.to_string()),
        );
    }

    if loaded.persistence.data_dir != effective.persistence.data_dir {
        set_toml(
            table,
            &["persistence", "data_dir"],
            text(&effective.persistence.data_dir.display().to_string()),
        );
    }

    sync_u64(
        table,
        &["persistence", "wal_segment_bytes"],
        loaded.persistence.wal_segment_bytes,
        effective.persistence.wal_segment_bytes,
    );
    sync_millis(
        table,
        &["persistence", "sync_interval_ms"],
        loaded.persistence.sync_interval,
        effective.persistence.sync_interval,
    );
    if loaded.persistence.sync_worker != effective.persistence.sync_worker {
        set_toml(
            table,
            &["persistence", "sync_worker"],
            toml::Value::String(effective.persistence.sync_worker.to_string()),
        );
    }
    sync_u64(
        table,
        &["persistence", "snapshot_max_bytes"],
        loaded.persistence.snapshot_max_bytes,
        effective.persistence.snapshot_max_bytes,
    );
    sync_usize(
        table,
        &["persistence", "snapshot_journal_bytes"],
        loaded.persistence.snapshot_journal_bytes,
        effective.persistence.snapshot_journal_bytes,
    );
    sync_millis(
        table,
        &["persistence", "snapshot_interval_ms"],
        loaded.persistence.snapshot_interval,
        effective.persistence.snapshot_interval,
    );
    sync_u64(
        table,
        &["persistence", "snapshot_min_mutations"],
        loaded.persistence.snapshot_min_mutations,
        effective.persistence.snapshot_min_mutations,
    );
    sync_u64(
        table,
        &["persistence", "cdc_retain_bytes"],
        loaded.persistence.cdc_retain_bytes,
        effective.persistence.cdc_retain_bytes,
    );
    sync_u64(
        table,
        &["persistence", "replica_retain_max_bytes"],
        loaded.persistence.replica_retain_max_bytes,
        effective.persistence.replica_retain_max_bytes,
    );

    if loaded.replication.role != effective.replication.role {
        let role = match effective.replication.role {
            ReplicationRole::Off => "off",
            ReplicationRole::Primary => "primary",
            ReplicationRole::Replica => "replica",
        };

        set_toml(table, &["replication", "role"], text(role));
    }

    sync_optional_text(
        table,
        &["replication", "listen"],
        &loaded.replication.listen.map(|value| value.to_string()),
        &effective.replication.listen.map(|value| value.to_string()),
    );
    sync_optional_text(
        table,
        &["replication", "primary"],
        &replication_primary_text(&loaded.replication),
        &replication_primary_text(&effective.replication),
    );

    if loaded.security.requirepass != effective.security.requirepass {
        set_toml(
            table,
            &["security", "requirepass"],
            text(&effective.security.requirepass),
        );
    }

    if loaded.security.protected_mode != effective.security.protected_mode {
        set_toml(
            table,
            &["security", "protected_mode"],
            toml::Value::Boolean(effective.security.protected_mode),
        );
    }

    if loaded.security.allow_unauthenticated_public
        != effective.security.allow_unauthenticated_public
    {
        set_toml(
            table,
            &["security", "allow_unauthenticated_public"],
            toml::Value::Boolean(effective.security.allow_unauthenticated_public),
        );
    }

    if loaded.security.metrics_token != effective.security.metrics_token {
        set_toml(
            table,
            &["security", "metrics_token"],
            text(&effective.security.metrics_token),
        );
    }

    if loaded.security.replication_password != effective.security.replication_password {
        set_toml(
            table,
            &["security", "replication_password"],
            text(&effective.security.replication_password),
        );
    }

    if loaded.security.tls_cert != effective.security.tls_cert {
        set_toml(
            table,
            &["security", "tls_cert"],
            text(&effective.security.tls_cert.display().to_string()),
        );
    }

    if loaded.security.tls_key != effective.security.tls_key {
        set_toml(
            table,
            &["security", "tls_key"],
            text(&effective.security.tls_key.display().to_string()),
        );
    }

    if loaded.security.tls_client_ca != effective.security.tls_client_ca {
        set_toml(
            table,
            &["security", "tls_client_ca"],
            text(&effective.security.tls_client_ca.display().to_string()),
        );
    }

    if loaded.security.tls_client_auth != effective.security.tls_client_auth {
        set_toml(
            table,
            &["security", "tls_client_auth"],
            toml::Value::Boolean(effective.security.tls_client_auth),
        );
    }

    sync_usize(
        table,
        &["memory", "max_shard_bytes"],
        loaded.memory.max_shard_bytes,
        effective.memory.max_shard_bytes,
    );
    sync_usize(
        table,
        &["memory", "max_process_bytes"],
        loaded.memory.max_process_bytes,
        effective.memory.max_process_bytes,
    );
    sync_usize(
        table,
        &["memory", "reserved_headroom_bytes"],
        loaded.memory.reserved_headroom_bytes,
        effective.memory.reserved_headroom_bytes,
    );

    if loaded.memory.eviction_policy != effective.memory.eviction_policy {
        set_toml(
            table,
            &["memory", "maxmemory_policy"],
            text(effective.memory.eviction_policy.as_str()),
        );
    }

    sync_usize(
        table,
        &["memory", "maxmemory_samples"],
        loaded.memory.maxmemory_samples,
        effective.memory.maxmemory_samples,
    );
    sync_usize(
        table,
        &["memory", "active_expire_keys"],
        loaded.memory.active_expire_keys,
        effective.memory.active_expire_keys,
    );
    sync_usize(
        table,
        &["memory", "active_expire_maintenance_keys"],
        loaded.memory.active_expire_maintenance_keys,
        effective.memory.active_expire_maintenance_keys,
    );
    sync_usize(
        table,
        &["memory", "hash_max_compact_entries"],
        loaded.memory.hash_max_compact_entries,
        effective.memory.hash_max_compact_entries,
    );
    sync_usize(
        table,
        &["memory", "hash_max_compact_value_bytes"],
        loaded.memory.hash_max_compact_value_bytes,
        effective.memory.hash_max_compact_value_bytes,
    );
    sync_usize(
        table,
        &["memory", "set_max_intset_entries"],
        loaded.memory.set_max_intset_entries,
        effective.memory.set_max_intset_entries,
    );
    sync_usize(
        table,
        &["memory", "list_max_compact_entries"],
        loaded.memory.list_max_compact_entries,
        effective.memory.list_max_compact_entries,
    );
    sync_usize(
        table,
        &["memory", "list_max_compact_value_bytes"],
        loaded.memory.list_max_compact_value_bytes,
        effective.memory.list_max_compact_value_bytes,
    );
    sync_optional_text(
        table,
        &["metrics_address"],
        &loaded.metrics_address,
        &effective.metrics_address,
    );
    sync_usize(
        table,
        &["scripts", "max_bytes"],
        loaded.scripts.max_bytes,
        effective.scripts.max_bytes,
    );
    sync_usize(
        table,
        &["scripts", "max_cached"],
        loaded.scripts.max_cached,
        effective.scripts.max_cached,
    );
    sync_usize(
        table,
        &["scripts", "max_calls"],
        loaded.scripts.max_calls,
        effective.scripts.max_calls,
    );
    sync_usize(
        table,
        &["scripts", "max_instructions"],
        loaded.scripts.max_instructions,
        effective.scripts.max_instructions,
    );
    sync_usize(
        table,
        &["scripts", "max_time_ms"],
        loaded.scripts.max_time_ms,
        effective.scripts.max_time_ms,
    );

    if loaded.hot_keys != effective.hot_keys {
        let keys = effective
            .hot_keys
            .iter()
            .map(|pattern| text(&String::from_utf8_lossy(pattern)))
            .collect();

        set_toml(table, &["hot_keys"], toml::Value::Array(keys));
    }

    if loaded.notify_keyspace_events != effective.notify_keyspace_events {
        set_toml(
            table,
            &["notify_keyspace_events"],
            text(&effective.notify_keyspace_events),
        );
    }
}

fn replication_primary_text(replication: &ReplicationConfig) -> Option<String> {
    if let Some((host, port)) = &replication.primary_name {
        return Some(format!("{host}:{port}"));
    }

    replication.primary.map(|value| value.to_string())
}

fn sync_usize(table: &mut toml::Table, path: &[&str], loaded: usize, effective: usize) {
    if loaded != effective {
        set_toml(table, path, number(effective as u64));
    }
}

fn sync_u64(table: &mut toml::Table, path: &[&str], loaded: u64, effective: u64) {
    if loaded != effective {
        set_toml(table, path, number(effective));
    }
}

fn sync_millis(table: &mut toml::Table, path: &[&str], loaded: Duration, effective: Duration) {
    if loaded != effective {
        set_toml(table, path, number(effective.as_millis() as u64));
    }
}

fn sync_optional_text(
    table: &mut toml::Table,
    path: &[&str],
    loaded: &Option<String>,
    effective: &Option<String>,
) {
    if loaded == effective {
        return;
    }

    match effective {
        Some(value) => set_toml(table, path, text(value)),
        None => remove_toml(table, path),
    }
}

fn sync_optional_number(
    table: &mut toml::Table,
    path: &[&str],
    loaded: Option<u64>,
    effective: Option<u64>,
) {
    if loaded == effective {
        return;
    }

    match effective {
        Some(value) => set_toml(table, path, number(value)),
        None => remove_toml(table, path),
    }
}

fn text(value: &str) -> toml::Value {
    toml::Value::String(value.to_owned())
}

fn number(value: u64) -> toml::Value {
    toml::Value::Integer(i64::try_from(value).unwrap_or(i64::MAX))
}

fn set_toml(table: &mut toml::Table, path: &[&str], value: toml::Value) {
    if path.len() == 1 {
        table.insert(path[0].to_owned(), value);

        return;
    }

    let entry = table
        .entry(path[0].to_owned())
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));

    if !entry.is_table() {
        *entry = toml::Value::Table(toml::Table::new());
    }

    set_toml(entry.as_table_mut().expect("toml table"), &path[1..], value);
}

fn remove_toml(table: &mut toml::Table, path: &[&str]) {
    if path.len() == 1 {
        table.remove(path[0]);

        return;
    }

    let Some(child) = table.get_mut(path[0]).and_then(toml::Value::as_table_mut) else {
        return;
    };

    remove_toml(child, &path[1..]);
}

fn parse_bool(value: &str, name: &str) -> Result<bool, String> {
    match value {
        "1" | "true" | "TRUE" | "yes" | "on" => Ok(true),
        "0" | "false" | "FALSE" | "no" | "off" => Ok(false),
        _ => Err(format!("{name} must be a boolean")),
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn a_script_time_limit_too_large_to_add_to_the_clock_is_refused() {
        let limits = super::ScriptLimits {
            max_time_ms: usize::MAX,
            ..super::ScriptLimits::default()
        };

        assert!(limits.validate().is_err());
        assert!(super::ScriptLimits::default().validate().is_ok());
    }

    #[test]
    fn default_config_file_holds_the_code_defaults() {
        let mut parsed = super::AppConfig::default();

        parsed.apply_toml(super::DEFAULT_CONFIG).unwrap();
        assert_eq!(parsed, super::AppConfig::default());
    }

    #[test]
    fn write_config_refuses_a_file_that_does_not_parse_and_writes_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ruvio.toml");
        let loaded = super::AppConfig::default();
        let changed = super::AppConfig {
            shards: 3,
            ..super::AppConfig::default()
        };
        let broken = "address = \"127.0.0.1:7000\"\n[persistence\nprofile = \"always\"\n";

        std::fs::write(&path, broken).unwrap();
        assert!(
            changed
                .write_overrides(&path, &loaded)
                .unwrap_err()
                .contains("does not parse")
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), broken);

        // A missing file is created; a valid one keeps its other settings.
        std::fs::remove_file(&path).unwrap();
        changed.write_overrides(&path, &loaded).unwrap();
        std::fs::write(&path, "address = \"127.0.0.1:7000\"\n").unwrap();
        changed.write_overrides(&path, &loaded).unwrap();
        let mut restored = super::AppConfig::default();

        restored
            .apply_toml(&std::fs::read_to_string(&path).unwrap())
            .unwrap();
        assert_eq!(restored.address, "127.0.0.1:7000");
        assert_eq!(restored.shards, 3);
        let names: Vec<_> = std::fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();

        assert_eq!(names, ["ruvio.toml"]);
    }

    #[test]
    fn sync_worker_modes_resolve_and_round_trip() {
        use crate::persistence::SyncWorkerMode;
        let mut config = super::AppConfig::default();
        assert_eq!(config.persistence.sync_worker, SyncWorkerMode::Auto);
        config
            .apply_toml("[persistence]\nprofile = \"always\"\n")
            .unwrap();
        config.resolve_sync_worker();
        assert_eq!(
            config.persistence.sync_worker_active,
            cfg!(target_os = "linux")
        );
        // Legacy key keeps working and maps onto the mode.
        config
            .apply_toml("[persistence]\nexperimental_sync_worker = true\n")
            .unwrap();
        assert_eq!(config.persistence.sync_worker, SyncWorkerMode::On);
        config.resolve_sync_worker();
        config.validate().unwrap();
        assert!(config.persistence.sync_worker_active);
        config
            .apply_environment(|name| (name == "RUVIO_SYNC_WORKER").then(|| "off".into()))
            .unwrap();
        config.resolve_sync_worker();
        assert!(!config.persistence.sync_worker_active);
        assert!(
            config
                .apply_environment(|name| (name == "RUVIO_SYNC_WORKER").then(|| "maybe".into()))
                .is_err()
        );
        config
            .apply_environment(|name| (name == "RUVIO_SYNC_WORKER").then(|| "on".into()))
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ruvio.toml");
        let loaded = super::AppConfig::default();
        std::fs::write(&path, "[persistence]\nprofile = \"memory\"\n").unwrap();
        config.write_overrides(&path, &loaded).unwrap();
        let mut restored = super::AppConfig::default();
        restored
            .apply_toml(&std::fs::read_to_string(&path).unwrap())
            .unwrap();
        assert_eq!(restored.persistence.sync_worker, SyncWorkerMode::On);
        // `on` outside the supported envelope is an error; `auto` just stays off.
        config.persistence.profile = crate::DurabilityProfile::Memory;
        assert!(config.validate().unwrap_err().contains("always durability"));
        config.persistence.profile = crate::DurabilityProfile::Always;
        config.hot_keys.push("cache:*".into());
        assert!(config.validate().unwrap_err().contains("hot_keys"));
        config.persistence.sync_worker = SyncWorkerMode::Auto;
        config.resolve_sync_worker();
        assert!(!config.persistence.sync_worker_active);
        config.validate().unwrap();
        // `auto` also stays off with more than one shard (not yet measured).
        config.hot_keys.clear();
        config.shards = 2;
        config.resolve_sync_worker();
        assert!(!config.persistence.sync_worker_active);
    }
    use super::*;

    #[test]
    fn environment_override_is_written_into_the_toml_and_users_stay() {
        let path = env::temp_dir().join(format!(
            "ruvio-config-{}-{}.toml",
            std::process::id(),
            "users"
        ));

        fs::write(
            &path,
            "shards = 1\naddress = \"127.0.0.1:6379\"\n\n[[security.users]]\nname = \"app\"\nrules = \"on >secret ~app:* +ping\"\n",
        )
        .unwrap();

        let mut loaded = AppConfig::default();

        loaded
            .apply_toml(&fs::read_to_string(&path).unwrap())
            .unwrap();

        let mut effective = loaded.clone();

        effective.shards = 2;
        effective.address = "0.0.0.0:6380".into();
        effective.write_overrides(&path, &loaded).unwrap();

        let text = fs::read_to_string(&path).unwrap();

        assert!(text.contains("shards = 2"), "{text}");
        assert!(text.contains("0.0.0.0:6380"), "{text}");
        assert!(text.contains("name = \"app\""), "{text}");
        assert!(text.contains("does not change the live server"), "{text}");

        let mut again = AppConfig::default();

        again.apply_toml(&text).unwrap();
        assert_eq!(again.shards, 2);
        assert_eq!(again.address, "0.0.0.0:6380");
        assert_eq!(again.security.users.len(), 1);

        let stable = fs::read_to_string(&path).unwrap();

        again.write_overrides(&path, &again).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), stable);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn rejects_disabled_timeouts() {
        let timeouts = ConnectionTimeouts {
            read: Duration::ZERO,
            ..ConnectionTimeouts::default()
        };

        assert_eq!(
            timeouts.validate(),
            Err("read timeout must be greater than zero")
        );
    }

    #[test]
    fn clamps_nested_connection_limits_to_the_global_cap() {
        let mut limits = ConnectionLimits {
            max_global: 10,
            max_per_ip: 11,
            max_per_user: 40,
            ..ConnectionLimits::default()
        };

        let notes = limits.clamp_to_global();

        assert_eq!(limits.max_per_ip, 10);
        assert_eq!(limits.max_per_user, 10);
        assert_eq!(notes.len(), 2);
        assert!(limits.validate().is_ok());
        assert!(
            ConnectionLimits {
                max_global: 0,
                ..ConnectionLimits::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn rejects_disabled_request_limits() {
        assert_eq!(
            RequestLimits {
                max_queued_requests: 0,
                ..RequestLimits::default()
            }
            .validate(),
            Err("request queue limit must be greater than zero")
        );
    }

    #[test]
    fn advertised_port_comes_from_the_environment_and_rejects_zero() {
        let mut config = AppConfig::default();

        config
            .apply_environment(|name| (name == "RUVIO_ADVERTISE_PORT").then(|| "17000".into()))
            .unwrap();
        assert_eq!(config.advertised_port, Some(17000));
        assert!(
            config
                .introspect()
                .contains(&("advertised-port".into(), "17000".into()))
        );
        assert!(
            AppConfig::default()
                .apply_environment(|name| (name == "RUVIO_ADVERTISE_PORT").then(|| "0".into()))
                .is_err()
        );

        let mut file = AppConfig::default();

        file.apply_toml("advertised_port = 7100\n").unwrap();
        assert_eq!(file.advertised_port, Some(7100));
    }

    #[test]
    fn merges_file_environment_and_cli_in_precedence_order() {
        let mut config = AppConfig::default();

        config
            .apply_toml(
                r#"
                address = "127.0.0.1:7000"

                [requests]
                max_queued_requests = 10
                "#,
            )
            .unwrap();
        config
            .apply_environment(|name| match name {
                "RUVIO_ADDR" => Some("127.0.0.1:7001".into()),
                "RUVIO_MAX_QUEUED_REQUESTS" => Some("11".into()),
                _ => None,
            })
            .unwrap();
        config.apply_overrides(ConfigOverrides {
            address: Some("127.0.0.1:7002".into()),
            ..ConfigOverrides::default()
        });

        assert_eq!(config.address, "127.0.0.1:7002");
        assert_eq!(config.requests.max_queued_requests, 11);
        config.validate().unwrap();
    }

    #[test]
    fn rejects_unknown_file_fields_and_overlapping_metrics() {
        let mut config = AppConfig::default();

        assert!(config.apply_toml("unknown = true").is_err());
        assert!(config.apply_toml("backend = \"evented\"\n").is_err());

        config.metrics_address = Some("127.0.0.1:6379".into());
        assert_eq!(
            config.validate(),
            Err("metrics port range overlaps the RESP listener range".into())
        );
        config.metrics_address = Some("127.0.0.1:9121".into());
        config.shards = 2;
        config.validate().unwrap();
    }

    #[test]
    fn consecutive_shard_ports_are_accepted() {
        let config = AppConfig {
            shards: 2,
            advertised_host: "127.0.0.1".into(),
            ..AppConfig::default()
        };

        config.validate().unwrap();
    }

    #[test]
    fn config_get_redacts_requirepass_and_matches_globs() {
        let mut config = AppConfig::default();

        config.security.requirepass = "super-secret-password".into();
        config.protocol.max_arguments = 32;

        let requirepass = config.config_get(b"requirepass");

        assert_eq!(requirepass, vec![("requirepass".into(), "***".into())]);
        let dump = config
            .config_get(b"*")
            .into_iter()
            .flat_map(|(name, value)| [name, value])
            .collect::<Vec<_>>()
            .join("\n");

        assert!(!dump.contains("super-secret-password"));
        assert!(dump.contains("requirepass\n***"));

        let matched: Vec<_> = config
            .config_get(b"max-arg*")
            .into_iter()
            .map(|(name, _)| name)
            .collect();

        assert_eq!(matched, ["max-arguments"]);
        assert!(config.config_get(b"unknown-setting").is_empty());
        assert_eq!(
            config.config_get(b"SAVE"),
            vec![("save".into(), String::new())]
        );
        assert_eq!(
            config.config_get(b"maxmemory-policy"),
            vec![("maxmemory-policy".into(), "noeviction".into())]
        );
        assert_eq!(
            config.config_get(b"notify-keyspace-events"),
            vec![("notify-keyspace-events".into(), String::new())]
        );
    }

    #[test]
    fn notify_keyspace_events_parse_from_file_and_environment() {
        let mut config = AppConfig::default();

        assert_eq!(config.memory.notify_mask, 0);
        config
            .apply_toml("notify_keyspace_events = \"K$\"\n")
            .unwrap();
        assert_eq!(config.notify_keyspace_events, "K$");
        assert_ne!(config.memory.notify_mask, 0);
        config
            .apply_environment(|name| {
                (name == "RUVIO_NOTIFY_KEYSPACE_EVENTS").then(|| "Eg".to_string())
            })
            .unwrap();
        assert_eq!(config.notify_keyspace_events, "Eg");
        assert!(
            config
                .apply_toml("notify_keyspace_events = \"nope\"\n")
                .is_err()
        );
        assert!(
            config
                .apply_toml("notify_keyspace_events = \"A\"\n")
                .is_ok()
        );
        assert_eq!(config.memory.notify_mask, 0);
    }

    #[test]
    fn parses_connection_quotas_from_file_and_environment() {
        let mut config = AppConfig::default();

        config
            .apply_toml(
                r#"
                [connections]
                max_per_user = 3
                max_commands_per_second = 40
                "#,
            )
            .unwrap();
        assert_eq!(config.connections.max_per_user, 3);
        assert_eq!(config.connections.max_commands_per_second, 40);
        config
            .apply_environment(|name| match name {
                "RUVIO_MAX_CONNECTIONS_PER_USER" => Some("2".into()),
                "RUVIO_MAX_COMMANDS_PER_SECOND" => Some("10".into()),
                _ => None,
            })
            .unwrap();
        assert_eq!(config.connections.max_per_user, 2);
        assert_eq!(config.connections.max_commands_per_second, 10);
        config.connections.max_per_user = 20_000;
        assert_eq!(config.connections.clamp_to_global().len(), 1);
        assert_eq!(
            config.connections.max_per_user,
            config.connections.max_global
        );
    }

    #[test]
    fn parses_acl_users_from_file_and_rejects_unknown_rules() {
        let mut config = AppConfig::default();

        config
            .apply_toml(
                r#"
                [security]
                requirepass = "secret"
                [[security.users]]
                name = "app"
                rules = "on >app-secret ~app:* +get +ping +auth"
                "#,
            )
            .unwrap();
        assert_eq!(config.security.users.len(), 1);
        assert!(config.security.users[0].rules.allows_command("get"));
        assert!(!config.security.users[0].rules.allows_command("set"));
        assert_eq!(
            config.config_get(b"acl-users"),
            vec![("acl-users".into(), "default app".into())]
        );
        assert!(
            AppConfig::default()
                .apply_toml(
                    r#"
                    [[security.users]]
                    name = "bad"
                    rules = "on +@not-a-category"
                    "#,
                )
                .is_err()
        );
    }

    #[test]
    fn accepts_paired_tls_paths() {
        let mut config = AppConfig::default();

        config
            .apply_toml(
                r#"
                [security]
                tls_cert = "/tmp/cert.pem"
                tls_key = "/tmp/key.pem"
                "#,
            )
            .unwrap();
        assert!(!config.security.tls_cert.as_os_str().is_empty());
        config
            .apply_environment(|name| match name {
                "RUVIO_TLS_CERT" => Some("/env/cert.pem".into()),
                "RUVIO_TLS_KEY" => Some("/env/key.pem".into()),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            config.security.tls_cert,
            std::path::PathBuf::from("/env/cert.pem")
        );
        assert_eq!(
            config.config_get(b"tls-cert"),
            vec![("tls-cert".into(), "/env/cert.pem".into())]
        );
        config.validate().unwrap();

        config.security.tls_key = std::path::PathBuf::new();
        assert_eq!(
            config.security.validate(),
            Err("tls-cert and tls-key must be set together")
        );
    }

    #[test]
    fn parses_maxmemory_policy_from_file_and_environment() {
        let mut config = AppConfig::default();

        config
            .apply_toml("[memory]\nmaxmemory_policy = \"allkeys-lru\"\n")
            .unwrap();
        assert_eq!(config.memory.eviction_policy, EvictionPolicy::AllKeysLru);
        config
            .apply_environment(|name| {
                (name == "RUVIO_MAXMEMORY_POLICY").then(|| "volatile-ttl".into())
            })
            .unwrap();
        assert_eq!(config.memory.eviction_policy, EvictionPolicy::VolatileTtl);
        assert!(
            AppConfig::default()
                .apply_toml("[memory]\nmaxmemory_policy = \"random\"\n")
                .is_err()
        );
    }

    #[test]
    fn parses_memory_tuning_from_file_and_environment() {
        let mut config = AppConfig::default();

        config
            .apply_toml(
                "[memory]\nmaxmemory_samples = 4\nactive_expire_keys = 32\nactive_expire_maintenance_keys = 64\nhash_max_compact_entries = 8\nhash_max_compact_value_bytes = 128\n",
            )
            .unwrap();
        assert_eq!(config.memory.maxmemory_samples, 4);
        assert_eq!(config.memory.active_expire_keys, 32);
        assert_eq!(config.memory.active_expire_maintenance_keys, 64);
        assert_eq!(config.memory.hash_max_compact_entries, 8);
        assert_eq!(config.memory.hash_max_compact_value_bytes, 128);
        config
            .apply_environment(|name| match name {
                "RUVIO_MAXMEMORY_SAMPLES" => Some("8".into()),
                "RUVIO_ACTIVE_EXPIRE_KEYS" => Some("64".into()),
                "RUVIO_ACTIVE_EXPIRE_MAINTENANCE_KEYS" => Some("32".into()),
                _ => None,
            })
            .unwrap();
        assert_eq!(config.memory.maxmemory_samples, 8);
        assert_eq!(config.memory.active_expire_keys, 64);
        assert_eq!(config.memory.active_expire_maintenance_keys, 32);
        assert_eq!(
            config.config_get(b"active-expire-*"),
            vec![
                ("active-expire-keys".into(), "64".into()),
                ("active-expire-maintenance-keys".into(), "32".into()),
            ]
        );
        assert_eq!(
            config.config_get(b"maxmemory-samples"),
            vec![("maxmemory-samples".into(), "8".into())]
        );
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ruvio.toml");
        std::fs::write(&path, "[memory]\nactive_expire_keys = 32\n").unwrap();
        let mut loaded = AppConfig::default();
        loaded
            .apply_toml(&std::fs::read_to_string(&path).unwrap())
            .unwrap();
        config.write_overrides(&path, &loaded).unwrap();
        let mut restored = AppConfig::default();
        restored
            .apply_toml(&std::fs::read_to_string(&path).unwrap())
            .unwrap();
        assert_eq!(restored.memory.active_expire_keys, 64);
        assert_eq!(restored.memory.active_expire_maintenance_keys, 32);
        config.memory.maxmemory_samples = 0;
        assert_eq!(
            config.memory.validate(),
            Err("maxmemory-samples must be between 1 and 64")
        );
        config.memory.maxmemory_samples = 8;
        for invalid in [0, 1_025] {
            config.memory.active_expire_maintenance_keys = invalid;
            assert_eq!(
                config.memory.validate(),
                Err("active-expire-maintenance-keys must be between 1 and 1024")
            );
        }
    }

    #[test]
    fn parses_database_count_from_file_and_environment() {
        let mut config = AppConfig::default();

        assert_eq!(config.protocol.databases, 16);
        config.apply_toml("[protocol]\ndatabases = 4\n").unwrap();
        assert_eq!(config.protocol.databases, 4);
        config
            .apply_environment(|name| (name == "RUVIO_DATABASES").then(|| "32".into()))
            .unwrap();
        assert_eq!(config.protocol.databases, 32);
        assert_eq!(
            config.config_get(b"databases"),
            vec![("databases".into(), "32".into())]
        );
        config.protocol.databases = 0;
        assert!(config.protocol.validate().is_err());
        config.protocol.databases = 256;
        assert!(config.protocol.validate().is_ok());
        config.protocol.databases = 257;
        assert!(config.protocol.validate().is_err());
    }

    #[test]
    fn accepts_hot_keys_from_file_and_environment() {
        let mut config = AppConfig::default();

        assert!(config.hot_keys.is_empty());
        config
            .apply_toml(r#"hot_keys = ["lock:*", "session:*"]"#)
            .unwrap();
        assert_eq!(
            config.hot_keys,
            vec![b"lock:*".to_vec(), b"session:*".to_vec()]
        );
        config
            .apply_environment(|name| (name == "RUVIO_HOT_KEYS").then(|| "cache:?,other:*".into()))
            .unwrap();
        assert_eq!(
            config.hot_keys,
            vec![b"cache:?".to_vec(), b"other:*".to_vec()]
        );
    }

    #[test]
    fn rejects_empty_hot_key_patterns() {
        let config = AppConfig {
            hot_keys: vec![Vec::new()],
            ..AppConfig::default()
        };

        assert_eq!(
            config.validate(),
            Err("hot_keys entries must be non-empty globs".into())
        );
    }

    #[test]
    fn process_maxmemory_split_keeps_the_same_usable_total_for_any_shard_count() {
        let budget_total = |shards: usize| {
            let mut config = AppConfig {
                shards,
                ..AppConfig::default()
            };

            config.memory.max_process_bytes = 64 * 1024 * 1024;
            config.validate().unwrap();
            config
                .shard_memory_limits()
                .shard_admission_budget()
                .unwrap()
                * shards
        };
        let single = budget_total(1);

        assert_eq!(single, 48 * 1024 * 1024);
        for shards in [2, 4, 8] {
            assert_eq!(budget_total(shards), single, "{shards} shards");
        }
    }

    #[test]
    fn maxmemory_that_leaves_no_shard_budget_is_rejected_at_startup() {
        let mut config = AppConfig {
            shards: 4,
            ..AppConfig::default()
        };

        config.memory.max_process_bytes = 16 * 1024 * 1024;
        let error = config.validate().unwrap_err();

        assert!(error.contains("per shard"), "{error}");

        config.memory.reserved_headroom_bytes = 0;
        config.validate().unwrap();
    }

    #[test]
    fn checkpoint_partitions_include_hot_store_when_hot_keys_set() {
        let off = AppConfig {
            shards: 2,
            ..AppConfig::default()
        };

        assert_eq!(off.checkpoint_partition_count(), 2);
        let on = AppConfig {
            shards: 2,
            hot_keys: vec![b"lock:*".to_vec()],
            ..AppConfig::default()
        };

        assert_eq!(on.checkpoint_partition_count(), 3);
    }

    #[test]
    fn replication_config_parses_role_and_rejects_a_non_wal_profile() {
        let mut config = AppConfig::default();

        config
            .apply_toml(
                r#"
            replication.role = "primary"
            replication.listen = "127.0.0.1:16379"
            persistence.profile = "everysec"
            persistence.data_dir = "target/replication-config"
            "#,
            )
            .unwrap();
        assert_eq!(config.replication.role, ReplicationRole::Primary);
        assert_eq!(config.replication.listen.unwrap().port(), 16379);
        config.replication.role = ReplicationRole::Primary;
        config.persistence.profile = DurabilityProfile::Memory;
        assert_eq!(
            config.validate(),
            Err("replication requires a WAL-backed durability profile".into())
        );
        config.persistence.profile = DurabilityProfile::Everysec;
        config
            .apply_environment(|name| match name {
                "RUVIO_REPLICATION_ROLE" => Some("replica".into()),
                "RUVIO_REPLICATION_PRIMARY" => Some("127.0.0.1:16379".into()),
                _ => None,
            })
            .unwrap();
        assert_eq!(config.replication.role, ReplicationRole::Replica);
        assert_eq!(config.replication.primary.unwrap().port(), 16379);
        config.hot_keys.clear();
        config.replication.role = ReplicationRole::Primary;
        config.replication.listen = None;
        assert_eq!(
            config.validate(),
            Err("replication primary requires replication.listen".into())
        );
        config.replication.role = ReplicationRole::Replica;
        config.replication.primary = None;
        assert_eq!(
            config.validate(),
            Err("replication replica requires replication.primary".into())
        );
        config.replication.role = ReplicationRole::Primary;
        config.replication.listen = Some("127.0.0.1:6379".parse().unwrap());
        assert_eq!(
            config.validate(),
            Err("replication port range overlaps the RESP listener range".into())
        );
        config.replication.listen = Some("127.0.0.1:16379".parse().unwrap());
        config.metrics_address = None;
        assert_eq!(
            config.validate(),
            Err("replication port range overlaps the cluster bus port range".into())
        );
        config.replication.listen = Some("127.0.0.1:26379".parse().unwrap());
        config.metrics_address = Some("127.0.0.1:26379".into());
        assert_eq!(
            config.validate(),
            Err("replication port range overlaps the metrics listener range".into())
        );
        assert!(
            config
                .apply_toml(
                    r#"
            replication.nope = true
            "#,
                )
                .is_err()
        );
        assert_eq!(
            config.apply_toml("replication.listen = \"localhost:16379\"\n"),
            Err("replication.listen must be a numeric socket address".into())
        );
        assert_eq!(
            config.apply_environment(|name| {
                (name == "RUVIO_REPLICATION_PRIMARY").then(|| "not-a-socket".into())
            }),
            Err("replication.primary must be host:port".into())
        );
    }

    #[test]
    fn validate_rejects_requirepass_longer_than_65535() {
        let mut config = AppConfig::default();

        config.security.requirepass = "x".repeat(65_536);
        assert_eq!(
            config.validate(),
            Err("requirepass must be at most 65535 bytes".into())
        );
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn refuses_a_public_listener_without_authentication() {
        let mut config = AppConfig::default();

        config.address = "0.0.0.0:6379".into();
        config.security.protected_mode = false;
        assert!(config.validate().unwrap_err().contains("protected mode"));

        config.security.requirepass = "secret".into();
        assert!(config.validate().is_ok());

        config.security.requirepass.clear();
        config.security.allow_unauthenticated_public = true;
        assert!(config.validate().is_ok());
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn public_metrics_and_replication_require_a_secret() {
        let mut config = AppConfig::default();

        config.metrics_address = Some("0.0.0.0:9121".into());
        assert!(config.validate().unwrap_err().contains("metrics"));
        config.security.metrics_token = "scrape".into();
        assert!(config.validate().is_ok());

        config.persistence.profile = crate::DurabilityProfile::Everysec;
        config.replication.role = ReplicationRole::Primary;
        config.replication.listen = Some("0.0.0.0:26379".parse().unwrap());
        assert!(config.validate().unwrap_err().contains("replication"));
        config.security.replication_password = "repl".into();
        assert!(config.validate().is_ok());
        assert_eq!(
            config.config_get(b"replication-password"),
            vec![("replication-password".into(), "***".into())]
        );
        assert_eq!(
            config.config_get(b"metrics-token"),
            vec![("metrics-token".into(), "***".into())]
        );
    }

    #[cfg(unix)]
    #[test]
    fn written_overrides_are_readable_by_the_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ruvio.toml");
        let loaded = AppConfig::default();
        let mut config = AppConfig::default();

        config.security.requirepass = "secret".into();
        config.write_overrides(&path, &loaded).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    /// Every setting the shipped template names answers `CONFIG GET` under one name. A
    /// key added to the template fails here until it has a row and a `CONFIG GET` entry.
    #[test]
    fn every_template_setting_answers_config_get() {
        const NAMES: &[(&str, &str)] = &[
            ("address", "address"),
            ("advertised_host", "advertised-host"),
            (
                "connections.max_commands_per_second",
                "max-commands-per-second",
            ),
            ("connections.max_global", "max-connections"),
            ("connections.max_per_ip", "max-connections-per-ip"),
            ("connections.max_per_user", "max-connections-per-user"),
            ("hot_keys", "hot-keys"),
            ("memory.active_expire_keys", "active-expire-keys"),
            (
                "memory.active_expire_maintenance_keys",
                "active-expire-maintenance-keys",
            ),
            (
                "memory.hash_max_compact_entries",
                "hash-max-compact-entries",
            ),
            (
                "memory.hash_max_compact_value_bytes",
                "hash-max-compact-value-bytes",
            ),
            (
                "memory.list_max_compact_entries",
                "list-max-compact-entries",
            ),
            (
                "memory.list_max_compact_value_bytes",
                "list-max-compact-value-bytes",
            ),
            ("memory.max_process_bytes", "maxmemory"),
            ("memory.max_shard_bytes", "max-shard-bytes"),
            ("memory.maxmemory_policy", "maxmemory-policy"),
            ("memory.maxmemory_samples", "maxmemory-samples"),
            ("memory.reserved_headroom_bytes", "memory-headroom-bytes"),
            ("memory.set_max_intset_entries", "set-max-intset-entries"),
            ("notify_keyspace_events", "notify-keyspace-events"),
            ("persistence.data_dir", "data-dir"),
            ("persistence.profile", "durability"),
            ("persistence.snapshot_interval_ms", "snapshot-interval-ms"),
            (
                "persistence.snapshot_journal_bytes",
                "snapshot-journal-bytes",
            ),
            ("persistence.snapshot_max_bytes", "snapshot-max-bytes"),
            (
                "persistence.snapshot_min_mutations",
                "snapshot-min-mutations",
            ),
            ("persistence.sync_interval_ms", "wal-sync-interval-ms"),
            ("persistence.wal_segment_bytes", "wal-segment-bytes"),
            ("protocol.databases", "databases"),
            ("protocol.max_arguments", "max-arguments"),
            ("protocol.max_command_bytes", "max-command-bytes"),
            (
                "protocol.max_connection_buffer_bytes",
                "max-connection-buffer-bytes",
            ),
            ("protocol.max_key_bytes", "max-key-bytes"),
            ("protocol.max_pipeline_commands", "max-pipeline-commands"),
            ("protocol.max_value_bytes", "max-value-bytes"),
            ("replication.role", "replication-role"),
            (
                "requests.max_output_buffer_bytes",
                "max-output-buffer-bytes",
            ),
            ("requests.max_queued_requests", "max-queued-requests"),
            ("scripts.max_bytes", "script-max-bytes"),
            ("scripts.max_cached", "script-max-cached"),
            ("scripts.max_calls", "script-max-calls"),
            ("scripts.max_instructions", "script-max-instructions"),
            (
                "security.allow_unauthenticated_public",
                "allow-unauthenticated-public",
            ),
            ("security.metrics_token", "metrics-token"),
            ("security.protected_mode", "protected-mode"),
            ("security.replication_password", "replication-password"),
            ("security.requirepass", "requirepass"),
            ("security.tls_cert", "tls-cert"),
            ("security.tls_key", "tls-key"),
            ("shards", "shards"),
            ("shutdown_grace_ms", "shutdown-grace-ms"),
            ("timeouts.idle_ms", "idle-timeout-ms"),
            ("timeouts.read_ms", "read-timeout-ms"),
            ("timeouts.write_ms", "write-timeout-ms"),
        ];

        fn keys(table: &toml::Table, prefix: &str, out: &mut Vec<String>) {
            for (name, value) in table {
                match value {
                    toml::Value::Table(inner) => keys(inner, &format!("{prefix}{name}."), out),
                    _ => out.push(format!("{prefix}{name}")),
                }
            }
        }

        let template: toml::Table = toml::from_str(DEFAULT_CONFIG).unwrap();
        let mut found = Vec::new();

        keys(&template, "", &mut found);
        let answered: Vec<String> = AppConfig::default()
            .introspect()
            .into_iter()
            .map(|(name, _)| name)
            .collect();

        for key in &found {
            let Some((_, name)) = NAMES.iter().find(|(path, _)| path == key) else {
                panic!("template setting {key} has no CONFIG GET name in this test");
            };

            assert!(
                answered.iter().any(|answered| answered == name),
                "CONFIG GET does not answer {name} (template {key})"
            );
        }
    }
}
