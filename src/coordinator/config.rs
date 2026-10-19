//! `coordinator.toml`: who the coordinators are, which replication groups they watch and how
//! quickly they act.

use std::{fmt, net::SocketAddr, path::PathBuf, time::Duration};

use serde::Deserialize;

/// A `host:port` the coordinator dials. The host may be a name (a container or DNS name); it
/// is resolved again on every connection, so a node that moves keeps working.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    pub fn parse(text: &str) -> Result<Self, String> {
        let (host, port) = text
            .rsplit_once(':')
            .ok_or_else(|| format!("{text:?} must be host:port"))?;
        let host = host.trim_start_matches('[').trim_end_matches(']');

        if host.is_empty() {
            return Err(format!("{text:?} has no host"));
        }

        Ok(Self {
            host: host.to_owned(),
            port: port
                .parse()
                .map_err(|_| format!("{text:?} has no valid port"))?,
        })
    }

    /// The same host, `offset` ports up: shard `i` of a data node.
    pub fn plus(&self, offset: u16) -> Self {
        Self {
            host: self.host.clone(),
            port: self.port + offset,
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(formatter, "[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "{}:{}", self.host, self.port)
        }
    }
}

impl<'de> Deserialize<'de> for Endpoint {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;

        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// One data node of a replication group.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Member {
    /// Its RESP address (shard 0's port in per-shard mode), the one clients use.
    pub address: Endpoint,
    /// Its replication base address (`replication.listen`), the one `REPLICAOF` names.
    pub replication: Endpoint,
}

/// A primary and its replicas, failed over together.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Group {
    pub name: String,
    /// The first member is the primary when the coordinators start for the first time.
    pub members: Vec<Member>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    id: u64,
    listen: SocketAddr,
    data_dir: PathBuf,
    /// `id@host:port` of every coordinator, this one included.
    peers: Vec<String>,
    #[serde(default = "default_ping_interval_ms")]
    ping_interval_ms: u64,
    #[serde(default = "default_down_after_ms")]
    down_after_ms: u64,
    /// Password (or `username password`) the coordinator logs in to data nodes with.
    #[serde(default)]
    auth: Option<String>,
    /// Shared by every coordinator: required on coordinator-to-coordinator calls and, with
    /// `AUTH`, from clients of the Sentinel API. Without it anyone who reaches the port can
    /// read the topology and take part in the coordinators' votes.
    #[serde(default)]
    secret: Option<String>,
    #[serde(default)]
    groups: Vec<Group>,
}

fn default_ping_interval_ms() -> u64 {
    500
}

fn default_down_after_ms() -> u64 {
    3000
}

#[derive(Clone, Debug)]
pub struct CoordinatorConfig {
    pub id: u64,
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub peers: Vec<(u64, Endpoint)>,
    pub ping_interval: Duration,
    pub down_after: Duration,
    pub auth: Option<String>,
    pub secret: Option<String>,
    pub groups: Vec<Group>,
}

impl CoordinatorConfig {
    pub fn parse(text: &str) -> Result<Self, String> {
        let file: FileConfig = toml::from_str(text).map_err(|error| error.to_string())?;
        let mut peers = Vec::with_capacity(file.peers.len());

        for peer in &file.peers {
            let (id, address) = peer
                .split_once('@')
                .ok_or_else(|| format!("peer {peer:?} must be id@host:port"))?;
            let id = id
                .parse()
                .map_err(|_| format!("peer {peer:?} has no numeric id"))?;
            let address = Endpoint::parse(address)?;

            if peers.iter().any(|(known, _)| *known == id) {
                return Err(format!("peer id {id} is listed twice"));
            }
            peers.push((id, address));
        }

        if !peers.iter().any(|(id, _)| *id == file.id) {
            return Err(format!(
                "peers must include this coordinator (id {})",
                file.id
            ));
        }
        if file.ping_interval_ms == 0 || file.down_after_ms < file.ping_interval_ms {
            return Err("down_after_ms must be at least ping_interval_ms, which is above 0".into());
        }
        for group in &file.groups {
            if group.members.len() < 2 {
                return Err(format!(
                    "group {:?} needs a primary and a replica",
                    group.name
                ));
            }
            if file
                .groups
                .iter()
                .filter(|other| other.name == group.name)
                .count()
                > 1
            {
                return Err(format!("group {:?} is listed twice", group.name));
            }
        }

        Ok(Self {
            id: file.id,
            listen: file.listen,
            data_dir: file.data_dir,
            peers,
            ping_interval: Duration::from_millis(file.ping_interval_ms),
            down_after: Duration::from_millis(file.down_after_ms),
            auth: file.auth.filter(|auth| !auth.is_empty()),
            secret: std::env::var("RUVIO_COORDINATOR_SECRET")
                .ok()
                .or(file.secret)
                .filter(|secret| !secret.is_empty()),
            groups: file.groups,
        })
    }

    /// How many coordinators make a majority.
    pub fn quorum(&self) -> usize {
        self.peers.len() / 2 + 1
    }

    pub fn group(&self, name: &str) -> Option<&Group> {
        self.groups.iter().find(|group| group.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
id = 2
listen = "127.0.0.1:26401"
data_dir = "/tmp/c2"
peers = ["1@127.0.0.1:26400", "2@127.0.0.1:26401", "3@127.0.0.1:26402"]
down_after_ms = 1000

[[groups]]
name = "main"
members = [
  { address = "127.0.0.1:6379", replication = "127.0.0.1:26379" },
  { address = "127.0.0.1:6380", replication = "127.0.0.1:26380" },
]
"#;

    #[test]
    fn parses_peers_groups_and_defaults() {
        let config = CoordinatorConfig::parse(SAMPLE).unwrap();

        assert_eq!(config.id, 2);
        assert_eq!(config.peers.len(), 3);
        assert_eq!(config.quorum(), 2);
        assert_eq!(config.ping_interval, Duration::from_millis(500));
        assert_eq!(config.down_after, Duration::from_millis(1000));
        assert_eq!(config.group("main").unwrap().members.len(), 2);
        assert!(config.group("other").is_none());
    }

    #[test]
    fn endpoints_take_names_and_addresses() {
        let named = Endpoint::parse("ruvio-1:6379").unwrap();

        assert_eq!((named.host.as_str(), named.port), ("ruvio-1", 6379));
        assert_eq!(named.plus(2).to_string(), "ruvio-1:6381");
        assert_eq!(
            Endpoint::parse("[::1]:6379").unwrap().to_string(),
            "[::1]:6379"
        );
        assert!(Endpoint::parse("6379").is_err());
        assert!(Endpoint::parse(":6379").is_err());
    }

    #[test]
    fn refuses_inconsistent_settings() {
        for (from, to, reason) in [
            ("id = 2", "id = 9", "include this coordinator"),
            (
                "down_after_ms = 1000",
                "down_after_ms = 100",
                "at least ping_interval_ms",
            ),
            (
                "\"1@127.0.0.1:26400\"",
                "\"x@127.0.0.1:26400\"",
                "numeric id",
            ),
            (
                "\"3@127.0.0.1:26402\"",
                "\"2@127.0.0.1:26402\"",
                "listed twice",
            ),
        ] {
            let error = CoordinatorConfig::parse(&SAMPLE.replace(from, to)).unwrap_err();

            assert!(error.contains(reason), "{error}");
        }
    }
}
