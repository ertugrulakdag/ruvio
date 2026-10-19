//! `coordinator.toml`: who the coordinators are, which replication groups they watch and how
//! quickly they act.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use serde::Deserialize;

/// One data node of a replication group.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Member {
    /// Its RESP address (shard 0's port in per-shard mode), the one clients use.
    pub address: SocketAddr,
    /// Its replication base address (`replication.listen`), the one `REPLICAOF` names.
    pub replication: SocketAddr,
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
    pub peers: Vec<(u64, SocketAddr)>,
    pub ping_interval: Duration,
    pub down_after: Duration,
    pub auth: Option<String>,
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
            let address = address
                .parse()
                .map_err(|_| format!("peer {peer:?} has no numeric socket address"))?;

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
