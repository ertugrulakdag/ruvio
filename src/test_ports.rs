//! Ports for tests that start a server on a port they name, instead of handing it a listener
//! that is already bound.
//!
//! Picking a port by binding `127.0.0.1:0` and closing the listener races: the OS hands out
//! that range for every outbound connection of every parallel test, so another socket can take
//! the port before the server binds it. A server with shards also binds the ports after it
//! (`port + shard`), and a replication listener does the same, so one free port is not enough.
//!
//! [`reserve`] instead hands out a block of [`BLOCK`] consecutive ports from a range below
//! every OS's ephemeral range (Linux starts at 32768, macOS and Windows at 49152), where no
//! outbound socket lands. A lock file per block keeps every other test, in this process or in
//! another test binary running at the same time, off the block until the reservation drops.
//! Each port is bound once before the block is handed out, so one held by an unrelated program
//! is skipped.
//!
//! This file is shared with the integration tests (`tests/common/mod.rs` includes it).

use std::{
    fs::{File, OpenOptions},
    net::{Ipv4Addr, SocketAddr, TcpListener},
    sync::atomic::{AtomicU32, Ordering},
};

/// Ports per block: one data port per shard, replication ports and spares.
pub const BLOCK: u16 = 16;

/// 20000..32000. Cluster bus ports (data port + 10000) are only advertised, never bound.
const FIRST: u16 = 20_000;
const BLOCKS: u32 = 750;

/// A block of consecutive ports this test owns until it drops.
pub struct PortBlock {
    base: u16,
    _lock: File,
}

impl PortBlock {
    /// The first port of the block.
    pub fn port(&self) -> u16 {
        self.base
    }

    /// The port `offset` places after the first; `offset` is below [`BLOCK`].
    pub fn port_at(&self, offset: u16) -> u16 {
        assert!(offset < BLOCK, "port offset {offset} is outside the block");
        self.base + offset
    }

    /// `127.0.0.1` and the port `offset` places after the first.
    pub fn address_at(&self, offset: u16) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, self.port_at(offset)))
    }
}

/// Reserves a block of [`BLOCK`] consecutive free ports.
pub fn reserve() -> PortBlock {
    static NEXT: AtomicU32 = AtomicU32::new(0);

    let directory = std::env::temp_dir().join("ruvio-test-ports");
    let _ = std::fs::create_dir_all(&directory);
    // Each process starts at its own place, so parallel test binaries rarely meet; the counter
    // walks forward, so a block a test just released (its ports maybe in TIME_WAIT) is the last
    // one this process takes again.
    let start = std::process::id().wrapping_mul(7919);

    for _ in 0..BLOCKS * 2 {
        let index = start.wrapping_add(NEXT.fetch_add(1, Ordering::Relaxed)) % BLOCKS;
        let base = FIRST + (index as u16) * BLOCK;
        // Not truncated: another holder's lock must stay intact on every OS.
        let Ok(lock) = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(directory.join(format!("{base}.lock")))
        else {
            continue;
        };

        if lock.try_lock().is_err() {
            continue;
        }

        if (0..BLOCK).all(|offset| TcpListener::bind((Ipv4Addr::LOCALHOST, base + offset)).is_ok())
        {
            return PortBlock { base, _lock: lock };
        }
    }

    panic!("no block of {BLOCK} free ports between {FIRST} and 32000");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_reservations_never_share_a_port() {
        let blocks: Vec<_> = (0..8).map(|_| reserve()).collect();

        for (index, block) in blocks.iter().enumerate() {
            for other in &blocks[index + 1..] {
                assert!(block.port().abs_diff(other.port()) >= BLOCK);
            }
        }
    }

    #[test]
    fn a_reserved_block_is_free_to_bind() {
        let block = reserve();

        for offset in 0..BLOCK {
            TcpListener::bind(block.address_at(offset)).unwrap();
        }
    }
}
