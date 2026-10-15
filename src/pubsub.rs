use std::sync::Arc;

use ahash::AHashMap;

/// Pub/Sub membership for one shard. The hub does not write sockets or the WAL.
/// Sharded channels use `smessage`. Classic channels and patterns stay on this shard.
#[derive(Debug, Default)]
pub struct PubSubHub {
    sharded: Membership,
    classic: Membership,
    patterns: Membership,
    /// Exact keyspace or keyevent channels per connection, for `notify_listeners`.
    notify_conns: AHashMap<u64, u32>,
}

/// One connection's names: its position in `members[name]` and its subscribe order.
type HeldNames = AHashMap<Arc<[u8]>, (usize, u64)>;

/// Who holds which name. Subscribe, unsubscribe and lookups cost O(1) whatever the counts.
#[derive(Debug, Default)]
struct Membership {
    /// Subscribers of each name, dense, so a publish walks one slice.
    members: AHashMap<Arc<[u8]>, Vec<u64>>,
    /// Names of each connection, with the connection's position in `members[name]` and
    /// the order it subscribed in.
    names: AHashMap<u64, HeldNames>,
    next_order: u64,
}

impl Membership {
    /// False when `conn` already holds `name`.
    fn add(&mut self, conn: u64, name: &[u8]) -> bool {
        let held = self.names.entry(conn).or_default();

        if held.contains_key(name) {
            return false;
        }

        let shared = match self.members.get_key_value(name) {
            Some((existing, _)) => Arc::clone(existing),
            None => Arc::from(name),
        };
        let list = self.members.entry(Arc::clone(&shared)).or_default();

        held.insert(shared, (list.len(), self.next_order));
        self.next_order += 1;
        list.push(conn);
        true
    }

    /// False when `conn` did not hold `name`.
    fn remove(&mut self, conn: u64, name: &[u8]) -> bool {
        let Some(held) = self.names.get_mut(&conn) else {
            return false;
        };

        let Some((position, _)) = held.remove(name) else {
            return false;
        };

        if held.is_empty() {
            self.names.remove(&conn);
        }

        let Some(list) = self.members.get_mut(name) else {
            return true;
        };

        list.swap_remove(position);

        if let Some(&moved) = list.get(position)
            && let Some(slot) = self
                .names
                .get_mut(&moved)
                .and_then(|held| held.get_mut(name))
        {
            slot.0 = position;
        }

        if list.is_empty() {
            self.members.remove(name);
        }

        true
    }

    fn count(&self, conn: u64) -> usize {
        self.names.get(&conn).map_or(0, |held| held.len())
    }

    /// The names `conn` holds, in the order it subscribed to them.
    fn names_of(&self, conn: u64) -> Vec<Arc<[u8]>> {
        let Some(held) = self.names.get(&conn) else {
            return Vec::new();
        };
        let mut names: Vec<(u64, &Arc<[u8]>)> = held
            .iter()
            .map(|(name, (_, order))| (*order, name))
            .collect();

        names.sort_unstable_by_key(|(order, _)| *order);
        names
            .into_iter()
            .map(|(_, name)| Arc::clone(name))
            .collect()
    }

    fn subscribers(&self, name: &[u8]) -> &[u64] {
        self.members.get(name).map(Vec::as_slice).unwrap_or(&[])
    }
}

impl PubSubHub {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&mut self, conn: u64, channel: &[u8]) -> usize {
        if self.sharded.add(conn, channel) && crate::notify::is_notify_channel(channel) {
            *self.notify_conns.entry(conn).or_default() += 1;
        }

        self.count(conn)
    }

    pub fn unsubscribe(&mut self, conn: u64, channel: Option<&[u8]>) -> Vec<(Vec<u8>, usize)> {
        let Some(channel) = channel else {
            let channels = self.sharded.names_of(conn);
            let mut replies = Vec::with_capacity(channels.len());

            for name in channels {
                self.remove_one(conn, &name);
                replies.push((name.to_vec(), self.count(conn)));
            }

            return replies;
        };

        self.remove_one(conn, channel);
        vec![(channel.to_vec(), self.count(conn))]
    }

    pub fn subscribers(&self, channel: &[u8]) -> &[u64] {
        self.sharded.subscribers(channel)
    }

    pub fn is_subscribed(&self, conn: u64) -> bool {
        self.count(conn) + self.classic_count(conn) > 0
    }

    pub fn subscribe_classic(&mut self, conn: u64, channel: &[u8]) -> usize {
        self.classic.add(conn, channel);
        self.classic_count(conn)
    }

    pub fn unsubscribe_classic(
        &mut self,
        conn: u64,
        channel: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, usize)> {
        let names = match channel {
            Some(name) => vec![Arc::from(name)],
            None => self.classic.names_of(conn),
        };
        let mut replies = Vec::with_capacity(names.len());

        for name in names {
            self.classic.remove(conn, &name);
            replies.push((name.to_vec(), self.classic_count(conn)));
        }

        replies
    }

    pub fn subscribe_pattern(&mut self, conn: u64, pattern: &[u8]) -> usize {
        self.patterns.add(conn, pattern);
        self.classic_count(conn)
    }

    pub fn unsubscribe_pattern(
        &mut self,
        conn: u64,
        pattern: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, usize)> {
        let names = match pattern {
            Some(name) => vec![Arc::from(name)],
            None => self.patterns.names_of(conn),
        };
        let mut replies = Vec::with_capacity(names.len());

        for name in names {
            self.patterns.remove(conn, &name);
            replies.push((name.to_vec(), self.classic_count(conn)));
        }

        replies
    }

    /// True when a classic publish to `channel` would reach nobody.
    pub fn classic_unheard(&self, channel: &[u8]) -> bool {
        self.patterns.members.is_empty() && !self.classic.members.contains_key(channel)
    }

    pub fn classic_frames(
        &self,
        channel: &[u8],
        message: &[u8],
    ) -> Vec<(u64, std::sync::Arc<[u8]>)> {
        let mut frames = Vec::new();
        let message_frame = std::sync::Arc::<[u8]>::from(encode_message(channel, message));

        for conn in self.classic.subscribers(channel) {
            frames.push((*conn, std::sync::Arc::clone(&message_frame)));
        }

        for (pattern, conns) in &self.patterns.members {
            if !crate::protocol::glob_match(pattern, channel) {
                continue;
            }

            let frame = std::sync::Arc::<[u8]>::from(encode_pmessage(pattern, channel, message));

            for conn in conns {
                frames.push((*conn, std::sync::Arc::clone(&frame)));
            }
        }

        frames
    }

    pub fn disconnect(&mut self, conn: u64) {
        let _ = self.unsubscribe(conn, None);
        let _ = self.unsubscribe_classic(conn, None);
        let _ = self.unsubscribe_pattern(conn, None);
    }

    /// Connections with at least one exact keyspace or keyevent channel.
    pub fn notify_listeners(&self) -> u32 {
        self.notify_conns.len() as u32
    }

    fn count(&self, conn: u64) -> usize {
        self.sharded.count(conn)
    }

    pub(crate) fn classic_count(&self, conn: u64) -> usize {
        self.classic.count(conn) + self.patterns.count(conn)
    }

    fn remove_one(&mut self, conn: u64, channel: &[u8]) -> bool {
        if !self.sharded.remove(conn, channel) {
            return false;
        }

        if crate::notify::is_notify_channel(channel)
            && let Some(count) = self.notify_conns.get_mut(&conn)
        {
            *count -= 1;

            if *count == 0 {
                self.notify_conns.remove(&conn);
            }
        }

        true
    }
}

pub fn encode_message(channel: &[u8], message: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();

    out.extend_from_slice(b"*3\r\n");
    write_bulk(&mut out, b"message");
    write_bulk(&mut out, channel);
    write_bulk(&mut out, message);
    out
}

pub fn encode_pmessage(pattern: &[u8], channel: &[u8], message: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();

    out.extend_from_slice(b"*4\r\n");
    write_bulk(&mut out, b"pmessage");
    write_bulk(&mut out, pattern);
    write_bulk(&mut out, channel);
    write_bulk(&mut out, message);
    out
}

pub fn encode_subscribe(channel: &[u8], count: usize) -> Vec<u8> {
    encode_kind_count(b"subscribe", Some(channel), count)
}

pub fn encode_unsubscribe(channel: Option<&[u8]>, count: usize) -> Vec<u8> {
    encode_kind_count(b"unsubscribe", channel, count)
}

pub fn encode_psubscribe(pattern: &[u8], count: usize) -> Vec<u8> {
    encode_kind_count(b"psubscribe", Some(pattern), count)
}

pub fn encode_punsubscribe(pattern: Option<&[u8]>, count: usize) -> Vec<u8> {
    encode_kind_count(b"punsubscribe", pattern, count)
}

pub fn encode_smessage(channel: &[u8], message: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();

    out.extend_from_slice(b"*3\r\n");
    write_bulk(&mut out, b"smessage");
    write_bulk(&mut out, channel);
    write_bulk(&mut out, message);
    out
}

pub fn encode_ssubscribe(channel: &[u8], count: usize) -> Vec<u8> {
    encode_kind_count(b"ssubscribe", Some(channel), count)
}

pub fn encode_sunsubscribe(channel: Option<&[u8]>, count: usize) -> Vec<u8> {
    encode_kind_count(b"sunsubscribe", channel, count)
}

#[cfg(test)]
pub fn encode_pubsub_pong(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();

    out.extend_from_slice(b"*2\r\n");
    write_bulk(&mut out, b"pong");
    write_bulk(&mut out, payload);
    out
}

fn encode_kind_count(kind: &[u8], channel: Option<&[u8]>, count: usize) -> Vec<u8> {
    let mut out = Vec::new();

    out.extend_from_slice(b"*3\r\n");
    write_bulk(&mut out, kind);
    match channel {
        Some(channel) => write_bulk(&mut out, channel),
        None => out.extend_from_slice(b"$-1\r\n"),
    }

    out.extend_from_slice(format!(":{count}\r\n").as_bytes());
    out
}

fn write_bulk(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}

/// Frames a change-feed reader keeps queued before it stops reading its source.
pub const PUBSUB_QUEUE: usize = 32;

/// A subscriber that falls this many queued bytes behind is disconnected.
pub const PUBSUB_QUEUE_BYTES: usize = 32 * 1024 * 1024;

pub fn subscription_wire(
    hub: &mut PubSubHub,
    conn: u64,
    channels: &[Vec<u8>],
    subscribe: bool,
) -> (Vec<u8>, bool) {
    let mut wire = Vec::new();

    if subscribe {
        for channel in channels {
            let count = hub.subscribe(conn, channel);

            wire.extend(encode_ssubscribe(channel, count));
        }
    } else if channels.is_empty() {
        let replies = hub.unsubscribe(conn, None);

        if replies.is_empty() {
            wire.extend(encode_sunsubscribe(None, 0));
        } else {
            for (name, count) in replies {
                wire.extend(encode_sunsubscribe(Some(&name), count));
            }
        }
    } else {
        for channel in channels {
            let replies = hub.unsubscribe(conn, Some(channel));

            for (name, count) in replies {
                wire.extend(encode_sunsubscribe(Some(&name), count));
            }
        }
    }

    (wire, hub.is_subscribed(conn))
}

pub fn deliver_frames<F>(
    hub: &mut PubSubHub,
    frames: Vec<(u64, F)>,
    send: impl Fn(u64, F) -> bool,
) -> i64 {
    let mut delivered = 0i64;
    let mut dropped = Vec::new();

    for (conn, frame) in frames {
        if send(conn, frame) {
            delivered += 1;
        } else {
            dropped.push(conn);
        }
    }

    for conn in dropped {
        hub.disconnect(conn);
    }

    delivered
}

#[derive(Clone, Copy)]
pub enum ClassicSub {
    Channel,
    Pattern,
}

pub fn classic_subscription_wire(
    hub: &mut PubSubHub,
    conn: u64,
    names: &[Vec<u8>],
    kind: ClassicSub,
    subscribe: bool,
) -> (Vec<u8>, bool) {
    let mut wire = Vec::new();

    if subscribe {
        for name in names {
            let count = match kind {
                ClassicSub::Channel => hub.subscribe_classic(conn, name),
                ClassicSub::Pattern => hub.subscribe_pattern(conn, name),
            };

            wire.extend(match kind {
                ClassicSub::Channel => encode_subscribe(name, count),
                ClassicSub::Pattern => encode_psubscribe(name, count),
            });
        }
    } else if names.is_empty() {
        let replies = match kind {
            ClassicSub::Channel => hub.unsubscribe_classic(conn, None),
            ClassicSub::Pattern => hub.unsubscribe_pattern(conn, None),
        };

        if replies.is_empty() {
            wire.extend(match kind {
                ClassicSub::Channel => encode_unsubscribe(None, hub.classic_count(conn)),
                ClassicSub::Pattern => encode_punsubscribe(None, hub.classic_count(conn)),
            });
        } else {
            for (name, count) in replies {
                wire.extend(match kind {
                    ClassicSub::Channel => encode_unsubscribe(Some(&name), count),
                    ClassicSub::Pattern => encode_punsubscribe(Some(&name), count),
                });
            }
        }
    } else {
        for name in names {
            let replies = match kind {
                ClassicSub::Channel => hub.unsubscribe_classic(conn, Some(name)),
                ClassicSub::Pattern => hub.unsubscribe_pattern(conn, Some(name)),
            };

            for (item, count) in replies {
                wire.extend(match kind {
                    ClassicSub::Channel => encode_unsubscribe(Some(&item), count),
                    ClassicSub::Pattern => encode_punsubscribe(Some(&item), count),
                });
            }
        }
    }

    (wire, hub.is_subscribed(conn))
}

pub fn deliver(
    hub: &mut PubSubHub,
    channel: &[u8],
    frame: Vec<u8>,
    send: impl Fn(u64, std::sync::Arc<[u8]>) -> bool,
) -> i64 {
    let targets = hub.subscribers(channel).to_vec();
    let shared = std::sync::Arc::<[u8]>::from(frame);
    let mut delivered = 0i64;
    let mut dropped = Vec::new();

    for conn in targets {
        if send(conn, std::sync::Arc::clone(&shared)) {
            delivered += 1;
        } else {
            dropped.push(conn);
        }
    }

    for conn in dropped {
        hub.disconnect(conn);
    }

    delivered
}

#[cfg(test)]
mod tests {
    use super::{
        PubSubHub, encode_pubsub_pong, encode_smessage, encode_ssubscribe, encode_sunsubscribe,
    };

    #[test]
    fn subscribe_publish_and_unsubscribe_track_clients_once() {
        let mut hub = PubSubHub::new();

        assert!(hub.subscribers(b"news").is_empty());

        assert_eq!(hub.subscribe(1, b"news"), 1);
        assert_eq!(hub.subscribe(1, b"news"), 1);
        assert_eq!(hub.subscribe(2, b"news"), 1);
        assert_eq!(hub.subscribe(2, b"other"), 2);
        assert_eq!(hub.subscribers(b"news"), &[1, 2]);
        assert_eq!(hub.subscribers(b"other"), &[2]);

        assert_eq!(
            hub.unsubscribe(1, Some(b"news")),
            vec![(b"news".to_vec(), 0)]
        );
        assert_eq!(hub.subscribers(b"news"), &[2]);
        assert!(!hub.is_subscribed(1));

        assert_eq!(
            hub.unsubscribe(2, None),
            vec![(b"news".to_vec(), 1), (b"other".to_vec(), 0)]
        );
        assert!(hub.subscribers(b"news").is_empty());
        assert!(!hub.is_subscribed(2));
    }

    #[test]
    fn unsubscribe_of_a_missing_channel_keeps_the_count() {
        let mut hub = PubSubHub::new();

        hub.subscribe(4, b"a");
        assert_eq!(
            hub.unsubscribe(4, Some(b"missing")),
            vec![(b"missing".to_vec(), 1)]
        );
        assert_eq!(hub.subscribers(b"a"), &[4]);
    }

    #[test]
    fn a_many_star_pattern_does_not_stall_publish() {
        let mut hub = PubSubHub::new();
        let channel = [b'a'; 80];
        let started = std::time::Instant::now();

        hub.subscribe_pattern(7, b"*a*a*a*a*a*a*b");
        hub.subscribe_pattern(8, b"a[ab]*");

        let frames = hub.classic_frames(&channel, b"m");

        assert_eq!(
            frames.iter().map(|(conn, _)| *conn).collect::<Vec<_>>(),
            [8]
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn pubsub_frames_stay_resp2_arrays() {
        assert_eq!(
            encode_ssubscribe(b"news", 1),
            b"*3\r\n$10\r\nssubscribe\r\n$4\r\nnews\r\n:1\r\n"
        );
        assert_eq!(
            encode_smessage(b"news", b"hello"),
            b"*3\r\n$8\r\nsmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n"
        );
        assert_eq!(encode_pubsub_pong(b""), b"*2\r\n$4\r\npong\r\n$0\r\n\r\n");
        assert_eq!(
            encode_sunsubscribe(None, 0),
            b"*3\r\n$12\r\nsunsubscribe\r\n$-1\r\n:0\r\n"
        );
    }

    #[test]
    fn disconnect_removes_every_subscription() {
        let mut hub = PubSubHub::new();

        hub.subscribe(7, b"a");
        hub.subscribe(7, b"b");
        hub.subscribe(8, b"a");
        hub.disconnect(7);
        assert_eq!(hub.subscribers(b"a"), &[8]);
        assert!(hub.subscribers(b"b").is_empty());
        assert!(!hub.is_subscribed(7));
    }

    /// PUBLISH reaches exact SUBSCRIBE clients and matching PSUBSCRIBE patterns on this shard.
    #[test]
    fn publish_reaches_exact_and_pattern_subscribers() {
        let mut hub = PubSubHub::new();

        assert_eq!(hub.subscribe_classic(1, b"news"), 1);
        assert_eq!(hub.subscribe_pattern(2, b"news.*"), 1);
        assert_eq!(hub.subscribe_pattern(2, b"other"), 2);
        hub.subscribe(3, b"news");

        let frames = hub.classic_frames(b"news.1", b"hello");
        let targets: Vec<u64> = frames.iter().map(|(conn, _)| *conn).collect();

        assert_eq!(targets, vec![2]);
        assert!(frames[0].1.windows(8).any(|window| window == b"pmessage"));
        assert!(
            hub.classic_frames(b"news", b"hello")
                .iter()
                .any(|(conn, _)| *conn == 1)
        );
        assert!(hub.subscribers(b"news").contains(&3));
        assert!(
            hub.classic_frames(b"news", b"hello")
                .iter()
                .all(|(conn, _)| *conn != 3)
        );

        hub.disconnect(2);
        assert!(!hub.is_subscribed(2));
        assert!(hub.is_subscribed(1));
    }

    #[test]
    fn membership_keeps_positions_through_swaps_and_counts_notify_listeners() {
        let mut hub = PubSubHub::new();

        for conn in 0..1_000 {
            hub.subscribe(conn, b"wide");
        }

        for channel in 0..1_000 {
            hub.subscribe(5_000, format!("ch:{channel}").as_bytes());
        }

        for conn in (0..1_000).step_by(3) {
            hub.unsubscribe(conn, Some(b"wide"));
        }

        let mut left = hub.subscribers(b"wide").to_vec();

        left.sort_unstable();
        assert_eq!(
            left,
            (0..1_000).filter(|conn| conn % 3 != 0).collect::<Vec<_>>()
        );

        for conn in 0..1_000 {
            hub.disconnect(conn);
        }

        assert!(hub.subscribers(b"wide").is_empty());
        assert_eq!(hub.unsubscribe(5_000, None).len(), 1_000);
        assert!(!hub.is_subscribed(5_000));

        assert_eq!(hub.notify_listeners(), 0);
        hub.subscribe(1, b"__keyspace@0__:a");
        hub.subscribe(1, b"__keyevent@0__:set");
        hub.subscribe(1, b"plain");
        hub.subscribe(2, b"__keyspace@0__:a");
        assert_eq!(hub.notify_listeners(), 2);
        hub.unsubscribe(1, Some(b"__keyspace@0__:a"));
        assert_eq!(hub.notify_listeners(), 2);
        hub.disconnect(1);
        assert_eq!(hub.notify_listeners(), 1);
        hub.unsubscribe(2, None);
        assert_eq!(hub.notify_listeners(), 0);
    }
}
