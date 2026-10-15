//! Iteration under change, through the commands clients send. For every cursor command
//! (SCAN, HSCAN, SSCAN over string and integer sets, ZSCAN) a seeded random walk starts at
//! sizes on both sides of every compact-encoding limit, and between pages adds, removes and
//! bulk-grows or empties transient elements, so the table converts, grows, migrates and
//! shrinks under the cursor. Every walk must end, return every element that was present for
//! the whole walk, and return nothing that never existed. Random draws (SRANDMEMBER,
//! HRANDFIELD, ZRANDMEMBER, RANDOMKEY) must reach every element with similar frequency.

use super::*;

use ahash::{AHashMap, AHashSet};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

#[derive(Clone, Copy, Debug)]
enum Kind {
    Keyspace,
    Hash,
    Set,
    IntSet,
    SortedSet,
}

impl Kind {
    /// Element name: integer sets hold numbers, so stable ones are even and transient odd.
    fn element(self, stable: bool, index: u64) -> Vec<u8> {
        match self {
            Kind::IntSet => (index * 2 + u64::from(!stable)).to_string().into_bytes(),
            _ if stable => format!("s{index}").into_bytes(),
            _ => format!("t{index}").into_bytes(),
        }
    }

    fn add(self, shard: &mut KvShard, elements: &[Vec<u8>]) {
        if elements.is_empty() {
            return;
        }

        let mut parts: Vec<&[u8]> = match self {
            Kind::Keyspace => vec![b"MSET"],
            Kind::Hash => vec![b"HSET", b"k"],
            Kind::Set | Kind::IntSet => vec![b"SADD", b"k"],
            Kind::SortedSet => vec![b"ZADD", b"k"],
        };

        for element in elements {
            match self {
                Kind::Keyspace | Kind::Hash => {
                    parts.push(element);
                    parts.push(b"v");
                }
                Kind::SortedSet => {
                    parts.push(b"1");
                    parts.push(element);
                }
                Kind::Set | Kind::IntSet => parts.push(element),
            }
        }

        let reply = execute(shard, command_of(&parts)).into_owned();

        assert!(!matches!(reply, Response::Error(_)), "{self:?}: {reply:?}");
    }

    fn remove(self, shard: &mut KvShard, elements: &[Vec<u8>]) {
        if elements.is_empty() {
            return;
        }

        let mut parts: Vec<&[u8]> = match self {
            Kind::Keyspace => vec![b"DEL"],
            Kind::Hash => vec![b"HDEL", b"k"],
            Kind::Set | Kind::IntSet => vec![b"SREM", b"k"],
            Kind::SortedSet => vec![b"ZREM", b"k"],
        };

        parts.extend(elements.iter().map(Vec::as_slice));
        let reply = execute(shard, command_of(&parts)).into_owned();

        assert!(!matches!(reply, Response::Error(_)), "{self:?}: {reply:?}");
    }

    /// One page: the next cursor and the element names (values and scores dropped).
    fn page(
        self,
        shard: &mut KvShard,
        cursor: u64,
        count: u64,
        pattern: Option<&[u8]>,
    ) -> (u64, Vec<Vec<u8>>) {
        let cursor = cursor.to_string().into_bytes();
        let count = count.to_string().into_bytes();
        let mut parts: Vec<&[u8]> = match self {
            Kind::Keyspace => vec![b"SCAN", &cursor],
            Kind::Hash => vec![b"HSCAN", b"k", &cursor],
            Kind::Set | Kind::IntSet => vec![b"SSCAN", b"k", &cursor],
            Kind::SortedSet => vec![b"ZSCAN", b"k", &cursor],
        };

        parts.extend([b"COUNT".as_slice(), &count]);
        if let Some(pattern) = pattern {
            parts.extend([b"MATCH".as_slice(), pattern]);
        }

        let Response::Scan { cursor, keys } = execute(shard, command_of(&parts)) else {
            panic!("{self:?}: not a scan reply");
        };
        let paired = matches!(self, Kind::Hash | Kind::SortedSet);
        let names = keys
            .iter()
            .enumerate()
            .filter(|(index, _)| !paired || index % 2 == 0)
            .map(|(_, name)| name.to_vec())
            .collect();

        (cursor, names)
    }
}

fn glob_prefix_matches(pattern: Option<&[u8]>, name: &[u8]) -> bool {
    match pattern {
        None => true,
        Some(pattern) => {
            let prefix = pattern.strip_suffix(b"*").expect("prefix patterns only");

            name.starts_with(prefix)
        }
    }
}

/// One walk; panics with the seed on any broken invariant.
fn walk(kind: Kind, seed: u64) {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut shard = KvShard::new();
    // Sizes on both sides of the compact limits (hash 64, list-pack sets 128, intset 512).
    let stable_count = [3u64, 40, 70, 200, 700, 3_000][rng.below(6) as usize];
    let count = [1u64, 5, 10, 100][rng.below(4) as usize];
    let pattern: Option<&[u8]> = match (kind, rng.below(3)) {
        (Kind::IntSet, _) | (_, 0 | 1) => None,
        (_, _) => Some(b"s1*"),
    };
    let stable: Vec<Vec<u8>> = (0..stable_count)
        .map(|index| kind.element(true, index))
        .collect();

    kind.add(&mut shard, &stable);

    let mut next_transient = 0u64;
    let mut live_transients: Vec<Vec<u8>> = Vec::new();
    let mut ever: AHashSet<Vec<u8>> = stable.iter().cloned().collect();
    let mut seen: AHashSet<Vec<u8>> = AHashSet::new();
    let mut cursor = 0u64;
    let mut pages = 0u64;
    // Generous: every element at COUNT 1 several times over, plus the bursts.
    let page_bound = 20 * (stable_count + 4_000) / count + 1_000;

    loop {
        let (next, names) = kind.page(&mut shard, cursor, count, pattern);

        for name in names {
            assert!(
                ever.contains(&name),
                "{kind:?} seed {seed}: returned {:?}, which never existed",
                String::from_utf8_lossy(&name)
            );
            assert!(
                glob_prefix_matches(pattern, &name),
                "{kind:?} seed {seed}: {:?} does not match the pattern",
                String::from_utf8_lossy(&name)
            );
            seen.insert(name);
        }

        pages += 1;
        assert!(
            pages < page_bound,
            "{kind:?} seed {seed}: no end after {pages} pages (size {stable_count}, COUNT {count})"
        );
        if next == 0 {
            break;
        }
        cursor = next;

        // Churn between pages.
        let mut added = Vec::new();

        for _ in 0..rng.below(8) {
            added.push(kind.element(false, next_transient));
            next_transient += 1;
        }
        match rng.below(20) {
            // A burst far past every compact limit: conversion and table growth.
            0 => {
                for _ in 0..1_500 {
                    added.push(kind.element(false, next_transient));
                    next_transient += 1;
                }
            }
            // Every transient goes: the table shrinks back.
            1 => {
                kind.remove(&mut shard, &live_transients);
                live_transients.clear();
            }
            _ => {}
        }
        ever.extend(added.iter().cloned());
        kind.add(&mut shard, &added);
        live_transients.extend(added);

        let removals = rng.below(6).min(live_transients.len() as u64);
        let mut removed = Vec::new();

        for _ in 0..removals {
            let index = rng.below(live_transients.len() as u64) as usize;

            removed.push(live_transients.swap_remove(index));
        }
        kind.remove(&mut shard, &removed);
    }

    for element in &stable {
        if glob_prefix_matches(pattern, element) {
            assert!(
                seen.contains(element),
                "{kind:?} seed {seed}: stable {:?} was never returned (size {stable_count}, COUNT {count}, {pages} pages)",
                String::from_utf8_lossy(element)
            );
        }
    }
}

#[test]
fn every_scan_command_ends_and_misses_no_stable_element_under_random_churn() {
    for kind in [
        Kind::Keyspace,
        Kind::Hash,
        Kind::Set,
        Kind::IntSet,
        Kind::SortedSet,
    ] {
        for seed in 1..=12 {
            walk(kind, seed);
        }
    }
}

/// Draw counts per element stay within a factor of three of the mean, and every element
/// is drawn.
fn assert_uniform(what: &str, draws: &[Vec<u8>], members: &[Vec<u8>]) {
    let mut counts: AHashMap<&[u8], usize> = AHashMap::new();

    for draw in draws {
        assert!(
            members.contains(draw),
            "{what}: drew {:?}, not a member",
            String::from_utf8_lossy(draw)
        );
        *counts.entry(draw).or_default() += 1;
    }

    let expected = draws.len() / members.len();

    assert_eq!(
        counts.len(),
        members.len(),
        "{what}: not every member drawn"
    );
    for (member, count) in counts {
        assert!(
            count > expected / 3 && count < expected * 3,
            "{what}: {:?} drawn {count} times, mean {expected}",
            String::from_utf8_lossy(member)
        );
    }
}

fn bulk_names(response: Response<'_>, paired: bool) -> Vec<Vec<u8>> {
    let names: Vec<Vec<u8>> = match response {
        Response::Bulk(Some(name)) => vec![name.into_owned()],
        Response::Array(values) => values
            .into_iter()
            .map(|value| value.expect("member").into_owned())
            .collect(),
        Response::Set(values) => values.into_iter().map(Cow::into_owned).collect(),
        Response::Exec(values) => values
            .into_iter()
            .map(|value| match value {
                Response::Bulk(Some(name)) => name.into_owned(),
                other => panic!("unexpected draw {other:?}"),
            })
            .collect(),
        Response::Map(pairs) => pairs
            .into_iter()
            .map(|(name, _)| name.into_owned())
            .collect(),
        Response::Scored(pairs, _) => pairs.into_iter().map(|(name, _)| name.to_vec()).collect(),
        other => panic!("unexpected draw reply {other:?}"),
    };

    if paired {
        names.into_iter().step_by(2).collect()
    } else {
        names
    }
}

#[test]
fn random_draws_reach_every_element_evenly_for_every_encoding() {
    for size in [8usize, 100, 700] {
        let members: Vec<Vec<u8>> = (0..size)
            .map(|index| format!("m{index:04}").into_bytes())
            .collect();
        let ints: Vec<Vec<u8>> = (0..size)
            .map(|index| index.to_string().into_bytes())
            .collect();
        let mut shard = KvShard::new();

        Kind::Set.add(&mut shard, &members);
        let mut add = |kind: Kind, key: &[u8], elements: &[Vec<u8>]| {
            let mut parts: Vec<&[u8]> = match kind {
                Kind::Hash => vec![b"HSET", key],
                Kind::SortedSet => vec![b"ZADD", key],
                _ => vec![b"SADD", key],
            };

            for element in elements {
                match kind {
                    Kind::Hash => parts.extend([element.as_slice(), b"v"]),
                    Kind::SortedSet => parts.extend([b"1".as_slice(), element]),
                    _ => parts.push(element),
                }
            }
            assert!(!matches!(
                execute(&mut shard, command_of(&parts)),
                Response::Error(_)
            ));
        };

        add(Kind::Hash, b"h", &members);
        add(Kind::SortedSet, b"z", &members);
        add(Kind::IntSet, b"i", &ints);

        let draws = size * 120;

        for (verb, key, pool) in [
            (b"SRANDMEMBER".as_slice(), b"k".as_slice(), &members),
            (b"SRANDMEMBER", b"i", &ints),
            (b"HRANDFIELD", b"h", &members),
            (b"ZRANDMEMBER", b"z", &members),
        ] {
            let what = format!("{} size {size}", String::from_utf8_lossy(verb));

            // Single draws.
            let singles: Vec<Vec<u8>> = (0..draws)
                .flat_map(|_| bulk_names(execute(&mut shard, command_of(&[verb, key])), false))
                .collect();
            assert_uniform(&format!("{what} single"), &singles, pool);

            // A negative count draws with repetition, each draw independent.
            let negative = format!("-{}", size * 4);
            let repeated: Vec<Vec<u8>> = (0..30)
                .flat_map(|_| {
                    bulk_names(
                        execute(&mut shard, command_of(&[verb, key, negative.as_bytes()])),
                        false,
                    )
                })
                .collect();
            assert_eq!(
                repeated.len(),
                30 * size * 4,
                "{what}: negative count length"
            );
            assert_uniform(&format!("{what} negative"), &repeated, pool);

            // A positive count draws distinct members, and different ones each time.
            let half = (size / 2).to_string();
            let mut union = AHashSet::new();

            for _ in 0..40 {
                let picked = bulk_names(
                    execute(&mut shard, command_of(&[verb, key, half.as_bytes()])),
                    false,
                );
                let distinct: AHashSet<&Vec<u8>> = picked.iter().collect();

                assert_eq!(distinct.len(), size / 2, "{what}: positive count repeats");
                union.extend(picked);
            }
            assert_eq!(
                union.len(),
                size,
                "{what}: positive counts stick to some members"
            );
        }
    }
}

#[test]
fn randomkey_reaches_every_key_evenly() {
    for size in [10usize, 400] {
        let mut shard = KvShard::new();
        let keys: Vec<Vec<u8>> = (0..size)
            .map(|index| format!("key{index}").into_bytes())
            .collect();

        Kind::Keyspace.add(&mut shard, &keys);

        let draws: Vec<Vec<u8>> = (0..size * 120)
            .flat_map(|_| bulk_names(execute(&mut shard, command_of(&[b"RANDOMKEY"])), false))
            .collect();

        assert_uniform(&format!("RANDOMKEY size {size}"), &draws, &keys);
    }
}
