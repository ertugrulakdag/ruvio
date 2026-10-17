//! Growth guards: one command over many elements, or many commands over one growing value,
//! must stay close to linear. Each case runs at `n` and `8n` and compares best timings:
//! linear work grows about 8x, quadratic work about 64x, and the bound sits between them.
//! Ratios of the thread's CPU time, not wall time, so a slow or busy machine does not fail
//! them.

use super::*;

use std::time::Duration;

const SMALL: usize = 1_000;
const FACTOR: usize = 8;
/// Linear is ~8x (a log factor adds a little), quadratic ~64x.
const MAX_RATIO: f64 = 24.0;
/// Below this the large run is too short to time; a quadratic case is far above it.
const MEASURABLE: Duration = Duration::from_millis(3);

fn run(shard: &mut KvShard, parts: &[Vec<u8>]) {
    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    let reply = execute(shard, command_of(&refs)).into_owned();

    assert!(
        !matches!(reply, Response::Error(_)),
        "{}: {reply:?}",
        String::from_utf8_lossy(&parts[0])
    );
}

fn words(head: &[&str]) -> Vec<Vec<u8>> {
    head.iter().map(|word| word.as_bytes().to_vec()).collect()
}

/// Builds the state for `n` elements and the commands to time on it.
type Prepare<'p> = &'p dyn Fn(usize) -> (KvShard, Vec<Vec<Vec<u8>>>);

/// CPU time of this thread: other processes competing for the cores do not add to it.
#[cfg(unix)]
#[allow(unsafe_code)]
fn thread_time() -> Duration {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };

    // SAFETY: `now` is a valid timespec for the call to fill.
    let status = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut now) };

    assert_eq!(status, 0);
    Duration::new(now.tv_sec as u64, now.tv_nsec as u32)
}

/// CPU time of this thread. Windows counts it in clock ticks of about 15.6 ms
/// (`GetThreadTimes`), far too coarse for these runs, but `QueryThreadCycleTime` counts the
/// cycles the thread ran with no such step. The cycles are converted with a rate measured
/// against the wall clock while this thread spins: losing the CPU during that spin only makes
/// the rate lower, so the best of a few spins is kept. Only `MEASURABLE` depends on the rate;
/// the ratios between runs do not.
#[cfg(windows)]
#[allow(unsafe_code)]
fn thread_time() -> Duration {
    use std::{ffi::c_void, sync::OnceLock, time::Instant};

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentThread() -> *mut c_void;
        fn QueryThreadCycleTime(thread: *mut c_void, cycles: *mut u64) -> i32;
    }

    fn cycles() -> u64 {
        let mut cycles = 0u64;

        // SAFETY: the pseudo handle of the calling thread needs no closing, and `cycles` is
        // a valid u64 for the call to fill.
        let status = unsafe { QueryThreadCycleTime(GetCurrentThread(), &mut cycles) };

        assert_ne!(status, 0, "QueryThreadCycleTime failed");
        cycles
    }

    static CYCLES_PER_SECOND: OnceLock<f64> = OnceLock::new();

    let rate = *CYCLES_PER_SECOND.get_or_init(|| {
        (0..3)
            .map(|_| {
                let wall = Instant::now();
                let first = cycles();

                while wall.elapsed() < Duration::from_millis(10) {
                    std::hint::spin_loop();
                }

                let spun = cycles().saturating_sub(first) as f64;

                spun / wall.elapsed().as_secs_f64()
            })
            .fold(1.0, f64::max)
    });

    Duration::from_secs_f64(cycles() as f64 / rate)
}

#[cfg(not(any(unix, windows)))]
fn thread_time() -> Duration {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

    START.get_or_init(std::time::Instant::now).elapsed()
}

/// Only the commands are timed.
fn time_once(n: usize, prepare: Prepare<'_>) -> Duration {
    let (mut shard, commands) = prepare(n);
    let started = thread_time();

    for command in &commands {
        run(&mut shard, command);
    }

    let elapsed = thread_time().saturating_sub(started);
    // Dropping a large shard is not part of the measured work.
    drop(shard);
    elapsed
}

fn assert_linear(name: &str, prepare: Prepare<'_>) {
    let small = (0..3).map(|_| time_once(SMALL, prepare)).min().unwrap();
    let mut large = Duration::MAX;

    // A second try only when the first looks slow, so a passing case runs once.
    for _ in 0..2 {
        large = large.min(time_once(SMALL * FACTOR, prepare));
        if large < MEASURABLE || large.as_secs_f64() < small.as_secs_f64() * MAX_RATIO {
            return;
        }
    }

    panic!(
        "{name}: {FACTOR}x the elements took {:.1}x the time ({small:?} -> {large:?})",
        large.as_secs_f64() / small.as_secs_f64().max(1e-9)
    );
}

fn member(index: usize) -> Vec<u8> {
    format!("member-{index:08}").into_bytes()
}

/// A shard holding `key` as a small collection built by `head` (kept in its compact form).
fn with_small(head: &[&str], pair: bool) -> KvShard {
    let mut shard = KvShard::new();
    let mut parts = words(head);

    for index in 0..100 {
        if pair {
            parts.push(b"1".to_vec());
        }
        parts.push(format!("seed-{index}").into_bytes());
    }
    run(&mut shard, &parts);
    shard
}

fn variadic(head: &[&str], n: usize, pair: Option<&[u8]>, value_first: bool) -> Vec<Vec<u8>> {
    let mut parts = words(head);

    for index in 0..n {
        match pair {
            Some(other) if value_first => {
                parts.push(other.to_vec());
                parts.push(member(index));
            }
            Some(other) => {
                parts.push(member(index));
                parts.push(other.to_vec());
            }
            None => parts.push(member(index)),
        }
    }

    parts
}

#[test]
fn one_command_over_many_members_stays_linear() {
    // ZADD of many new members onto a small set, and with every member given twice.
    assert_linear("ZADD many onto a small set", &|n| {
        (
            with_small(&["ZADD", "k"], true),
            vec![variadic(&["ZADD", "k"], n, Some(b"2"), true)],
        )
    });
    assert_linear("ZADD with repeated members", &|n| {
        let mut parts = variadic(&["ZADD", "k"], n / 2, Some(b"2"), true);

        parts.extend(variadic(&[], n / 2, Some(b"3"), true));
        (KvShard::new(), vec![parts])
    });
    assert_linear("SADD many onto a small set", &|n| {
        (
            with_small(&["SADD", "k"], false),
            vec![variadic(&["SADD", "k"], n, None, false)],
        )
    });
    assert_linear("HSET many fields", &|n| {
        (
            KvShard::new(),
            vec![variadic(&["HSET", "k"], n, Some(b"v"), false)],
        )
    });
    assert_linear("RPUSH many", &|n| {
        (
            KvShard::new(),
            vec![variadic(&["RPUSH", "k"], n, None, false)],
        )
    });
    assert_linear("MSET many keys", &|n| {
        (
            KvShard::new(),
            vec![variadic(&["MSET"], n, Some(b"v"), false)],
        )
    });

    // Removing many members of a collection that holds them.
    for (name, add, remove, pair) in [
        ("SREM many", ["SADD", "k"], ["SREM", "k"], None),
        ("HDEL many", ["HSET", "k"], ["HDEL", "k"], Some(&b"v"[..])),
        ("ZREM many", ["ZADD", "k"], ["ZREM", "k"], Some(&b"1"[..])),
    ] {
        assert_linear(name, &|n| {
            let mut shard = KvShard::new();

            run(&mut shard, &variadic(&add, n, pair, add[0] == "ZADD"));
            (shard, vec![variadic(&remove, n, None, false)])
        });
    }

    assert_linear("DEL many keys", &|n| {
        let mut shard = KvShard::new();

        run(&mut shard, &variadic(&["MSET"], n, Some(b"v"), false));
        (shard, vec![variadic(&["DEL"], n, None, false)])
    });
    assert_linear("SUNIONSTORE of two large sets", &|n| {
        let mut shard = KvShard::new();
        let mut other = variadic(&["SADD", "b"], n, None, false);

        run(&mut shard, &variadic(&["SADD", "a"], n, None, false));
        other.iter_mut().skip(2).for_each(|word| word.push(b'x'));
        run(&mut shard, &other);
        (shard, vec![words(&["SUNIONSTORE", "d", "a", "b"])])
    });
    assert_linear("HEXPIRE over many fields", &|n| {
        let mut shard = KvShard::new();
        let count = n.to_string();

        run(&mut shard, &variadic(&["HSET", "k"], n, Some(b"v"), false));
        let expire = variadic(&["HEXPIRE", "k", "1000", "FIELDS", &count], n, None, false);

        (shard, vec![expire])
    });
    assert_linear("HTTL over many fields", &|n| {
        let mut shard = KvShard::new();
        let count = n.to_string();

        run(&mut shard, &variadic(&["HSET", "k"], n, Some(b"v"), false));
        run(
            &mut shard,
            &variadic(&["HEXPIRE", "k", "1000", "FIELDS", &count], n, None, false),
        );
        let ttl = variadic(&["HTTL", "k", "FIELDS", &count], n, None, false);

        (shard, vec![ttl])
    });
    assert_linear("HPERSIST over many fields", &|n| {
        let mut shard = KvShard::new();
        let count = n.to_string();

        run(&mut shard, &variadic(&["HSET", "k"], n, Some(b"v"), false));
        run(
            &mut shard,
            &variadic(&["HEXPIRE", "k", "1000", "FIELDS", &count], n, None, false),
        );
        let persist = variadic(&["HPERSIST", "k", "FIELDS", &count], n, None, false);

        (shard, vec![persist])
    });
    assert_linear("HDEL of fields with deadlines", &|n| {
        let mut shard = KvShard::new();
        let count = n.to_string();

        run(&mut shard, &variadic(&["HSET", "k"], n, Some(b"v"), false));
        run(
            &mut shard,
            &variadic(&["HEXPIRE", "k", "1000", "FIELDS", &count], n, None, false),
        );
        (shard, vec![variadic(&["HDEL", "k"], n, None, false)])
    });
}

#[test]
fn many_commands_on_one_growing_value_stay_linear() {
    assert_linear("APPEND to one string", &|n| {
        let chunk = vec![b'x'; 64];
        let command = vec![b"APPEND".to_vec(), b"k".to_vec(), chunk];

        (KvShard::new(), vec![command; n])
    });
    assert_linear("SETRANGE past the end of one string", &|n| {
        let commands = (0..n)
            .map(|index| {
                vec![
                    b"SETRANGE".to_vec(),
                    b"k".to_vec(),
                    (index * 64).to_string().into_bytes(),
                    vec![b'y'; 64],
                ]
            })
            .collect();

        (KvShard::new(), commands)
    });
    assert_linear("ZADD one member at a time", &|n| {
        let commands = (0..n)
            .map(|index| {
                vec![
                    b"ZADD".to_vec(),
                    b"k".to_vec(),
                    index.to_string().into_bytes(),
                    member(index),
                ]
            })
            .collect();

        (KvShard::new(), commands)
    });
    assert_linear("HSET one field at a time", &|n| {
        let commands = (0..n)
            .map(|index| {
                vec![
                    b"HSET".to_vec(),
                    b"k".to_vec(),
                    member(index),
                    b"v".to_vec(),
                ]
            })
            .collect();

        (KvShard::new(), commands)
    });
    assert_linear("XADD one entry at a time", &|n| {
        let commands = (0..n)
            .map(|_| {
                vec![
                    b"XADD".to_vec(),
                    b"k".to_vec(),
                    b"*".to_vec(),
                    b"f".to_vec(),
                    b"v".to_vec(),
                ]
            })
            .collect();

        (KvShard::new(), commands)
    });
    assert_linear("FLUSHALL after many keys", &|n| {
        let mut shard = KvShard::new();

        run(&mut shard, &variadic(&["MSET"], n * 4, Some(b"v"), false));
        (shard, vec![words(&["FLUSHALL"])])
    });
}
