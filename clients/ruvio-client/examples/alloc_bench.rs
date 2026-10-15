//! Allocations and time per command, codec cost, and serial/concurrent throughput.
//!
//! `RUVIO_ADDR=127.0.0.1:6379 cargo run --release --example alloc_bench`
//!
//! Runs in a few seconds. The concurrent section uses one connection per thread
//! (the client is synchronous; share-nothing threads are the scaling model).

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ruvio_client::{__bench, Client, SetMode, SortedSetCombine};

struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);

        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);

        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size as u64, Ordering::Relaxed);

        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn measure(name: &str, rounds: u32, mut operation: impl FnMut()) {
    for _ in 0..200 {
        operation();
    }

    let (allocations, bytes) = (
        ALLOCATIONS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    );
    let started = Instant::now();

    for _ in 0..rounds {
        operation();
    }

    let elapsed = started.elapsed();
    let rounds = u64::from(rounds);

    println!(
        "{name:<28} {:>6.1} alloc/op {:>8.0} B/op {:>9.3} us/op",
        (ALLOCATIONS.load(Ordering::Relaxed) - allocations) as f64 / rounds as f64,
        (BYTES.load(Ordering::Relaxed) - bytes) as f64 / rounds as f64,
        elapsed.as_secs_f64() * 1e6 / rounds as f64,
    );
}

fn throughput(name: &str, threads: usize, host: &str, port: u16, set: bool) {
    let window = Duration::from_millis(1500);
    let workers: Vec<_> = (0..threads)
        .map(|thread| {
            let host = host.to_owned();

            std::thread::spawn(move || {
                let mut client = Client::connect(host, port).expect("connect");
                let key = format!("bench:t{thread}");
                let started = Instant::now();
                let mut done = 0_u64;

                client.set(&key, "value").unwrap();

                while started.elapsed() < window {
                    for _ in 0..100 {
                        if set {
                            client.set(&key, "value").unwrap();
                        } else {
                            client.get(&key).unwrap();
                        }
                    }

                    done += 100;
                }

                done as f64 / started.elapsed().as_secs_f64()
            })
        })
        .collect();
    let total: f64 = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .sum();

    println!("{name:<28} {threads:>2} thread(s) {total:>12.0} ops/s");
}

fn main() {
    let address = std::env::var("RUVIO_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_owned());
    let (host, port) = address.split_once(':').expect("host:port");
    let port: u16 = port.parse().expect("port");

    println!("-- codec (no I/O)");

    let mut buffer = Vec::with_capacity(256);

    measure("encode SET k v", 200_000, || {
        buffer.clear();
        __bench::encode(&mut buffer, &["SET", "bench:key", "value"]).unwrap();
    });

    let reply_ok = b"+OK\r\n".to_vec();
    let reply_int = b":12345\r\n".to_vec();
    let reply_bulk = b"$32\r\nvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\r\n".to_vec();
    let reply_array = b"*3\r\n$1\r\na\r\n:2\r\n$-1\r\n".to_vec();

    measure("parse +OK", 200_000, || {
        __bench::parse(&mut Cursor::new(&reply_ok[..])).unwrap();
    });
    measure("parse :int", 200_000, || {
        __bench::parse(&mut Cursor::new(&reply_int[..])).unwrap();
    });
    measure("parse $32 bulk", 200_000, || {
        __bench::parse(&mut Cursor::new(&reply_bulk[..])).unwrap();
    });
    measure("parse *3 array", 200_000, || {
        __bench::parse(&mut Cursor::new(&reply_array[..])).unwrap();
    });

    println!("-- serial round trips");

    let mut client = Client::connect(host, port).expect("connect");
    let small = "v".repeat(32);
    let large = "v".repeat(4096);

    client.set("bench:small", &small).unwrap();
    client.set("bench:large", &large).unwrap();
    client.set("bench:n", "1").unwrap();
    client.set("bench:lock", "token").unwrap();
    client.zadd("bench:z", &[("a", 1.0), ("b", 2.0)]).unwrap();

    measure("GET small", 20_000, || {
        client.get("bench:small").unwrap();
    });
    measure("GET 4 KB", 20_000, || {
        client.get("bench:large").unwrap();
    });
    measure("GET missing", 20_000, || {
        client.get("bench:missing").unwrap();
    });
    measure("SET small", 20_000, || {
        client.set("bench:small", &small).unwrap();
    });
    measure("SET 4 KB", 20_000, || {
        client.set("bench:large", &large).unwrap();
    });
    measure("SET PX", 20_000, || {
        client
            .set_with(
                "bench:ttl",
                &small,
                Some(Duration::from_secs(60)),
                SetMode::Always,
            )
            .unwrap();
    });
    measure("INCR", 20_000, || {
        client.incr("bench:n").unwrap();
    });
    measure("EXPIRE", 20_000, || {
        client.expire("bench:n", Duration::from_secs(60)).unwrap();
    });
    measure("SET IFEQ PX", 20_000, || {
        client
            .set_if_eq(
                "bench:lock",
                "token",
                "token",
                Some(Duration::from_secs(30)),
            )
            .unwrap();
    });
    measure("DELEX miss", 20_000, || {
        client.delex("bench:lock", "other").unwrap();
    });
    measure("LPUSHCAP", 20_000, || {
        client.lpushcap("bench:recent", 100, &["item"]).unwrap();
    });
    measure("ZCOUNT", 20_000, || {
        client.zcount("bench:z", "-inf", "+inf").unwrap();
    });
    measure("ZUNION 2 keys", 20_000, || {
        client
            .zunion(&["bench:z", "bench:z"], SortedSetCombine::default())
            .unwrap();
    });
    measure("BF.MADD 2", 20_000, || {
        client.bf_madd("bench:bf", &["a", "b"]).unwrap();
    });
    measure("LIMIT", 20_000, || {
        client
            .rate_limit("bench:rl", u64::MAX / 2, Duration::from_secs(60))
            .unwrap();
    });
    measure("PING", 20_000, || {
        client.ping().unwrap();
    });
    measure("execute_many 10 x GET", 5_000, || {
        let get: &[&str] = &["GET", "bench:small"];

        client.execute_many(&[get; 10]).unwrap();
    });

    println!("-- throughput (1.5 s each)");

    for threads in [1, 4] {
        throughput("GET", threads, host, port, false);
        throughput("SET", threads, host, port, true);
    }
}
