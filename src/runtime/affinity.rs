//! Shard thread CPU pinning from the `cpu_set` setting (Linux `cpulist` syntax, for
//! example `2`, `0-3` or `0,2,4-5`). Shard `n` takes the `n`th listed core, wrapping
//! when there are more shards than cores.

/// Parses a Linux `cpulist`. Cores come back in the order written, duplicates kept out.
pub(crate) fn parse_cpu_list(text: &str) -> Result<Vec<usize>, String> {
    let mut cores = Vec::new();

    for part in text.split(',') {
        let part = part.trim();

        if part.is_empty() {
            return Err(format!("empty entry in CPU set `{text}`"));
        }

        let (first, last) = match part.split_once('-') {
            Some((first, last)) => (parse_core(first)?, parse_core(last)?),
            None => {
                let core = parse_core(part)?;

                (core, core)
            }
        };

        if first > last {
            return Err(format!("descending CPU range `{part}`"));
        }

        for core in first..=last {
            if !cores.contains(&core) {
                cores.push(core);
            }
        }
    }

    Ok(cores)
}

fn parse_core(text: &str) -> Result<usize, String> {
    let core: usize = text
        .trim()
        .parse()
        .map_err(|_| format!("`{text}` is not a CPU number"))?;

    if core >= MAX_CPUS {
        return Err(format!("CPU {core} is beyond the {MAX_CPUS} supported"));
    }

    Ok(core)
}

pub(crate) fn core_for_shard(cores: &[usize], shard: usize) -> Option<usize> {
    (!cores.is_empty()).then(|| cores[shard % cores.len()])
}

const MAX_CPUS: usize = 1024;

/// Pins the calling thread to the core `cpu_set` assigns to `shard`. Does nothing when
/// the setting is absent. Failures only warn: a missing pin costs speed, not safety.
pub(crate) fn pin_shard_thread(cpu_set: Option<&str>, shard: usize) {
    let Some(text) = cpu_set else {
        return;
    };

    let cores = match parse_cpu_list(text) {
        Ok(cores) => cores,
        Err(error) => {
            tracing::warn!(shard, %error, "ignoring cpu_set");

            return;
        }
    };

    let Some(core) = core_for_shard(&cores, shard) else {
        return;
    };

    match pin_current_thread(core) {
        Ok(()) => tracing::info!(shard, core, "pinned shard thread"),
        Err(error) => tracing::warn!(shard, core, %error, "could not pin shard thread"),
    }
}

// Pinning keeps a shard on one core (cache locality, no migration). std has no affinity API,
// so this is a direct FFI call; `libc` is not a dependency.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn pin_current_thread(core: usize) -> std::io::Result<()> {
    unsafe extern "C" {
        fn sched_setaffinity(pid: i32, cpusetsize: usize, mask: *const u64) -> i32;
    }

    // `cpu_set_t` is 1024 bits. Pid 0 means the calling thread.
    let mut mask = [0u64; MAX_CPUS / 64];

    mask[core / 64] |= 1 << (core % 64);

    // SAFETY: `mask` is a valid, initialised buffer of exactly `cpusetsize` bytes and
    // the call only reads it.
    let result = unsafe { sched_setaffinity(0, size_of_val(&mask), mask.as_ptr()) };

    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread(_core: usize) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "CPU pinning is only implemented on Linux",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cpulist_syntax() {
        assert_eq!(parse_cpu_list("2").unwrap(), vec![2]);
        assert_eq!(parse_cpu_list("0-3").unwrap(), vec![0, 1, 2, 3]);
        assert_eq!(parse_cpu_list("0, 2,4-5,2").unwrap(), vec![0, 2, 4, 5]);
        assert!(parse_cpu_list("").is_err());
        assert!(parse_cpu_list("3-1").is_err());
        assert!(parse_cpu_list("a").is_err());
        assert!(parse_cpu_list("2048").is_err());
    }

    #[test]
    fn shards_take_listed_cores_in_order_and_wrap() {
        let cores = [4, 6];

        assert_eq!(core_for_shard(&cores, 0), Some(4));
        assert_eq!(core_for_shard(&cores, 1), Some(6));
        assert_eq!(core_for_shard(&cores, 2), Some(4));
        assert_eq!(core_for_shard(&[], 0), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[allow(unsafe_code)]
    fn pinning_restricts_the_calling_thread() {
        unsafe extern "C" {
            fn sched_getaffinity(pid: i32, cpusetsize: usize, mask: *mut u64) -> i32;
        }

        std::thread::spawn(|| {
            let mut allowed = [0u64; MAX_CPUS / 64];

            // SAFETY: valid writable buffer of the stated size.
            #[allow(unsafe_code)]
            let result =
                unsafe { sched_getaffinity(0, size_of_val(&allowed), allowed.as_mut_ptr()) };

            assert_eq!(result, 0);

            let Some(core) = (0..MAX_CPUS).find(|core| allowed[core / 64] >> (core % 64) & 1 == 1)
            else {
                return;
            };

            pin_current_thread(core).unwrap();

            let mut pinned = [0u64; MAX_CPUS / 64];

            #[allow(unsafe_code)]
            // SAFETY: `pinned` is a writable buffer of exactly the size passed.
            let result = unsafe { sched_getaffinity(0, size_of_val(&pinned), pinned.as_mut_ptr()) };

            assert_eq!(result, 0);
            assert_eq!(pinned.iter().map(|word| word.count_ones()).sum::<u32>(), 1);
            assert_eq!(pinned[core / 64] >> (core % 64) & 1, 1);
        })
        .join()
        .unwrap();
    }
}
