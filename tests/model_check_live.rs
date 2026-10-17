//! Seeded smoke runs of `scripts/model_check.py`, so the checker itself keeps working: a short
//! run against the real binary must pass, and a server that silently loses every write across a
//! restart must be reported.

#![cfg(target_os = "linux")]

#[path = "../src/test_ports.rs"]
mod test_ports;

use std::{
    os::unix::process::CommandExt,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// The checker in a process group of its own, with the servers it starts. Ending it kills the
/// whole group, so neither a timeout nor a failed assertion leaves a server running; the
/// servers also die with the checker on their own (`PR_SET_PDEATHSIG`).
///
/// The checker is reaped only after the group is killed: until then its pid, which names the
/// group, cannot be given to another process.
struct CheckerProcess(Option<Child>);

impl CheckerProcess {
    /// Whether the checker has exited, without reaping it.
    fn exited(&self) -> bool {
        let child = self.0.as_ref().expect("running");
        // SAFETY: an all-zero siginfo_t is valid; waitid only writes into it.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: waitid(2) on our own child with WNOWAIT leaves it to be reaped later.
        let status = unsafe {
            libc::waitid(
                libc::P_PID,
                child.id() as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };

        assert_eq!(status, 0, "waitid: {}", std::io::Error::last_os_error());
        // SAFETY: waitid filled `info`; si_pid is 0 while the child still runs.
        unsafe { info.si_pid() != 0 }
    }

    /// Kills what is left of the group, then reaps the checker.
    fn finish(&mut self) -> std::process::ExitStatus {
        let mut child = self.0.take().expect("running");

        // SAFETY: plain killpg(2) on the group this test created; the unreaped checker keeps
        // its id. ESRCH (nothing left in it) is fine.
        unsafe { libc::killpg(child.id() as libc::pid_t, libc::SIGKILL) };
        child.wait().unwrap()
    }
}

impl Drop for CheckerProcess {
    fn drop(&mut self) {
        if self.0.is_some() {
            self.finish();
        }
    }
}

/// Runs the checker with `args` on a reserved port block; returns (exit success, combined
/// output). Panics on a hang.
fn run_checker(args: &[&str], timeout: Duration) -> Option<(bool, String)> {
    if Command::new("python3")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_err()
    {
        eprintln!("python3 is not installed; skipping the model checker smoke run");
        return None;
    }

    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("output.txt");
    let output = std::fs::File::create(&output_path).unwrap();
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/model_check.py");
    // Held until the checker and its servers are gone: the checker uses these ports as given.
    let ports = test_ports::reserve();
    // Not while a parallel test probes ports: see `test_ports::spawn_lock`.
    let spawning = test_ports::spawn_lock();
    let mut checker = CheckerProcess(Some(
        Command::new("python3")
            .arg(script)
            .args(["--bin", env!("CARGO_BIN_EXE_ruvio")])
            .arg("--results")
            .arg(dir.path().join("results"))
            .arg("--work-dir")
            .arg(dir.path().join("work"))
            .args(["--port-base", &ports.port().to_string()])
            .args(args)
            .stdout(output.try_clone().unwrap())
            .stderr(output)
            .process_group(0)
            .spawn()
            .unwrap(),
    ));
    drop(spawning);
    let deadline = Instant::now() + timeout;

    while !checker.exited() {
        if Instant::now() > deadline {
            drop(checker);
            panic!(
                "the model checker did not finish within {timeout:?}:\n{}",
                std::fs::read_to_string(&output_path).unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Whatever the checker left in its group is gone before the ports are released.
    let status = checker.finish();
    drop(ports);

    Some((
        status.success(),
        std::fs::read_to_string(&output_path).unwrap_or_default(),
    ))
}

#[test]
fn a_short_seeded_model_check_run_passes() {
    let Some((passed, output)) = run_checker(
        &[
            "--seed",
            "11",
            "--config",
            "always-1,everysec-2",
            "--duration",
            "8",
        ],
        Duration::from_secs(90),
    ) else {
        return;
    };

    assert!(passed, "the model checker reported a failure:\n{output}");
    assert!(
        output.contains("seed 11"),
        "the seed is not printed:\n{output}"
    );
}

/// Small WAL segments rotate every few hundred writes, so injected sync failures land on a
/// rotation's flush too: refused `always` writes must stay out of the log there as well.
#[test]
fn a_short_run_with_small_wal_segments_and_sync_faults_passes() {
    let Some((passed, output)) = run_checker(
        &[
            "--seed",
            "13",
            "--config",
            "always-2",
            "--duration",
            "8",
            "--wal-segment-bytes",
            "65536",
            "--events",
            "disk,kill9",
        ],
        Duration::from_secs(90),
    ) else {
        return;
    };

    assert!(passed, "the model checker reported a failure:\n{output}");
}

#[test]
fn the_model_checker_reports_a_server_that_loses_writes() {
    let Some((passed, output)) = run_checker(
        &[
            "--seed",
            "12",
            "--config",
            "always-1",
            "--duration",
            "4",
            "--events",
            "kill9",
            "--mutant",
            "lose-writes",
        ],
        Duration::from_secs(90),
    ) else {
        return;
    };

    assert!(
        !passed,
        "a server that loses writes was not reported:\n{output}"
    );
    assert!(
        output.contains("FAIL always-1 seed 12: state"),
        "the failure is not a state mismatch:\n{output}"
    );
}
