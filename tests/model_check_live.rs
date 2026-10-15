//! Seeded smoke runs of `scripts/model_check.py`, so the checker itself keeps working: a short
//! run against the real binary must pass, and a server that silently loses every write across a
//! restart must be reported.

#![cfg(target_os = "linux")]

use std::{
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

/// Runs the checker with `args`; returns (exit success, combined output). Panics on a hang.
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
    let mut child = Command::new("python3")
        .arg(script)
        .args(["--bin", env!("CARGO_BIN_EXE_ruvio")])
        .arg("--results")
        .arg(dir.path().join("results"))
        .arg("--work-dir")
        .arg(dir.path().join("work"))
        .args(args)
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + timeout;

    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "the model checker did not finish within {timeout:?}:\n{}",
                std::fs::read_to_string(&output_path).unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };

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
            "--port-base",
            "27400",
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
            "--port-base",
            "27500",
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
            "--port-base",
            "27600",
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
