use std::fs;
use std::path::Path;
use std::process::Command;

use midge_destroyer::config::RunScale;
use midge_destroyer::scenario::{DeterministicPlan, FaultClass};

#[test]
fn should_run_local_smoke_once() {
    let artifacts_root = tempfile::tempdir().expect("create temp artifact dir");
    let exe = std::env::current_exe().expect("locate integration test executable");
    let binary_dir = exe
        .parent()
        .and_then(|parent| parent.parent())
        .expect("locate target debug directory");
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let binary = binary_dir.join(format!("midge-destroyer{suffix}"));

    let scenario_name = "smoke-local";
    let output = Command::new(&binary)
        .arg("--artifacts-root")
        .arg(artifacts_root.path())
        .arg("run")
        .arg(scenario_name)
        .arg("--cloud")
        .arg("local")
        .arg("--scale")
        .arg("small")
        .arg("--seed")
        .arg("12")
        .output()
        .expect("run destroyer");

    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let has_report = fs::read_dir(artifacts_root.path())
        .expect("read artifacts root")
        .filter_map(Result::ok)
        .any(|entry| {
            let path = entry.path();
            path.is_dir() && path.join("scenario-report.json").is_file()
        });

    assert!(has_report, "expected scenario report under artifacts root");

    let latest =
        find_any_report(artifacts_root.path()).expect("discover a generated scenario report");
    let raw = fs::read_to_string(&latest).expect("read scenario report");
    assert!(raw.contains("\"scenario\""));
    assert!(raw.contains("midge-destroyer.report"));
}

#[test]
fn should_continue_after_expected_worker_crash_without_report_file() {
    let artifacts_root = tempfile::tempdir().expect("create temp artifact dir");
    let exe = std::env::current_exe().expect("locate integration test executable");
    let binary_dir = exe
        .parent()
        .and_then(|parent| parent.parent())
        .expect("locate target debug directory");
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let binary = binary_dir.join(format!("midge-destroyer{suffix}"));
    let crash_seed = (0_u64..64)
        .find(|seed| {
            DeterministicPlan::from_seed("recovery-crash-loop", *seed, RunScale::Small)
                .scenario
                .faults
                .iter()
                .any(|fault| matches!(fault.class, FaultClass::ProcessKill))
        })
        .expect("find deterministic hard-crash seed");

    let output = Command::new(&binary)
        .arg("--artifacts-root")
        .arg(artifacts_root.path())
        .arg("run")
        .arg("recovery-crash-loop")
        .arg("--cloud")
        .arg("local")
        .arg("--scale")
        .arg("small")
        .arg("--seed")
        .arg(crash_seed.to_string())
        .output()
        .expect("run crash-loop destroyer");

    assert!(
        output.status.success(),
        "crash-loop command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = find_any_report(artifacts_root.path()).expect("discover crash-loop report");
    let raw = fs::read_to_string(report).expect("read crash-loop report");
    assert!(raw.contains("recovery-crash-loop"));
}

fn find_any_report(root: &Path) -> Option<std::path::PathBuf> {
    fs::read_dir(root)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("scenario-report.json"))
        .find(|path| path.is_file())
}
