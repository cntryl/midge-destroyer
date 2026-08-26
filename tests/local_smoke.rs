use std::fs;
use std::path::Path;
use std::process::Command;

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

fn find_any_report(root: &Path) -> Option<std::path::PathBuf> {
    fs::read_dir(root)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("scenario-report.json"))
        .find(|path| path.is_file())
}
