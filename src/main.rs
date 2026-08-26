use anyhow::Result;
use clap::Parser;
use midge_destroyer::cli::{Cli, Command};
use midge_destroyer::types::BackendKind;
use midge_destroyer::{
    collect_reports,
    report::{ScenarioReport, SuiteReport},
    run_frontier, run_scenario, run_suite,
};
use std::path::PathBuf;

fn main() -> Result<()> {
    let args = Cli::parse();
    let artifacts_root = PathBuf::from(args.artifacts_root.clone());

    match args.command {
        Command::Run(run) => {
            let backend: BackendKind = run.cloud.clone().into();
            let cfg = run.to_config();
            with_emulator(backend, || {
                let result = run_scenario(cfg, artifacts_root)?;
                print_run_report(&result.report);
                if !result.report.passed {
                    anyhow::bail!("scenario failed; artifacts={}", result.report.artifacts_dir);
                }
                Ok(())
            })?;
        }
        Command::Suite(suite) => {
            let backend: BackendKind = suite.cloud.clone().into();
            let report_json = suite.report_json;
            with_emulator(backend, || {
                let report: SuiteReport = run_suite(suite, artifacts_root)?;
                if report_json {
                    println!("{}", report.to_json_pretty()?);
                } else {
                    println!(
                        "preset={} scenarios={} pass={} fail={}",
                        report.preset, report.scenario_count, report.pass_count, report.fail_count
                    );
                }
                Ok(())
            })?;
        }
        Command::Report(report_args) => {
            let aggregate = collect_reports(&report_args.artifacts_root)?;
            if report_args.report_json {
                println!("{}", serde_json::to_string_pretty(&aggregate)?);
            } else {
                for suite in aggregate.suites {
                    println!(
                        "cloud={} scenarios={} pass={} fail={}",
                        suite.preset, suite.scenario_count, suite.pass_count, suite.fail_count
                    );
                }
            }
        }
        Command::Frontier(frontier) => {
            let backend: BackendKind = frontier.cloud.clone().into();
            with_emulator(backend, || {
                let report = run_frontier(frontier, artifacts_root.clone())?;
                println!(
                    "scenario={} cloud={} runs={} wobble={} break={}",
                    report.scenario,
                    report.cloud,
                    report.runs.len(),
                    report.first_wobble.is_some(),
                    report.first_break.is_some()
                );
                if let Some(r) = &report.first_wobble {
                    println!(
                        "first-wobble scale={} seed={} artifacts={}",
                        r.scale, r.seed, r.artifacts_dir
                    );
                }
                if let Some(r) = &report.first_break {
                    println!(
                        "first-break scale={} seed={} artifacts={}",
                        r.scale, r.seed, r.artifacts_dir
                    );
                }
                std::fs::write(
                    artifacts_root.join("frontier-report.json"),
                    serde_json::to_vec_pretty(&report)?,
                )?;
                Ok(())
            })?;
        }
    }

    Ok(())
}

fn with_emulator<T>(backend: BackendKind, run: impl FnOnce() -> Result<T>) -> Result<T> {
    let Some((compose_file, project)) = emulator_compose(backend) else {
        return run();
    };
    let up = std::process::Command::new("docker")
        .args(["compose", "-p", project, "-f"])
        .arg(&compose_file)
        .args(["up", "-d", "--wait", "sqrzl"])
        .status()?;
    if !up.success() {
        let _ = std::process::Command::new("docker")
            .args(["compose", "-p", project, "-f"])
            .arg(&compose_file)
            .arg("down")
            .status();
        anyhow::bail!(
            "failed to start {project} emulator with {}",
            compose_file.display()
        );
    }
    if let Err(error) = wait_for_sqrzl() {
        let _ = std::process::Command::new("docker")
            .args(["compose", "-p", project, "-f"])
            .arg(&compose_file)
            .arg("down")
            .status();
        return Err(error);
    }
    let result = run();
    let down = std::process::Command::new("docker")
        .args(["compose", "-p", project, "-f"])
        .arg(&compose_file)
        .arg("down")
        .status();
    match (result, down) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
        (Ok(_), Ok(status)) if !status.success() => {
            anyhow::bail!("failed to stop {project} emulator")
        }
        (Ok(value), Ok(_)) => Ok(value),
    }
}

fn wait_for_sqrzl() -> Result<()> {
    let address = "127.0.0.1:9000".parse()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect_timeout(&address, std::time::Duration::from_millis(250))
            .is_ok()
        {
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    anyhow::bail!("Sqrzl did not become reachable at 127.0.0.1:9000 within 30 seconds")
}

fn emulator_compose(backend: BackendKind) -> Option<(PathBuf, &'static str)> {
    let file = match backend {
        BackendKind::S3 => ("compose.s3.yml", "midge-destroyer-s3"),
        BackendKind::Azure => ("compose.azure.yml", "midge-destroyer-azure"),
        BackendKind::Gcs => ("compose.gcs.yml", "midge-destroyer-gcs"),
        BackendKind::Local | BackendKind::Sqrzl => return None,
    };
    Some((
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(file.0),
        file.1,
    ))
}

fn print_run_report(report: &ScenarioReport) {
    println!(
        "scenario={} seed={} schema={}",
        report.scenario, report.seed, report.schema_version
    );
    if report.notes.is_empty() {
        println!("notes=[none]");
    } else {
        for note in &report.notes {
            println!("note={note}");
        }
    }
    println!(
        "ok={} expected={} acked={} failed={} unknown={} duplicate={} missing={}",
        report.passed,
        report.classifier.expected,
        report.classifier.acked,
        report.classifier.failed,
        report.classifier.unknown,
        report.classifier.duplicate,
        report.classifier.missing,
    );
    println!(
        "timed_out={} recovery_verified={} verification_incomplete={} duration_ms={}",
        report.timed_out,
        report.recovery_verified,
        report.verification_incomplete,
        report.duration_ms
    );
}
