use anyhow::Result;
use clap::Parser;
use midge_destroyer::cli::{Cli, Command};
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
            let cfg = run.to_config();
            let result = run_scenario(cfg, artifacts_root)?;
            print_run_report(&result.report);
            if !result.report.passed {
                anyhow::bail!("scenario failed; artifacts={}", result.report.artifacts_dir);
            }
        }
        Command::Suite(suite) => {
            let report_json = suite.report_json;
            let report: SuiteReport = run_suite(suite, artifacts_root)?;
            if report_json {
                println!("{}", report.to_json_pretty()?);
            } else {
                println!(
                    "preset={} backend={} scenarios={} pass={} wobble={} bend={} break={} infrastructure_error={} skipped={}",
                    report.preset,
                    report.backend,
                    report.scenario_count,
                    report.pass_count,
                    report.wobble_count,
                    report.bend_count,
                    report.break_count,
                    report.infrastructure_error_count,
                    report.skipped_count,
                );
            }
        }
        Command::Report(report_args) => {
            let aggregate = collect_reports(&report_args.artifacts_root)?;
            if report_args.report_json {
                println!("{}", serde_json::to_string_pretty(&aggregate)?);
            } else {
                for suite in aggregate.suites {
                    println!(
                        "backend={} scenarios={} pass={} wobble={} bend={} break={} infrastructure_error={} skipped={}",
                        suite.backend,
                        suite.scenario_count,
                        suite.pass_count,
                        suite.wobble_count,
                        suite.bend_count,
                        suite.break_count,
                        suite.infrastructure_error_count,
                        suite.skipped_count,
                    );
                }
            }
        }
        Command::Frontier(frontier) => {
            let report = run_frontier(frontier, artifacts_root.clone())?;
            println!(
                "scenario={} cloud={} runs={} wobble={} bend={} break={}",
                report.scenario,
                report.cloud,
                report.runs.len(),
                report.first_wobble.is_some(),
                report.first_bend.is_some(),
                report.first_break.is_some()
            );
            std::fs::create_dir_all(&artifacts_root)?;
            std::fs::write(
                artifacts_root.join("frontier-report.json"),
                serde_json::to_vec_pretty(&report)?,
            )?;
        }
    }

    Ok(())
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
        "verdict={:?} ok={} expected={} acked={} failed={} unknown={} duplicate={} missing={}",
        report.verdict,
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
