use crate::cli::{FrontierArgs, ParsedRunConfig, SuiteArgs};
use crate::config::{ScenarioConfig, SuiteConfig};
use crate::ledger::{Ledger, OutcomeClassifier};
use crate::report::{FrontierReport, ScenarioReport, SuiteReport};
use crate::scenario::{DeterministicPlan, FaultClass, MutationOp};
use crate::types::BackendKind;
use crate::worker_protocol::{OperationReport, WorkerCommand};
use anyhow::{Context, Result};
use rand::prelude::{IndexedRandom, SliceRandom};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunArtifact {
    pub artifacts_dir: PathBuf,
    pub seed: u64,
    pub scenario: String,
    pub cloud: BackendKind,
    pub scale: String,
    pub plan_path: PathBuf,
    pub ledger_path: PathBuf,
    pub report_path: PathBuf,
    pub command_reports_dir: PathBuf,
    pub db_path: PathBuf,
}

#[derive(Debug)]
pub struct RunResult {
    pub report: ScenarioReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportAggregate {
    pub suites: Vec<SuiteReport>,
}

const REPORT_SCHEMA_VERSION: &str = "midge-destroyer.report/v1";
const LEDGER_SCHEMA_VERSION: &str = "midge-destroyer.ledger/v1";
const SUITE_SCHEMA_VERSION: &str = "midge-destroyer.suite-report/v1";
const WORKER_BINARY: &str = "midge-destroyer-worker";

#[derive(Debug, Clone)]
struct RunMetadata {
    scenario: String,
    seed: u64,
    cloud: BackendKind,
    scale: String,
}

pub fn run_scenario(cfg: ParsedRunConfig, artifacts_root: PathBuf) -> Result<RunResult> {
    run_scenario_at(artifacts_root, cfg)
}

pub fn run_scenario_at(root: PathBuf, cfg: ParsedRunConfig) -> Result<RunResult> {
    std::fs::create_dir_all(&root).context("create artifact root")?;

    let started_at = SystemTime::now();
    let started_clock = std::time::Instant::now();
    let run_id = format!(
        "{}-{}-{}",
        cfg.scenario,
        cfg.config.seed,
        started_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );

    let artifacts_dir = root.join(run_id);
    std::fs::create_dir_all(&artifacts_dir).context("create scenario artifact dir")?;

    let mut notes = Vec::new();
    let db_path = if cfg.config.cloud.is_cloud() {
        artifacts_dir.join("db_sqrzl")
    } else {
        artifacts_dir.join("db_local")
    };

    if cfg.config.cloud_only_manual && std::env::var("MIDGE_DESTROYER_CLOUD_SMOKE").is_err() {
        notes.push("cloud mode was requested, but cloud smoke is disabled".to_string());
        notes.push("set MIDGE_DESTROYER_CLOUD_SMOKE=1 to run cloud scenarios".to_string());
        let report = ScenarioReport {
            schema_version: REPORT_SCHEMA_VERSION.to_string(),
            scenario: cfg.scenario.clone(),
            seed: cfg.config.seed,
            cloud: format!("{:?}", cfg.config.cloud),
            scale: format!("{:?}", cfg.config.scale),
            artifacts_dir: artifacts_dir.to_str().unwrap_or_default().to_string(),
            classifier: OutcomeClassifier::default(),
            passed: false,
            duration_ms: started_clock.elapsed().as_millis(),
            notes: notes.clone(),
        };
        write_report(&artifacts_dir.join("scenario-report.json"), &report)?;
        return Ok(RunResult { report });
    }

    let plan = DeterministicPlan::from_seed(&cfg.scenario, cfg.config.seed, cfg.config.scale);
    let command_path = artifacts_dir.join("commands.json");
    let segment_dir = artifacts_dir.join("segments");
    std::fs::create_dir_all(&segment_dir).context("create segment dir")?;

    std::fs::write(
        &command_path,
        serde_json::to_vec_pretty(
            &plan
                .scenario
                .operations
                .iter()
                .map(worker_command_from_op)
                .collect::<Vec<_>>(),
        )?,
    )
    .context("write command artifact")?;

    let metadata = RunMetadata {
        scenario: cfg.scenario.clone(),
        seed: cfg.config.seed,
        cloud: cfg.config.cloud,
        scale: format!("{:?}", cfg.config.scale),
    };

    let mut ledger = Ledger::new();
    ledger.entries = plan.to_expected_ledger();
    ledger.schema_version = LEDGER_SCHEMA_VERSION.to_string();

    let mut start = 0usize;
    let mut segment_index = 0usize;
    let mut observed: Vec<OperationReport> = Vec::new();
    let mut faults = plan.scenario.faults.clone();
    faults.sort_by_key(|fault| fault.step);

    while start < plan.scenario.operations.len() {
        let next_fault = faults.iter().find(|fault| fault.step >= start).cloned();
        let fault_at = next_fault
            .as_ref()
            .map(|fault| fault.step.saturating_sub(start));
        let hard_crash = next_fault
            .as_ref()
            .is_some_and(|fault| matches!(fault.class, FaultClass::ProcessKill));

        let segment_commands = plan
            .scenario
            .operations
            .iter()
            .skip(start)
            .map(worker_command_from_op)
            .collect::<Vec<_>>();

        let segment_command_path =
            segment_dir.join(format!("segment-{segment_index}-commands.json"));
        let segment_report_path = segment_dir.join(format!("segment-{segment_index}-report.jsonl"));

        std::fs::write(
            &segment_command_path,
            serde_json::to_vec_pretty(&segment_commands)?,
        )
        .context("write segment command file")?;

        let retry_deadline = std::time::Instant::now() + Duration::from_secs(65);
        let (status, mut segment_reports) = loop {
            let _ = std::fs::remove_file(&segment_report_path);
            let status = run_worker(
                &metadata,
                hard_crash.then_some(fault_at.unwrap_or_default()),
                next_fault.as_ref().filter(|_| !hard_crash).and(fault_at),
                &db_path,
                &segment_command_path,
                &segment_report_path,
                &command_path,
            )
            .context("run worker segment")?;
            let reports =
                read_report_lines(&segment_report_path).context("read worker segment report")?;
            let lease_held = reports.iter().any(is_lease_held_report);
            if lease_held && std::time::Instant::now() < retry_deadline {
                notes.push("reopen fenced by live lease; retrying until safe takeover".to_string());
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
            break (status, reports);
        };

        // Process/open errors are lifecycle evidence, not mutations. Keeping
        // them out of the ledger prevents synthetic operation 0 from changing
        // the expected mutation count.
        let lifecycle_reports = segment_reports
            .iter()
            .filter(|report| report.operation_id == 0)
            .count();
        if lifecycle_reports > 0 {
            notes.push(format!(
                "worker emitted {lifecycle_reports} lifecycle error report(s)"
            ));
        }
        segment_reports.retain(|report| report.operation_id != 0);
        observed.append(&mut segment_reports);

        if matches!(status, WorkerStatus::Crashed | WorkerStatus::Interrupted) {
            if let Some(fault) = &next_fault {
                notes.push(format!(
                    "simulated {:?} at step {}",
                    fault.class, fault.step
                ));
                if let Err(error) = apply_fault(&db_path, fault) {
                    notes.push(format!("fault {:?} was not applied: {error}", fault.class));
                    break;
                }
                start = fault.step;
                segment_index += 1;
                faults.retain(|entry| entry.step != fault.step);
                continue;
            }
            notes.push("worker crashed without an expected fault trigger".to_string());
            break;
        }

        if status == WorkerStatus::Failed {
            notes.push("worker process failed".to_string());
            break;
        }

        if status == WorkerStatus::Ok {
            if start >= plan.scenario.operations.len() {
                break;
            }
            start = plan.scenario.operations.len();
        }
    }

    let observed_outcomes = observed
        .iter()
        .map(|entry| entry.outcome.clone())
        .collect::<Vec<_>>();
    ledger.classify(&observed_outcomes);
    let ledger_path = artifacts_dir.join("ledger-final.json");
    std::fs::write(&ledger_path, ledger.serialize_json()?)?;

    let classifier = OutcomeClassifier::from_ledger(&ledger);
    let report = ScenarioReport {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        scenario: metadata.scenario,
        seed: metadata.seed,
        cloud: format!("{:?}", metadata.cloud),
        scale: metadata.scale,
        artifacts_dir: artifacts_dir.to_str().unwrap_or(".").to_string(),
        classifier: classifier.clone(),
        passed: classifier.is_strictly_safe() && classifier.acked >= 1,
        duration_ms: started_clock.elapsed().as_millis(),
        notes,
    };
    let report_path = artifacts_dir.join("scenario-report.json");
    write_report(&report_path, &report)?;

    Ok(RunResult { report })
}

#[derive(Debug, PartialEq)]
enum WorkerStatus {
    Ok,
    Failed,
    Crashed,
    Interrupted,
}

fn run_worker(
    metadata: &RunMetadata,
    crash_at: Option<usize>,
    interrupt_at: Option<usize>,
    db_path: &PathBuf,
    command_file: &PathBuf,
    report_file: &PathBuf,
    verify_commands: &PathBuf,
) -> Result<WorkerStatus> {
    let exe = std::env::current_exe().context("locate current executable")?;
    let worker = exe
        .parent()
        .map(|parent| parent.join(WORKER_BINARY))
        .or_else(|| Some(PathBuf::from(WORKER_BINARY)))
        .context("locate worker executable")?;

    let mut cmd = Command::new(worker);
    cmd.arg("--commands")
        .arg(command_file)
        .arg("--db-root")
        .arg(db_path)
        .arg("--report")
        .arg(report_file)
        .arg("--verify-commands")
        .arg(verify_commands);

    cmd.arg("--cloud-provider").arg(metadata.cloud.as_arg());
    if let Some(step) = crash_at {
        cmd.arg("--crash-on-step").arg(step.to_string());
    }
    if let Some(step) = interrupt_at {
        cmd.arg("--interrupt-on-step").arg(step.to_string());
    }

    let status = cmd.status().context("spawn worker")?;
    if status.success() {
        Ok(WorkerStatus::Ok)
    } else if status.code() == Some(1) {
        Ok(WorkerStatus::Crashed)
    } else if status.code() == Some(3) {
        Ok(WorkerStatus::Interrupted)
    } else {
        Ok(WorkerStatus::Failed)
    }
}

fn read_report_lines(path: &PathBuf) -> Result<Vec<OperationReport>> {
    use std::io::BufRead as _;
    let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut out = Vec::new();
    for line in std::io::BufReader::new(file).lines() {
        let raw = line?;
        if raw.trim().is_empty() {
            continue;
        }
        out.push(serde_json::from_str::<OperationReport>(&raw)?);
    }
    Ok(out)
}

fn apply_fault(path: &Path, fault: &crate::scenario::ScenarioFault) -> Result<()> {
    match fault.class {
        FaultClass::StaleCacheCleanup => {
            let _ = std::fs::remove_dir_all(path.join("sst"));
            let _ = std::fs::remove_dir_all(path.join("wal"));
        }
        FaultClass::DroppedWrite => {
            let wal =
                find_wal_artifact(path).context("no WAL artifact exists at the fault boundary")?;
            std::fs::remove_file(&wal)
                .with_context(|| format!("remove WAL artifact {}", wal.display()))?;
        }
        FaultClass::ManifestInterruption => {
            let _ = std::fs::remove_file(path.join("MANIFEST"));
            let _ = std::fs::remove_file(path.join("MANIFEST.prev"));
        }
        FaultClass::WalTruncationRace => {
            let _ = std::fs::remove_file(path.join("wal").join("wal.log"));
            let _ = std::fs::remove_file(path.join("wal.log"));
        }
        FaultClass::SstCorruption => {
            if let Some(file) = pick_random_file(path.join("sst")) {
                let _ = std::fs::write(file, b"corrupted");
            }
        }
        FaultClass::ProviderLatencySpike => {
            std::thread::sleep(Duration::from_millis(250));
        }
        FaultClass::ProcessKill
        | FaultClass::ForcedReopen
        | FaultClass::CompactionRace
        | FaultClass::LeaseStalenessWindow
        | FaultClass::RegionPartition
        | FaultClass::StrictAsyncDurabilityFlip
        | FaultClass::ExactWalPathFault
        | FaultClass::ManifestCheckpointCut
        | FaultClass::FlushCompactionBarrierFault
        | FaultClass::LeaseRenewalCut
        | FaultClass::MigrationBoundaryFault => {
            let _ = std::fs::write(path.join("fault-marker"), b"marker");
        }
    }
    Ok(())
}

fn is_lease_held_report(report: &OperationReport) -> bool {
    matches!(&report.outcome, crate::worker_protocol::ObservedOutcome::Failed { error, .. }
        if error.contains("Writer lease held"))
}

fn find_wal_artifact(root: &Path) -> Option<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir).ok()? {
            let path = entry.ok()?.path();
            if path.is_dir() {
                pending.push(path);
            } else if path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().to_ascii_lowercase().contains("wal"))
            {
                return Some(path);
            }
        }
    }
    None
}

fn pick_random_file(dir: PathBuf) -> Option<PathBuf> {
    let mut rng = SmallRng::seed_from_u64(17);
    let entries = std::fs::read_dir(dir).ok()?;
    entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect::<Vec<_>>()
        .choose(&mut rng)
        .cloned()
}

fn worker_command_from_op(op: &MutationOp) -> WorkerCommand {
    WorkerCommand {
        operation_id: op.id,
        sequence: op.sequence,
        action: op.action.clone(),
        key: op.key.clone(),
        value: op.value.clone(),
        durable: op.durable,
    }
}

fn write_report(path: &PathBuf, report: &ScenarioReport) -> Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(report)?)?;
    Ok(())
}

pub fn run_suite(args: SuiteArgs, artifacts_root: PathBuf) -> Result<SuiteReport> {
    let suite_config: SuiteConfig = args.build_config();
    let mut scenario_reports = Vec::new();
    let mut scenarios = build_suite_plan(&suite_config);

    if let Some(limit) = args.max_scenarios {
        scenarios.truncate(limit);
    } else {
        scenarios.truncate(suite_config.max_scenarios);
    }

    for scenario in scenarios {
        let cfg = ParsedRunConfig {
            scenario: scenario.clone(),
            config: ScenarioConfig {
                scenario: scenario.clone(),
                seed: ScenarioConfig::derived_seed(None),
                cloud: if suite_config.cloud {
                    BackendKind::Sqrzl
                } else {
                    BackendKind::Local
                },
                scale: suite_config.scale,
                max_runtime_ms: suite_config.scale.max_runtime_ms(),
                fault_window_ms: 250,
                cloud_only_manual: false,
                continue_on_failure: true,
            },
        };

        match run_scenario(cfg.clone(), artifacts_root.clone()) {
            Ok(result) => scenario_reports.push(result.report),
            Err(err) => {
                scenario_reports.push(ScenarioReport {
                    schema_version: REPORT_SCHEMA_VERSION.to_string(),
                    scenario,
                    seed: cfg.config.seed,
                    cloud: format!("{:?}", cfg.config.cloud),
                    scale: format!("{:?}", cfg.config.scale),
                    artifacts_dir: String::from(".") + "artifacts",
                    classifier: OutcomeClassifier {
                        expected: 0,
                        acked: 0,
                        failed: 0,
                        unknown: 0,
                        duplicate: 0,
                        missing: 0,
                    },
                    passed: false,
                    duration_ms: 0,
                    notes: vec![format!("suite run failed: {err}")],
                });
            }
        }

        if !cfg.config.continue_on_failure && !scenario_reports.last().is_some_and(|r| r.passed) {
            break;
        }
    }

    Ok(SuiteReport {
        schema_version: SUITE_SCHEMA_VERSION.to_string(),
        preset: format!("{:?}", suite_config.preset),
        scenario_count: scenario_reports.len(),
        pass_count: scenario_reports
            .iter()
            .filter(|report| report.passed)
            .count(),
        fail_count: scenario_reports
            .iter()
            .filter(|report| !report.passed)
            .count(),
        results: scenario_reports,
    })
}

pub fn run_frontier(args: FrontierArgs, artifacts_root: PathBuf) -> Result<FrontierReport> {
    let cloud: BackendKind = args.cloud.clone().into();
    let names: Vec<&str> = if args.scenario == "all" {
        vec![
            "recovery-crash-loop",
            "dupe-dispatch",
            "flush-barrier",
            "manifest-race",
            "sst-corruption",
        ]
    } else {
        vec![args.scenario.as_str()]
    };
    let mut runs = Vec::new();
    let mut first_wobble = None;
    let mut first_break = None;
    for scale in [
        crate::config::RunScale::Small,
        crate::config::RunScale::Medium,
        crate::config::RunScale::Large,
        crate::config::RunScale::XLarge,
    ] {
        for name in &names {
            for offset in 0..args.seeds {
                let seed = args.seed_start.saturating_add(offset as u64);
                let cfg = ParsedRunConfig {
                    scenario: (*name).to_string(),
                    config: ScenarioConfig {
                        scenario: (*name).to_string(),
                        seed,
                        cloud,
                        scale,
                        max_runtime_ms: scale.max_runtime_ms(),
                        fault_window_ms: 250,
                        cloud_only_manual: cloud.requires_manual_opt_in(),
                        continue_on_failure: true,
                    },
                };
                let report = run_scenario(cfg, artifacts_root.clone())?.report;
                // Missing entries can be an expected temporary outage after a
                // crash/reopen fault; retain them as wobble but do not call
                // them a safety break by themselves.
                let wobble = report.classifier.unknown > 0
                    || report.classifier.duplicate > 0
                    || report.classifier.missing > 0
                    || report.classifier.failed > 0;
                if wobble && first_wobble.is_none() {
                    first_wobble = Some(report.clone());
                }
                let safety_break = report.classifier.failed > 0
                    || report.classifier.unknown > 0
                    || report.classifier.duplicate > 0;
                if safety_break && first_break.is_none() {
                    first_break = Some(report.clone());
                }
                runs.push(report);
            }
        }
        if first_break.is_some() {
            break;
        }
    }
    Ok(FrontierReport {
        schema_version: "midge-destroyer.frontier/v1".to_string(),
        scenario: args.scenario,
        cloud: format!("{cloud:?}"),
        seeds_per_scale: args.seeds,
        first_wobble,
        first_break,
        runs,
    })
}

pub fn collect_reports(root: &str) -> Result<ReportAggregate> {
    let mut suites: BTreeMap<String, Vec<ScenarioReport>> = BTreeMap::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let report_file = path.join("scenario-report.json");
        if !report_file.exists() {
            continue;
        }

        let raw = std::fs::read_to_string(&report_file)?;
        let parsed: ScenarioReport = serde_json::from_str(&raw)?;
        let key = format!("{:?}", parsed.cloud);
        suites.entry(key).or_default().push(parsed);
    }

    let suite_reports = suites
        .into_iter()
        .map(|(cloud, results)| SuiteReport {
            schema_version: SUITE_SCHEMA_VERSION.to_string(),
            preset: cloud,
            scenario_count: results.len(),
            pass_count: results.iter().filter(|r| r.passed).count(),
            fail_count: results.iter().filter(|r| !r.passed).count(),
            results,
        })
        .collect();

    Ok(ReportAggregate {
        suites: suite_reports,
    })
}

fn build_suite_plan(config: &SuiteConfig) -> Vec<String> {
    let mut scenarios = vec![
        "recovery-crash-loop",
        "dupe-dispatch",
        "flush-barrier",
        "manifest-race",
        "sst-corruption",
    ];
    if config.cloud {
        scenarios.push("sqrzl-visibility");
    }

    let mut rng = SmallRng::seed_from_u64(42);
    scenarios.shuffle(&mut rng);
    scenarios
        .into_iter()
        .map(std::string::ToString::to_string)
        .collect()
}
