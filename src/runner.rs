use crate::cli::{FrontierArgs, ParsedRunConfig, SuiteArgs};
use crate::config::{ScenarioConfig, SuiteConfig};
use crate::ledger::{Ledger, OutcomeClassifier};
use crate::report::{FrontierReport, LifecycleSummary, ScenarioReport, SuiteReport};
use crate::scenario::{DeterministicPlan, FaultClass, MutationOp};
use crate::types::BackendKind;
use crate::worker_protocol::{OperationReport, ReportPhase, WorkerCommand};
use anyhow::{Context, Result};
use rand::prelude::{IndexedRandom, SliceRandom};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, Stdio};
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
    cloud_prefix: String,
}

pub fn run_scenario(cfg: ParsedRunConfig, artifacts_root: PathBuf) -> Result<RunResult> {
    run_scenario_at(artifacts_root, cfg)
}

pub fn run_scenario_at(root: PathBuf, cfg: ParsedRunConfig) -> Result<RunResult> {
    const FAILPOINT_SCENARIOS: &[&str] = &[
        "wal-sync-ack-cut",
        "manifest-sync-failure",
        "compaction-commit-cut",
        "wal-prune-cut",
        "lease-renewal-failure",
        "flush-barrier",
    ];
    if FAILPOINT_SCENARIOS.contains(&cfg.scenario.as_str()) && !cfg!(feature = "failpoint-tier") {
        anyhow::bail!(
            "{} requires a build with --features failpoint-tier",
            cfg.scenario
        );
    }
    if cfg.scenario == "cloud-cache-loss" && !cfg.config.cloud.is_cloud() {
        anyhow::bail!("cloud-cache-loss requires a cloud backend");
    }
    if cfg.scenario == "dupe-dispatch" {
        anyhow::bail!("dupe-dispatch is retired; use ack-kill-window");
    }
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

    let artifacts_dir = root.join(&run_id);
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
            lifecycle: None,
            timed_out: false,
            recovery_verified: false,
            verification_incomplete: false,
        };
        write_report(&artifacts_dir.join("scenario-report.json"), &report)?;
        return Ok(RunResult { report });
    }

    let plan = DeterministicPlan::from_seed(&cfg.scenario, cfg.config.seed, cfg.config.scale);
    let crash_recovery_grace_ms = plan
        .scenario
        .faults
        .iter()
        .filter(|fault| {
            matches!(
                fault.class,
                FaultClass::ProcessKill | FaultClass::AckBeforeReportCrash
            )
        })
        .count() as u64
        * 70_000;
    let effective_max_runtime_ms = cfg
        .config
        .max_runtime_ms
        .saturating_add(crash_recovery_grace_ms);
    let scenario_deadline = started_clock + Duration::from_millis(effective_max_runtime_ms);
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
        cloud_prefix: format!("destroyer/{run_id}"),
    };

    let mut ledger = Ledger::new();
    ledger.entries = plan.to_expected_ledger();
    ledger.schema_version = LEDGER_SCHEMA_VERSION.to_string();

    let mut start = 0usize;
    let mut segment_index = 0usize;
    let mut observed: Vec<OperationReport> = Vec::new();
    let mut timed_out = false;
    let mut execution_incomplete = false;
    let mut recovery_verification_needed = false;
    let mut recovery_verified = false;
    let mut verification_incomplete = false;
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
        let crash_after_ack = next_fault
            .as_ref()
            .is_some_and(|fault| matches!(fault.class, FaultClass::AckBeforeReportCrash));

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
        let lifecycle_report_path =
            segment_dir.join(format!("segment-{segment_index}-lifecycle.json"));

        std::fs::write(
            &segment_command_path,
            serde_json::to_vec_pretty(&segment_commands)?,
        )
        .context("write segment command file")?;

        let retry_deadline = std::cmp::min(
            std::time::Instant::now() + Duration::from_secs(65),
            scenario_deadline,
        );
        let (status, mut segment_reports) = loop {
            let _ = std::fs::remove_file(&segment_report_path);
            let status = run_worker(
                &metadata,
                hard_crash.then_some(fault_at.unwrap_or_default()),
                crash_after_ack.then_some(fault_at.unwrap_or_default()),
                next_fault
                    .as_ref()
                    .filter(|_| !hard_crash && !crash_after_ack)
                    .and(fault_at),
                &db_path,
                &segment_command_path,
                &segment_report_path,
                &command_path,
                &lifecycle_report_path,
                scenario_deadline,
            )
            .context("run worker segment")?;
            let reports =
                read_report_lines(&segment_report_path).context("read worker segment report")?;
            let lease_held = reports.iter().any(is_lease_held_report);
            if lease_held && std::time::Instant::now() < retry_deadline {
                notes.push("reopen fenced by live lease; retrying until safe takeover".to_string());
                let remaining =
                    scenario_deadline.saturating_duration_since(std::time::Instant::now());
                std::thread::sleep(std::cmp::min(Duration::from_secs(1), remaining));
                continue;
            }
            break (status, reports);
        };

        // Process/open errors are lifecycle evidence, not mutations. Keeping
        // them out of the ledger prevents synthetic operation 0 from changing
        // the expected mutation count.
        let lifecycle_reports = segment_reports
            .iter()
            .filter(|report| report.phase == ReportPhase::Lifecycle)
            .count();
        if lifecycle_reports > 0 {
            notes.push(format!(
                "worker emitted {lifecycle_reports} lifecycle error report(s)"
            ));
        }
        segment_reports.retain(|report| report.phase != ReportPhase::Lifecycle);
        observed.append(&mut segment_reports);

        if matches!(status, WorkerStatus::Crashed | WorkerStatus::Interrupted) {
            recovery_verification_needed = true;
            if let Some(fault) = &next_fault {
                notes.push(format!(
                    "simulated {:?} at step {}",
                    fault.class, fault.step
                ));
                if let Err(error) = apply_fault(&db_path, fault) {
                    notes.push(format!("fault {:?} was not applied: {error}", fault.class));
                    execution_incomplete = true;
                    recovery_verification_needed = true;
                    break;
                }
                start = if matches!(fault.class, FaultClass::AckBeforeReportCrash) {
                    fault.step.saturating_add(1)
                } else {
                    fault.step
                };
                segment_index += 1;
                faults.retain(|entry| entry.step != fault.step);
                continue;
            }
            notes.push("worker crashed without an expected fault trigger".to_string());
            execution_incomplete = true;
            recovery_verification_needed = true;
            break;
        }

        if status == WorkerStatus::Failed {
            notes.push("worker process failed".to_string());
            execution_incomplete = true;
            recovery_verification_needed = true;
            break;
        }

        if status == WorkerStatus::TimedOut {
            timed_out = true;
            recovery_verification_needed = true;
            notes.push(format!(
                "scenario exceeded max runtime of {} ms",
                effective_max_runtime_ms
            ));
            break;
        }

        if status == WorkerStatus::Ok {
            if start >= plan.scenario.operations.len() {
                break;
            }
            start = plan.scenario.operations.len();
        }
    }

    if recovery_verification_needed {
        let verifier_commands = segment_dir.join("recovery-verifier-commands.json");
        let verifier_report = segment_dir.join("recovery-verifier-report.jsonl");
        let verifier_lifecycle = segment_dir.join("recovery-verifier-lifecycle.json");
        std::fs::write(&verifier_commands, b"[]")
            .context("write recovery verifier command file")?;
        let recovery_deadline =
            std::time::Instant::now() + Duration::from_millis(cfg.config.recovery_timeout_ms);
        let (verifier_status, mut verifier_reports) = loop {
            let _ = std::fs::remove_file(&verifier_report);
            let status = run_worker(
                &metadata,
                None,
                None,
                None,
                &db_path,
                &verifier_commands,
                &verifier_report,
                &command_path,
                &verifier_lifecycle,
                recovery_deadline,
            )
            .context("run post-fault recovery verifier")?;
            let reports = read_report_lines(&verifier_report)
                .context("read post-fault recovery verifier report")?;
            if reports.iter().any(is_lease_held_report)
                && std::time::Instant::now() < recovery_deadline
            {
                notes.push(
                    "recovery verifier fenced by live lease; retrying until safe takeover"
                        .to_string(),
                );
                let remaining =
                    recovery_deadline.saturating_duration_since(std::time::Instant::now());
                std::thread::sleep(std::cmp::min(Duration::from_secs(1), remaining));
                continue;
            }
            break (status, reports);
        };
        verifier_reports.retain(|report| report.phase != ReportPhase::Lifecycle);
        observed.append(&mut verifier_reports);
        match verifier_status {
            WorkerStatus::Ok => {
                recovery_verified = true;
                notes.push("post-fault recovery verification completed".to_string());
            }
            WorkerStatus::TimedOut => {
                verification_incomplete = true;
                notes.push(format!(
                    "post-fault recovery verification exceeded {} ms",
                    cfg.config.recovery_timeout_ms
                ));
            }
            WorkerStatus::Failed | WorkerStatus::Crashed | WorkerStatus::Interrupted => {
                verification_incomplete = true;
                notes.push("post-fault recovery verifier failed before completing".to_string());
            }
        }
    }

    if timed_out || execution_incomplete {
        ledger.classify_reports_after_timeout(&observed);
    } else {
        ledger.classify_reports(&observed);
    }
    let ledger_path = artifacts_dir.join("ledger-final.json");
    std::fs::write(&ledger_path, ledger.serialize_json()?)?;

    let classifier = OutcomeClassifier::from_ledger(&ledger);
    let lifecycle = read_lifecycle_reports(&segment_dir);
    let report = ScenarioReport {
        schema_version: "midge-destroyer.report/v2".to_string(),
        scenario: metadata.scenario,
        seed: metadata.seed,
        cloud: format!("{:?}", metadata.cloud),
        scale: metadata.scale,
        artifacts_dir: artifacts_dir.to_str().unwrap_or(".").to_string(),
        classifier: classifier.clone(),
        passed: classifier.is_strictly_safe() && classifier.acked >= 1,
        duration_ms: started_clock.elapsed().as_millis(),
        notes,
        lifecycle: (!lifecycle.is_empty()).then(|| LifecycleSummary::from_reports(&lifecycle)),
        timed_out,
        recovery_verified,
        verification_incomplete,
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
    TimedOut,
}

fn run_worker(
    metadata: &RunMetadata,
    crash_at: Option<usize>,
    crash_after_at: Option<usize>,
    interrupt_at: Option<usize>,
    db_path: &PathBuf,
    command_file: &PathBuf,
    report_file: &PathBuf,
    verify_commands: &PathBuf,
    lifecycle_report: &PathBuf,
    deadline: std::time::Instant,
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
        .arg(verify_commands)
        .arg("--lifecycle-report")
        .arg(lifecycle_report);

    cmd.arg("--cloud-provider").arg(metadata.cloud.as_arg());
    cmd.arg("--cloud-prefix").arg(&metadata.cloud_prefix);
    if let Some(step) = crash_at {
        cmd.arg("--crash-on-step").arg(step.to_string());
    }
    if let Some(step) = crash_after_at {
        cmd.arg("--crash-after-step").arg(step.to_string());
    }
    if let Some(step) = interrupt_at {
        cmd.arg("--interrupt-on-step").arg(step.to_string());
    }

    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawn worker")?;
    let status = loop {
        if let Some(status) = child.try_wait().context("poll worker")? {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(WorkerStatus::TimedOut);
        }
        std::thread::sleep(Duration::from_millis(25));
    };
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
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = std::fs::read_to_string(path).with_context(|| format!("open {}", path.display()))?;
    let has_truncated_tail = !raw.ends_with('\n');
    let line_count = raw.lines().count();
    let mut out = Vec::new();
    for (index, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<OperationReport>(line) {
            Ok(report) => out.push(report),
            Err(_) if has_truncated_tail && index + 1 == line_count => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(out)
}

fn read_lifecycle_reports(dir: &Path) -> Vec<crate::worker_protocol::LifecycleReport> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut paths = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().contains("lifecycle"))
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
        .into_iter()
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .filter_map(|raw| serde_json::from_str(&raw).ok())
        .collect()
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
        FaultClass::ExactWalPathFault => {
            write_failpoint_sentinel(path, "midge::wal::txn_after_sync_before_ack", "panic")?;
        }
        FaultClass::ManifestCheckpointCut => {
            write_failpoint_sentinel(
                path,
                "midge::manifest::inject_required_sync_failure",
                "return",
            )?;
        }
        FaultClass::FlushCompactionBarrierFault => {
            write_failpoint_sentinel(
                path,
                "midge::compaction::inject_failure_after_manifest_batch",
                "return",
            )?;
        }
        FaultClass::CompactionRace => {
            write_failpoint_sentinel(
                path,
                "midge::cloud::after_wal_prune_dependency_validation",
                "panic",
            )?;
        }
        FaultClass::LeaseRenewalCut => {
            write_failpoint_sentinel(
                path,
                "midge::lease::inject_renewal_thread_spawn_failure",
                "return",
            )?;
        }
        FaultClass::ProcessKill
        | FaultClass::AckBeforeReportCrash
        | FaultClass::ForcedReopen
        | FaultClass::LeaseStalenessWindow
        | FaultClass::RegionPartition
        | FaultClass::StrictAsyncDurabilityFlip
        | FaultClass::MigrationBoundaryFault => {
            let _ = std::fs::write(path.join("fault-marker"), b"marker");
        }
        FaultClass::CloudCacheLoss => {
            if path.exists() {
                std::fs::remove_dir_all(path)
                    .with_context(|| format!("remove cloud cache {}", path.display()))?;
            }
            std::fs::create_dir_all(path)
                .with_context(|| format!("recreate cloud cache {}", path.display()))?;
        }
    }
    Ok(())
}

fn write_failpoint_sentinel(path: &Path, name: &str, payload: &str) -> Result<()> {
    let dir = path.join("failpoints");
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("create failpoint directory {}", dir.display()))?;
    crate::failpoint::write_sentinel(&dir, name, payload)
        .with_context(|| format!("write failpoint sentinel '{name}'"))?;
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
                recovery_timeout_ms: args.recovery_timeout_secs.saturating_mul(1_000),
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
                    lifecycle: None,
                    timed_out: false,
                    recovery_verified: false,
                    verification_incomplete: true,
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
    let max_scale = crate::config::RunScale::from(args.max_scale.clone());
    let names: Vec<&str> = if args.scenario == "all" {
        let mut names = vec![
            "recovery-crash-loop",
            "ack-kill-window",
            "manifest-race",
            "sst-corruption",
        ];
        if cfg!(feature = "failpoint-tier") {
            names.push("flush-barrier");
        }
        if cloud.is_cloud() {
            names.push("cloud-cache-loss");
        }
        names
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
        if scale.ops() > max_scale.ops() {
            break;
        }
        for name in &names {
            for offset in 0..args.seeds {
                let seed = args.seed_start.saturating_add(offset as u64);
                eprintln!(
                    "frontier-start scenario={} cloud={cloud:?} scale={scale:?} seed={seed}",
                    name
                );
                let cfg = ParsedRunConfig {
                    scenario: (*name).to_string(),
                    config: ScenarioConfig {
                        scenario: (*name).to_string(),
                        seed,
                        cloud,
                        scale,
                        max_runtime_ms: scale.max_runtime_ms(),
                        recovery_timeout_ms: args.recovery_timeout_secs.saturating_mul(1_000),
                        fault_window_ms: 250,
                        cloud_only_manual: cloud.requires_manual_opt_in(),
                        continue_on_failure: true,
                    },
                };
                let report = run_scenario(cfg, artifacts_root.clone())?.report;
                eprintln!(
                    "frontier-finish scenario={} scale={scale:?} seed={seed} passed={} timed_out={} duration_ms={} artifacts={}",
                    name,
                    report.passed,
                    report.timed_out,
                    report.duration_ms,
                    report.artifacts_dir
                );
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
                    || report.classifier.duplicate > 0
                    || (report.classifier.unknown > 0 && !report.recovery_verified);
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
        "ack-kill-window",
        "manifest-race",
        "sst-corruption",
    ];
    if cfg!(feature = "failpoint-tier") {
        scenarios.push("flush-barrier");
    }
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

#[cfg(test)]
mod tests {
    use super::read_report_lines;
    use crate::worker_protocol::{ObservedOutcome, OperationReport, ReportPhase};

    fn report() -> OperationReport {
        OperationReport {
            operation_id: 1,
            sequence: 0,
            key: "key-1".to_string(),
            phase: ReportPhase::Mutation,
            outcome: ObservedOutcome::Acked {
                operation_id: 1,
                sequence: 0,
                key: "key-1".to_string(),
            },
        }
    }

    #[test]
    fn should_ignore_unterminated_report_tail_after_worker_kill() {
        // Arrange
        let dir = tempfile::tempdir().expect("create report directory");
        let path = dir.path().join("worker.jsonl");
        let complete = serde_json::to_string(&report()).expect("serialize complete report");
        std::fs::write(&path, format!("{complete}\n{{\"operation_id\":"))
            .expect("write truncated report");

        // Act
        let reports = read_report_lines(&path).expect("read complete report prefix");

        // Assert
        assert_eq!(reports.len(), 1);
    }

    #[test]
    fn should_reject_malformed_completed_report_line() {
        // Arrange
        let dir = tempfile::tempdir().expect("create report directory");
        let path = dir.path().join("worker.jsonl");
        std::fs::write(&path, "{not-json}\n").expect("write malformed report");

        // Act
        let result = read_report_lines(&path);

        // Assert
        assert!(result.is_err());
    }
}
