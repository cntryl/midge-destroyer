use crate::cli::{FrontierArgs, ParsedRunConfig, SuiteArgs};
use crate::config::{ScenarioConfig, SuiteConfig, SuitePreset};
use crate::emulator::EmulatorController;
use crate::ledger::{Ledger, OutcomeClassifier};
use crate::report::{
    classify_verdict, infer_invariant_violation, mark_infrastructure_error, FrontierReport,
    LifecycleSummary, RecoveryEvent, RecoveryOutcome, ScenarioReport, SuiteReport, Verdict,
};
use crate::scenario::{
    scenario_definition, suite_scenarios, DeterministicPlan, FaultClass, FaultExpectation,
    MutationOp, ScenarioAvailability, ScenarioDefinition,
};
use crate::types::BackendKind;
use crate::worker_protocol::{OperationReport, WorkerCommand, WorkerLifecycleChannel};
use anyhow::{Context, Result};
use rand::prelude::IndexedRandom;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

const REPORT_SCHEMA_VERSION: &str = "midge-destroyer.report/v3";
const LEDGER_SCHEMA_VERSION: &str = "midge-destroyer.ledger/v1";
const WORKER_BINARY: &str = "midge-destroyer-worker";

#[derive(Debug, Clone)]
struct RunMetadata {
    scenario: String,
    seed: u64,
    cloud: BackendKind,
    scale: String,
    cloud_prefix: String,
    lease_profile: crate::config::LeaseProfile,
    provider_endpoint: Option<String>,
}

struct PendingRecovery {
    fault_class: FaultClass,
    step: usize,
    started: Instant,
    attempts: usize,
}

/// Run one scenario with an execution-scoped emulator when required.
///
/// # Errors
///
/// Returns an error when artifacts, Compose, or the scenario runner fail.
#[allow(clippy::needless_pass_by_value)]
pub fn run_scenario(mut cfg: ParsedRunConfig, artifacts_root: PathBuf) -> Result<RunResult> {
    let (execution_id, execution_dir) =
        create_execution_dir(&artifacts_root, "run", cfg.config.cloud, &cfg.scenario)?;
    let mut emulator =
        EmulatorController::for_backend(cfg.config.cloud, &execution_dir, &execution_id)?;
    if let Some(controller) = emulator.as_mut() {
        match controller.ensure_ready("before-scenario") {
            Ok(endpoint) => cfg.config.provider_endpoint = Some(endpoint),
            Err(error) => {
                let definition = scenario_definition(&cfg.scenario)
                    .context("scenario is absent from the catalog")?;
                let report = infrastructure_report(
                    &execution_dir.join("scenarios"),
                    &cfg.config,
                    definition,
                    format!("Sqrzl failed before scenario start: {error}"),
                )?;
                write_report(execution_dir.join("scenario-report.json"), &report)?;
                return Ok(RunResult { report });
            }
        }
    }

    let mut result = run_scenario_at(execution_dir.join("scenarios"), cfg)?;
    if let Some(controller) = emulator.as_mut() {
        if let Err(error) = controller.probe("after-scenario") {
            controller.mark_unhealthy("scenario-infrastructure-failure");
            mark_infrastructure_error(
                &mut result.report,
                format!("Sqrzl became unhealthy during the scenario: {error}"),
            );
            write_report(
                Path::new(&result.report.artifacts_dir).join("scenario-report.json"),
                &result.report,
            )?;
        }
    }
    write_report(execution_dir.join("scenario-report.json"), &result.report)?;
    Ok(result)
}

/// Run one scenario beneath an existing execution directory.
///
/// # Errors
///
/// Returns an error when the scenario is invalid, artifacts cannot be written,
/// or a worker process cannot be executed.
#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub fn run_scenario_at(root: PathBuf, cfg: ParsedRunConfig) -> Result<RunResult> {
    let definition = scenario_definition(&cfg.scenario)
        .with_context(|| format!("unknown scenario: {}", cfg.scenario))?;
    validate_scenario(definition, &cfg)?;
    std::fs::create_dir_all(&root).context("create scenario artifact root")?;

    let started_at = SystemTime::now();
    let started_clock = Instant::now();
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
    std::fs::create_dir_all(&artifacts_dir).context("create scenario artifact directory")?;

    if cfg.config.cloud_only_manual && std::env::var("MIDGE_DESTROYER_CLOUD_SMOKE").is_err() {
        return Ok(RunResult {
            report: skipped_report(
                &artifacts_dir,
                &cfg.config,
                definition,
                "set MIDGE_DESTROYER_CLOUD_SMOKE=1 to run the in-process Sqrzl tier",
            )?,
        });
    }

    let plan = DeterministicPlan::from_seed(&cfg.scenario, cfg.config.seed, cfg.config.scale);
    let budget = cfg.config.recovery_budget();
    let effective_max_runtime_ms = cfg.config.max_runtime_ms.saturating_add(
        (plan.scenario.faults.len() as u64).saturating_mul(budget.hard_deadline_ms),
    );
    let scenario_deadline = started_clock + Duration::from_millis(effective_max_runtime_ms);
    let command_path = artifacts_dir.join("commands.json");
    let plan_path = artifacts_dir.join("scenario-plan.json");
    let segment_dir = artifacts_dir.join("segments");
    std::fs::create_dir_all(&segment_dir).context("create segment directory")?;
    std::fs::write(&plan_path, serde_json::to_vec_pretty(&plan)?).context("write scenario plan")?;
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

    let db_path = if cfg.config.cloud.is_cloud() {
        artifacts_dir.join("db_cloud_cache")
    } else {
        artifacts_dir.join("db_local")
    };
    let metadata = RunMetadata {
        scenario: cfg.scenario.clone(),
        seed: cfg.config.seed,
        cloud: cfg.config.cloud,
        scale: format!("{:?}", cfg.config.scale),
        cloud_prefix: format!("destroyer/{}/{run_id}", cfg.config.cloud.as_arg()),
        lease_profile: cfg.config.lease_profile,
        provider_endpoint: cfg.config.provider_endpoint.clone(),
    };

    let mut ledger = Ledger::new();
    ledger.entries = plan.to_expected_ledger();
    ledger.schema_version = LEDGER_SCHEMA_VERSION.to_string();
    let mut notes = Vec::new();
    let mut observed = Vec::new();
    let mut recovery_events = Vec::new();
    let mut pending_recovery: Option<PendingRecovery> = None;
    let mut start = 0_usize;
    let mut segment_index = 0_usize;
    let mut timed_out = false;
    let mut execution_incomplete = false;
    let mut verification_incomplete = false;
    let mut recovery_verification_needed = false;
    let mut recovery_verified = plan.scenario.faults.is_empty();
    let mut faults = plan.scenario.faults.clone();
    faults.sort_by_key(|fault| fault.step);

    while start < plan.scenario.operations.len() {
        let next_fault = faults.iter().find(|fault| fault.step >= start).cloned();
        let fault_at = next_fault
            .as_ref()
            .map(|fault| fault.step.saturating_sub(start));
        let hard_crash = next_fault
            .as_ref()
            .is_some_and(|fault| fault.class == FaultClass::ProcessKill);
        let crash_after_ack = next_fault
            .as_ref()
            .is_some_and(|fault| fault.class == FaultClass::AckBeforeReportCrash);
        let segment_commands = plan
            .scenario
            .operations
            .iter()
            .skip(start)
            .map(worker_command_from_op)
            .collect::<Vec<_>>();
        let segment_command_path =
            segment_dir.join(format!("segment-{segment_index}-commands.json"));
        std::fs::write(
            &segment_command_path,
            serde_json::to_vec_pretty(&segment_commands)?,
        )
        .context("write segment command file")?;

        let mut attempt = 0_usize;
        let (status, mut reports) = loop {
            let report_path = segment_dir.join(format!(
                "segment-{segment_index}-attempt-{attempt}-report.jsonl"
            ));
            let lifecycle_path = segment_dir.join(format!(
                "segment-{segment_index}-attempt-{attempt}-lifecycle.json"
            ));
            let attempt_started = Instant::now();
            if let Some(pending) = pending_recovery.as_mut() {
                pending.attempts = pending.attempts.saturating_add(1);
            }
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
                &report_path,
                &command_path,
                &lifecycle_path,
                scenario_deadline,
            )?;
            let reports = read_report_lines(&report_path)?;
            let channel = read_lifecycle_channel(&lifecycle_path);
            let lease_held = channel.as_ref().is_some_and(channel_reports_lease_held);
            if lease_held {
                if let Some(pending) = pending_recovery.as_ref() {
                    let hard_deadline =
                        pending.started + Duration::from_millis(budget.hard_deadline_ms);
                    if Instant::now() < hard_deadline {
                        notes.push(format!(
                            "recovery after {:?} remained fenced on attempt {}",
                            pending.fault_class, pending.attempts
                        ));
                        std::thread::sleep(Duration::from_millis(500));
                        attempt = attempt.saturating_add(1);
                        continue;
                    }
                }
            } else if matches!(
                status,
                WorkerStatus::Ok | WorkerStatus::Interrupted | WorkerStatus::Crashed
            ) {
                if let Some(pending) = pending_recovery.take() {
                    let open_ms = channel
                        .as_ref()
                        .and_then(|entry| entry.lifecycle.as_ref())
                        .map_or(0, |lifecycle| lifecycle.open_ms);
                    let recovery_latency_ms = attempt_started
                        .duration_since(pending.started)
                        .as_millis()
                        .saturating_add(open_ms);
                    recovery_events.push(RecoveryEvent::recovered(
                        pending.fault_class,
                        pending.step,
                        pending.attempts,
                        attempt_started.duration_since(pending.started).as_millis(),
                        recovery_latency_ms,
                        budget,
                    ));
                }
            }
            break (status, reports);
        };
        let next_start_after_reports = next_worker_start(start, &reports);
        observed.append(&mut reports);

        match status {
            WorkerStatus::Crashed | WorkerStatus::Interrupted => {
                recovery_verification_needed = true;
                let Some(fault) = next_fault else {
                    notes.push("worker stopped without an expected fault trigger".to_string());
                    execution_incomplete = true;
                    break;
                };
                notes.push(format!(
                    "simulated {:?} at step {}",
                    fault.class, fault.step
                ));
                pending_recovery = Some(PendingRecovery {
                    fault_class: fault.class,
                    step: fault.step,
                    started: Instant::now(),
                    attempts: 0,
                });
                if status == WorkerStatus::Crashed {
                    clear_terminated_worker_acquisition_lock(&db_path)?;
                    notes.push(
                        "cleared the terminated worker's acquisition lock after crash proof"
                            .to_string(),
                    );
                }
                if let Err(error) = apply_fault(&db_path, &fault) {
                    notes.push(format!("fault {:?} was not applied: {error}", fault.class));
                    recovery_events.push(RecoveryEvent {
                        fault_class: fault.class,
                        step: fault.step,
                        attempts: 0,
                        contention_duration_ms: 0,
                        recovery_latency_ms: None,
                        outcome: RecoveryOutcome::RecoveryFailed,
                    });
                    pending_recovery = None;
                    execution_incomplete = true;
                    break;
                }
                start = if fault.class == FaultClass::AckBeforeReportCrash {
                    fault.step.saturating_add(1)
                } else {
                    fault.step
                };
                segment_index = segment_index.saturating_add(1);
                faults.retain(|entry| entry.step != fault.step);
            }
            WorkerStatus::Ok => {
                start = plan.scenario.operations.len();
                recovery_verified = true;
            }
            WorkerStatus::Failed | WorkerStatus::Incomplete if pending_recovery.is_some() => {
                clear_terminated_worker_acquisition_lock(&db_path)?;
                notes.push(
                    "cleared the terminated fault worker's acquisition lock before recovery"
                        .to_string(),
                );
                recovery_verification_needed = true;
                if pending_recovery.as_ref().is_some_and(|pending| {
                    pending.started.elapsed() >= Duration::from_millis(budget.hard_deadline_ms)
                }) {
                    verification_incomplete = true;
                    break;
                }
                start = next_start_after_reports;
                if start >= plan.scenario.operations.len() {
                    break;
                }
                segment_index = segment_index.saturating_add(1);
            }
            WorkerStatus::Failed | WorkerStatus::Incomplete => {
                notes.push("worker process failed".to_string());
                execution_incomplete = true;
                break;
            }
            WorkerStatus::TimedOut => {
                timed_out = true;
                execution_incomplete = true;
                notes.push(format!(
                    "scenario exceeded its {effective_max_runtime_ms} ms observation deadline"
                ));
                break;
            }
        }
    }

    if recovery_verification_needed {
        let verifier_commands = segment_dir.join("recovery-verifier-commands.json");
        std::fs::write(&verifier_commands, b"[]")?;
        let verifier_started = Instant::now();
        let verifier_deadline = pending_recovery.as_ref().map_or(
            verifier_started + Duration::from_millis(budget.hard_deadline_ms),
            |pending| pending.started + Duration::from_millis(budget.hard_deadline_ms),
        );
        let mut attempt = 0_usize;
        let verifier_status = loop {
            let report_path =
                segment_dir.join(format!("recovery-verifier-attempt-{attempt}-report.jsonl"));
            let lifecycle_path = segment_dir.join(format!(
                "recovery-verifier-attempt-{attempt}-lifecycle.json"
            ));
            let attempt_started = Instant::now();
            if let Some(pending) = pending_recovery.as_mut() {
                pending.attempts = pending.attempts.saturating_add(1);
            }
            let status = run_worker(
                &metadata,
                None,
                None,
                None,
                &db_path,
                &verifier_commands,
                &report_path,
                &command_path,
                &lifecycle_path,
                verifier_deadline,
            )?;
            let mut reports = read_report_lines(&report_path)?;
            let channel = read_lifecycle_channel(&lifecycle_path);
            let lease_held = channel.as_ref().is_some_and(channel_reports_lease_held);
            if lease_held && Instant::now() < verifier_deadline {
                attempt = attempt.saturating_add(1);
                std::thread::sleep(Duration::from_millis(500));
                continue;
            }
            if !lease_held && status == WorkerStatus::Ok {
                if let Some(pending) = pending_recovery.take() {
                    let open_ms = channel
                        .as_ref()
                        .and_then(|entry| entry.lifecycle.as_ref())
                        .map_or(0, |lifecycle| lifecycle.open_ms);
                    let recovery_latency_ms = attempt_started
                        .duration_since(pending.started)
                        .as_millis()
                        .saturating_add(open_ms);
                    recovery_events.push(RecoveryEvent::recovered(
                        pending.fault_class,
                        pending.step,
                        pending.attempts,
                        attempt_started.duration_since(pending.started).as_millis(),
                        recovery_latency_ms,
                        budget,
                    ));
                }
            }
            observed.append(&mut reports);
            break status;
        };
        match verifier_status {
            WorkerStatus::Ok => recovery_verified = true,
            WorkerStatus::TimedOut => {
                verification_incomplete = true;
                notes.push("recovery verifier reached the hard observation deadline".to_string());
            }
            WorkerStatus::Failed
            | WorkerStatus::Incomplete
            | WorkerStatus::Crashed
            | WorkerStatus::Interrupted => {
                verification_incomplete = true;
                notes.push("recovery verifier failed before completing".to_string());
            }
        }
    }

    if let Some(pending) = pending_recovery.take() {
        recovery_events.push(RecoveryEvent {
            fault_class: pending.fault_class,
            step: pending.step,
            attempts: pending.attempts,
            contention_duration_ms: pending.started.elapsed().as_millis(),
            recovery_latency_ms: None,
            outcome: if pending.started.elapsed() >= Duration::from_millis(budget.hard_deadline_ms)
            {
                RecoveryOutcome::HardDeadlineExceeded
            } else {
                RecoveryOutcome::RecoveryFailed
            },
        });
        verification_incomplete = true;
    }

    if timed_out || execution_incomplete {
        ledger.classify_reports_after_timeout(&observed);
    } else {
        ledger.classify_reports(&observed);
    }
    let ledger_path = artifacts_dir.join("ledger-final.json");
    std::fs::write(&ledger_path, ledger.serialize_json()?)?;
    let classifier = OutcomeClassifier::from_ledger(&ledger);
    let channels = read_lifecycle_channels(&segment_dir);
    let lifecycle_errors = channels
        .iter()
        .flat_map(|channel| channel.errors.iter().cloned())
        .collect::<Vec<_>>();
    let verdict = classify_verdict(&classifier, &recovery_events, verification_incomplete);
    let invariant_violated =
        infer_invariant_violation(&classifier, &recovery_events, verification_incomplete);
    let report = ScenarioReport {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        scenario: metadata.scenario,
        seed: metadata.seed,
        cloud: format!("{:?}", metadata.cloud),
        scale: metadata.scale,
        artifacts_dir: artifacts_dir.to_string_lossy().into_owned(),
        classifier,
        verdict,
        passed: matches!(verdict, Verdict::Pass | Verdict::Wobble | Verdict::Bend),
        duration_ms: started_clock.elapsed().as_millis(),
        notes,
        lifecycle: (!channels.is_empty()).then(|| LifecycleSummary::from_channels(&channels)),
        lifecycle_errors,
        timed_out,
        recovery_verified,
        verification_incomplete,
        lease_profile: cfg.config.lease_profile.as_arg().to_string(),
        recovery_budget: budget,
        recovery_events,
        invariant_violated,
        expected_behavior: format!("{:?}", definition.expected_behavior),
    };
    write_report(artifacts_dir.join("scenario-report.json"), &report)?;
    Ok(RunResult { report })
}

#[derive(Debug, PartialEq, Eq)]
enum WorkerStatus {
    Ok,
    Failed,
    Incomplete,
    Crashed,
    Interrupted,
    TimedOut,
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
    metadata: &RunMetadata,
    crash_at: Option<usize>,
    crash_after_at: Option<usize>,
    interrupt_at: Option<usize>,
    db_path: &Path,
    command_file: &Path,
    report_file: &Path,
    verify_commands: &Path,
    lifecycle_report: &Path,
    deadline: Instant,
) -> Result<WorkerStatus> {
    let executable = std::env::current_exe().context("locate current executable")?;
    let worker = executable.parent().map_or_else(
        || PathBuf::from(WORKER_BINARY),
        |parent| parent.join(WORKER_BINARY),
    );
    let mut command = build_worker_command(
        &worker,
        metadata,
        crash_at,
        crash_after_at,
        interrupt_at,
        db_path,
        command_file,
        report_file,
        verify_commands,
        lifecycle_report,
    );
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawn worker")?;
    let status = loop {
        if let Some(status) = child.try_wait().context("poll worker")? {
            break status;
        }
        if Instant::now() >= deadline {
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
    } else if status.code() == Some(4) {
        Ok(WorkerStatus::Incomplete)
    } else {
        Ok(WorkerStatus::Failed)
    }
}

fn next_worker_start(current_start: usize, reports: &[OperationReport]) -> usize {
    let mut next = current_start;
    for report in reports
        .iter()
        .filter(|report| report.phase == crate::worker_protocol::ReportPhase::Mutation)
    {
        match report.outcome {
            crate::worker_protocol::ObservedOutcome::Acked { .. } => {
                next = next.max(report.sequence.saturating_add(1));
            }
            crate::worker_protocol::ObservedOutcome::Failed { .. }
            | crate::worker_protocol::ObservedOutcome::Unknown { .. } => {
                return report.sequence;
            }
        }
    }
    next
}

fn clear_terminated_worker_acquisition_lock(db_path: &Path) -> Result<()> {
    let lock_path = db_path.join(".midge_leader.lock");
    match std::fs::remove_file(&lock_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("clear terminated worker lock {}", lock_path.display())),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_worker_command(
    worker: &Path,
    metadata: &RunMetadata,
    crash_at: Option<usize>,
    crash_after_at: Option<usize>,
    interrupt_at: Option<usize>,
    db_path: &Path,
    command_file: &Path,
    report_file: &Path,
    verify_commands: &Path,
    lifecycle_report: &Path,
) -> Command {
    let mut command = Command::new(worker);
    command
        .arg("--commands")
        .arg(command_file)
        .arg("--db-root")
        .arg(db_path)
        .arg("--report")
        .arg(report_file)
        .arg("--verify-commands")
        .arg(verify_commands)
        .arg("--lifecycle-report")
        .arg(lifecycle_report)
        .arg("--cloud-provider")
        .arg(metadata.cloud.as_arg())
        .arg("--cloud-prefix")
        .arg(&metadata.cloud_prefix)
        .arg("--lease-profile")
        .arg(metadata.lease_profile.as_arg());
    if let Some(endpoint) = &metadata.provider_endpoint {
        command.arg("--provider-endpoint").arg(endpoint);
    }
    if let Some(step) = crash_at {
        command.arg("--crash-on-step").arg(step.to_string());
    }
    if let Some(step) = crash_after_at {
        command.arg("--crash-after-step").arg(step.to_string());
    }
    if let Some(step) = interrupt_at {
        command.arg("--interrupt-on-step").arg(step.to_string());
    }
    command
}

fn read_report_lines(path: &Path) -> Result<Vec<OperationReport>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("open worker report {}", path.display()))?;
    let has_truncated_tail = !raw.ends_with('\n');
    let line_count = raw.lines().count();
    let mut reports = Vec::new();
    for (index, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<OperationReport>(line) {
            Ok(report) => reports.push(report),
            Err(_) if has_truncated_tail && index + 1 == line_count => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(reports)
}

fn read_lifecycle_channel(path: &Path) -> Option<WorkerLifecycleChannel> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
}

fn read_lifecycle_channels(directory: &Path) -> Vec<WorkerLifecycleChannel> {
    let Ok(entries) = std::fs::read_dir(directory) else {
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
        .iter()
        .filter_map(|path| read_lifecycle_channel(path))
        .collect()
}

fn channel_reports_lease_held(channel: &WorkerLifecycleChannel) -> bool {
    channel
        .errors
        .iter()
        .any(|error| error.error.contains("Writer lease held"))
}

fn apply_fault(path: &Path, fault: &crate::scenario::ScenarioFault) -> Result<()> {
    match fault.class {
        FaultClass::StaleCacheCleanup => {
            let _ = std::fs::remove_dir_all(path.join("sst"));
            let _ = std::fs::remove_dir_all(path.join("wal"));
        }
        FaultClass::DroppedWrite => {
            let wal = find_wal_artifact(path).context("no WAL artifact at fault boundary")?;
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
                std::fs::write(file, b"corrupted")?;
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
            std::fs::write(path.join("fault-marker"), b"marker")?;
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
    let directory = path.join("failpoints");
    std::fs::create_dir_all(&directory)?;
    crate::failpoint::write_sentinel(&directory, name, payload)
        .with_context(|| format!("write failpoint sentinel '{name}'"))?;
    Ok(())
}

fn find_wal_artifact(root: &Path) -> Option<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).ok()? {
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

fn pick_random_file(directory: PathBuf) -> Option<PathBuf> {
    let mut rng = SmallRng::seed_from_u64(17);
    std::fs::read_dir(directory)
        .ok()?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file())
        .collect::<Vec<_>>()
        .choose(&mut rng)
        .cloned()
}

fn worker_command_from_op(operation: &MutationOp) -> WorkerCommand {
    WorkerCommand {
        operation_id: operation.id,
        sequence: operation.sequence,
        action: operation.action.clone(),
        key: operation.key.clone(),
        value: operation.value.clone(),
        durable: operation.durable,
        workload_lane: operation.workload_lane,
        workload_batch: operation.workload_batch,
        workload_kind: operation.workload_kind,
        column_family: operation.column_family.clone(),
    }
}

/// Run a complete preset catalog and write one suite manifest.
///
/// # Errors
///
/// Returns an error when execution artifacts or the emulator cannot be managed.
#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub fn run_suite(args: SuiteArgs, artifacts_root: PathBuf) -> Result<SuiteReport> {
    let suite_config: SuiteConfig = args.build_config();
    let seed = ScenarioConfig::derived_seed(args.seed);
    let (execution_id, execution_dir) = create_execution_dir(
        &artifacts_root,
        "suite",
        suite_config.cloud,
        &format!("{:?}", suite_config.preset),
    )?;
    let mut emulator =
        EmulatorController::for_backend(suite_config.cloud, &execution_dir, &execution_id)?;
    let mut selections = suite_scenarios(
        suite_config.preset,
        suite_config.cloud,
        cfg!(feature = "failpoint-tier"),
    );
    if let Some(limit) = args.max_scenarios {
        selections.truncate(limit);
    }
    let scenarios_root = execution_dir.join("scenarios");
    std::fs::create_dir_all(&scenarios_root)?;
    let mut reports = Vec::with_capacity(selections.len());

    for selection in selections {
        let definition = selection.definition;
        let mut config = ScenarioConfig {
            scenario: definition.name.to_string(),
            seed,
            cloud: suite_config.cloud,
            scale: suite_config.scale,
            max_runtime_ms: suite_config.scale.max_runtime_ms(),
            recovery_timeout_ms: args.recovery_timeout_secs.saturating_mul(1_000),
            fault_window_ms: 250,
            cloud_only_manual: suite_config.cloud == BackendKind::Sqrzl,
            continue_on_failure: true,
            lease_profile: suite_config.lease_profile,
            provider_endpoint: None,
        };
        if let ScenarioAvailability::Skipped { reason } = selection.availability {
            reports.push(skipped_report(
                &scenarios_root.join(format!("{}-skipped", definition.name)),
                &config,
                definition,
                reason,
            )?);
            continue;
        }

        if let Some(controller) = emulator.as_mut() {
            match controller.ensure_ready(&format!("before-{}", definition.name)) {
                Ok(endpoint) => config.provider_endpoint = Some(endpoint),
                Err(error) => {
                    controller.mark_unhealthy(&format!("{}-startup-failure", definition.name));
                    reports.push(infrastructure_report(
                        &scenarios_root,
                        &config,
                        definition,
                        format!("Sqrzl was unavailable before the scenario: {error}"),
                    )?);
                    continue;
                }
            }
        }

        let parsed = ParsedRunConfig {
            scenario: definition.name.to_string(),
            config,
        };
        let report_result = run_scenario_at(scenarios_root.clone(), parsed.clone());
        let mut report = match report_result {
            Ok(result) => result.report,
            Err(error) => failure_report(
                &scenarios_root,
                &parsed.config,
                definition,
                format!("scenario runner failed: {error}"),
            )?,
        };
        if let Some(controller) = emulator.as_mut() {
            if let Err(error) = controller.probe(&format!("after-{}", definition.name)) {
                controller.mark_unhealthy(&format!("{}-infrastructure-failure", definition.name));
                mark_infrastructure_error(
                    &mut report,
                    format!("Sqrzl became unhealthy during the scenario: {error}"),
                );
                write_report(
                    Path::new(&report.artifacts_dir).join("scenario-report.json"),
                    &report,
                )?;
            }
        }
        reports.push(report);
    }

    let suite = SuiteReport::new(
        execution_id,
        format!("{:?}", suite_config.preset),
        format!("{:?}", suite_config.cloud),
        seed,
        execution_dir.to_string_lossy().into_owned(),
        reports,
    );
    std::fs::write(
        execution_dir.join("suite-manifest.json"),
        serde_json::to_vec_pretty(&suite)?,
    )?;
    Ok(suite)
}

/// Escalate scales until a reportable availability or safety frontier appears.
///
/// # Errors
///
/// Returns an error when a scenario cannot be executed.
#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub fn run_frontier(args: FrontierArgs, artifacts_root: PathBuf) -> Result<FrontierReport> {
    let backend: BackendKind = args.cloud.clone().into();
    let max_scale = crate::config::RunScale::from(args.max_scale.clone());
    let (execution_id, execution_dir) =
        create_execution_dir(&artifacts_root, "frontier", backend, &args.scenario)?;
    let scenarios_root = execution_dir.join("scenarios");
    std::fs::create_dir_all(&scenarios_root)?;
    let mut emulator = EmulatorController::for_backend(backend, &execution_dir, &execution_id)?;
    let names = if args.scenario == "all" {
        suite_scenarios(
            SuitePreset::Standard,
            backend,
            cfg!(feature = "failpoint-tier"),
        )
        .into_iter()
        .filter(|selection| selection.availability == ScenarioAvailability::Runnable)
        .map(|selection| selection.definition.name.to_string())
        .collect::<Vec<_>>()
    } else {
        vec![args.scenario.clone()]
    };
    let mut runs = Vec::new();
    let mut first_wobble = None;
    let mut first_bend = None;
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
                let definition = scenario_definition(name)
                    .with_context(|| format!("unknown frontier scenario: {name}"))?;
                let attempt_root = scenarios_root.join(format!(
                    "{}-{}-{seed}",
                    format!("{scale:?}").to_ascii_lowercase(),
                    definition.name
                ));
                let mut config = ScenarioConfig {
                    scenario: name.clone(),
                    seed,
                    cloud: backend,
                    scale,
                    max_runtime_ms: scale.max_runtime_ms(),
                    recovery_timeout_ms: args.recovery_timeout_secs.saturating_mul(1_000),
                    fault_window_ms: 250,
                    cloud_only_manual: backend == BackendKind::Sqrzl,
                    continue_on_failure: true,
                    lease_profile: args.lease_profile.clone().into(),
                    provider_endpoint: None,
                };
                if let Some(controller) = emulator.as_mut() {
                    match controller
                        .ensure_ready(&format!("before-{}-{scale:?}-{seed}", definition.name))
                    {
                        Ok(endpoint) => config.provider_endpoint = Some(endpoint),
                        Err(error) => {
                            controller.mark_unhealthy(&format!(
                                "{}-{scale:?}-{seed}-startup-failure",
                                definition.name
                            ));
                            let report = infrastructure_report(
                                &attempt_root,
                                &config,
                                definition,
                                format!("Sqrzl was unavailable before the scenario: {error}"),
                            )?;
                            runs.push(report);
                            continue;
                        }
                    }
                }
                let parsed = ParsedRunConfig {
                    scenario: name.clone(),
                    config,
                };
                let mut report = match run_scenario_at(attempt_root.clone(), parsed.clone()) {
                    Ok(result) => result.report,
                    Err(error) => failure_report(
                        &attempt_root,
                        &parsed.config,
                        definition,
                        format!("scenario runner failed: {error}"),
                    )?,
                };
                if let Some(controller) = emulator.as_mut() {
                    if let Err(error) =
                        controller.probe(&format!("after-{}-{scale:?}-{seed}", definition.name))
                    {
                        controller.mark_unhealthy(&format!(
                            "{}-{scale:?}-{seed}-infrastructure-failure",
                            definition.name
                        ));
                        mark_infrastructure_error(
                            &mut report,
                            format!("Sqrzl became unhealthy during the scenario: {error}"),
                        );
                        write_report(
                            Path::new(&report.artifacts_dir).join("scenario-report.json"),
                            &report,
                        )?;
                    }
                }
                match report.verdict {
                    Verdict::Wobble if first_wobble.is_none() => {
                        first_wobble = Some(report.clone());
                    }
                    Verdict::Bend if first_bend.is_none() => first_bend = Some(report.clone()),
                    Verdict::Break if first_break.is_none() => first_break = Some(report.clone()),
                    _ => {}
                }
                runs.push(report);
            }
        }
        if first_break.is_some() {
            break;
        }
    }
    let report = FrontierReport {
        schema_version: "midge-destroyer.frontier/v3".to_string(),
        scenario: args.scenario,
        cloud: format!("{backend:?}"),
        artifacts_dir: execution_dir.to_string_lossy().into_owned(),
        seeds_per_scale: args.seeds,
        first_wobble,
        first_bend,
        first_break,
        runs,
    };
    write_frontier_manifest(&report)?;
    Ok(report)
}

fn write_frontier_manifest(report: &FrontierReport) -> Result<()> {
    std::fs::write(
        Path::new(&report.artifacts_dir).join("frontier-manifest.json"),
        serde_json::to_vec_pretty(report)?,
    )?;
    Ok(())
}

/// Collect only v3 suite manifests beneath an artifact root.
///
/// # Errors
///
/// Returns an error when a manifest cannot be read, parsed, or validated.
pub fn collect_reports(root: &str) -> Result<ReportAggregate> {
    let mut manifests = Vec::new();
    collect_manifest_paths(Path::new(root), &mut manifests)?;
    manifests.sort();
    let suites = manifests
        .into_iter()
        .map(|path| {
            let raw = std::fs::read_to_string(&path)?;
            let suite: SuiteReport = serde_json::from_str(&raw)?;
            if suite.schema_version != "midge-destroyer.suite-manifest/v3" {
                anyhow::bail!("unsupported suite manifest at {}", path.display());
            }
            Ok(suite)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ReportAggregate { suites })
}

fn collect_manifest_paths(root: &Path, manifests: &mut Vec<PathBuf>) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_manifest_paths(&path, manifests)?;
        } else if path
            .file_name()
            .is_some_and(|name| name == "suite-manifest.json")
        {
            manifests.push(path);
        }
    }
    Ok(())
}

fn validate_scenario(definition: &ScenarioDefinition, cfg: &ParsedRunConfig) -> Result<()> {
    if definition.required_feature.is_some() && !cfg!(feature = "failpoint-tier") {
        anyhow::bail!("{} requires --features failpoint-tier", definition.name);
    }
    match definition.applicability {
        crate::scenario::BackendApplicability::LocalOnly
            if cfg.config.cloud != BackendKind::Local =>
        {
            anyhow::bail!("{} requires the local backend", definition.name);
        }
        crate::scenario::BackendApplicability::CloudOnly if !cfg.config.cloud.is_cloud() => {
            anyhow::bail!("{} requires a cloud backend", definition.name);
        }
        _ => {}
    }
    Ok(())
}

fn create_execution_dir(
    root: &Path,
    kind: &str,
    backend: BackendKind,
    label: &str,
) -> Result<(String, PathBuf)> {
    std::fs::create_dir_all(root)?;
    let execution_id = format!(
        "{}-{}-{}-{}-{}",
        kind,
        backend.as_arg(),
        label.to_ascii_lowercase(),
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
    .replace(
        |character: char| !character.is_ascii_alphanumeric() && character != '-',
        "-",
    );
    let directory = root.join(&execution_id);
    std::fs::create_dir_all(&directory)?;
    Ok((execution_id, directory))
}

fn skipped_report(
    artifacts_dir: &Path,
    config: &ScenarioConfig,
    definition: &ScenarioDefinition,
    reason: &str,
) -> Result<ScenarioReport> {
    std::fs::create_dir_all(artifacts_dir)?;
    let report = empty_report(
        artifacts_dir,
        config,
        definition,
        Verdict::Skipped,
        vec![reason.to_string()],
    );
    write_report(artifacts_dir.join("scenario-report.json"), &report)?;
    Ok(report)
}

fn infrastructure_report(
    root: &Path,
    config: &ScenarioConfig,
    definition: &ScenarioDefinition,
    reason: String,
) -> Result<ScenarioReport> {
    let artifacts_dir = root.join(format!("{}-infrastructure-error", definition.name));
    std::fs::create_dir_all(&artifacts_dir)?;
    let report = empty_report(
        &artifacts_dir,
        config,
        definition,
        Verdict::InfrastructureError,
        vec![reason],
    );
    write_report(artifacts_dir.join("scenario-report.json"), &report)?;
    Ok(report)
}

fn failure_report(
    root: &Path,
    config: &ScenarioConfig,
    definition: &ScenarioDefinition,
    reason: String,
) -> Result<ScenarioReport> {
    let artifacts_dir = root.join(format!("{}-runner-error", definition.name));
    std::fs::create_dir_all(&artifacts_dir)?;
    let mut report = empty_report(
        &artifacts_dir,
        config,
        definition,
        Verdict::Break,
        vec![reason],
    );
    report.verification_incomplete = true;
    report.invariant_violated = Some("scenario execution did not complete".to_string());
    write_report(artifacts_dir.join("scenario-report.json"), &report)?;
    Ok(report)
}

fn empty_report(
    artifacts_dir: &Path,
    config: &ScenarioConfig,
    definition: &ScenarioDefinition,
    verdict: Verdict,
    notes: Vec<String>,
) -> ScenarioReport {
    ScenarioReport {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        scenario: definition.name.to_string(),
        seed: config.seed,
        cloud: format!("{:?}", config.cloud),
        scale: format!("{:?}", config.scale),
        artifacts_dir: artifacts_dir.to_string_lossy().into_owned(),
        classifier: OutcomeClassifier::default(),
        verdict,
        passed: false,
        duration_ms: 0,
        notes,
        lifecycle: None,
        lifecycle_errors: Vec::new(),
        timed_out: false,
        recovery_verified: false,
        verification_incomplete: false,
        lease_profile: config.lease_profile.as_arg().to_string(),
        recovery_budget: config.recovery_budget(),
        recovery_events: Vec::new(),
        invariant_violated: None,
        expected_behavior: match definition.expected_behavior {
            FaultExpectation::SafetyPreserved => "safety_preserved".to_string(),
            FaultExpectation::TemporarilyUnavailable => "temporarily_unavailable".to_string(),
        },
    }
}

fn write_report(path: impl AsRef<Path>, report: &ScenarioReport) -> Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(report)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LeaseProfile;
    use crate::worker_protocol::{ObservedOutcome, ReportPhase};

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
        let directory = tempfile::tempdir().expect("create report directory");
        let path = directory.path().join("worker.jsonl");
        let complete = serde_json::to_string(&report()).expect("serialize report");
        std::fs::write(&path, format!("{complete}\n{{\"operation_id\":"))
            .expect("write truncated report");

        let reports = read_report_lines(&path).expect("read complete report prefix");

        assert_eq!(reports.len(), 1);
    }

    #[test]
    fn should_reject_malformed_completed_report_line() {
        let directory = tempfile::tempdir().expect("create report directory");
        let path = directory.path().join("worker.jsonl");
        std::fs::write(&path, "{not-json}\n").expect("write malformed report");

        let result = read_report_lines(&path);

        assert!(result.is_err());
    }

    #[test]
    fn should_pass_resolved_endpoint_to_every_worker() {
        // Arrange
        let metadata = RunMetadata {
            scenario: "test".to_string(),
            seed: 1,
            cloud: BackendKind::S3,
            scale: "Small".to_string(),
            cloud_prefix: "prefix".to_string(),
            lease_profile: LeaseProfile::Conservative,
            provider_endpoint: Some("http://127.0.0.1:49153".to_string()),
        };

        // Act
        let command = build_worker_command(
            Path::new("worker"),
            &metadata,
            None,
            None,
            None,
            Path::new("db"),
            Path::new("commands"),
            Path::new("report"),
            Path::new("verify"),
            Path::new("lifecycle"),
        );
        let arguments = command
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        // Assert
        assert!(arguments
            .windows(2)
            .any(|pair| { pair == ["--provider-endpoint", "http://127.0.0.1:49153"] }));
    }

    #[test]
    fn should_collect_only_suite_manifests() {
        // Arrange
        let root = tempfile::tempdir().expect("create aggregate root");
        std::fs::create_dir(root.path().join("stale-scenario")).expect("create stale directory");
        std::fs::write(
            root.path().join("stale-scenario/scenario-report.json"),
            "{}",
        )
        .expect("write stale report");

        // Act
        let aggregate =
            collect_reports(root.path().to_str().expect("UTF-8 path")).expect("collect manifests");

        // Assert
        assert!(aggregate.suites.is_empty());
    }

    #[test]
    fn should_resume_at_failed_operation_after_fault_worker_exits() {
        // Arrange
        let reports = vec![
            report(),
            OperationReport {
                operation_id: 2,
                sequence: 1,
                key: "key-2".to_string(),
                phase: ReportPhase::Mutation,
                outcome: ObservedOutcome::Failed {
                    operation_id: 2,
                    sequence: 1,
                    key: "key-2".to_string(),
                    error: "injected failure".to_string(),
                },
            },
        ];

        // Act
        let next = next_worker_start(0, &reports);

        // Assert
        assert_eq!(next, 1);
    }

    #[test]
    fn should_clear_acquisition_lock_after_fault_worker_terminates() {
        // Arrange
        let directory = tempfile::tempdir().expect("create database directory");
        let lock = directory.path().join(".midge_leader.lock");
        std::fs::write(&lock, "terminated worker").expect("write acquisition lock");

        // Act
        clear_terminated_worker_acquisition_lock(directory.path())
            .expect("clear terminated worker lock");

        // Assert
        assert!(!lock.exists());
    }

    #[test]
    fn should_write_frontier_manifest_inside_unique_execution_directory() {
        // Arrange
        let directory = tempfile::tempdir().expect("create frontier directory");
        let report = FrontierReport {
            schema_version: "midge-destroyer.frontier/v3".to_string(),
            scenario: "recovery-crash-loop".to_string(),
            cloud: "Local".to_string(),
            artifacts_dir: directory.path().to_string_lossy().into_owned(),
            seeds_per_scale: 1,
            first_wobble: None,
            first_bend: None,
            first_break: None,
            runs: Vec::new(),
        };

        // Act
        write_frontier_manifest(&report).expect("write frontier manifest");

        // Assert
        let path = directory.path().join("frontier-manifest.json");
        let saved: FrontierReport =
            serde_json::from_slice(&std::fs::read(path).expect("read frontier manifest"))
                .expect("parse frontier manifest");
        assert_eq!(saved.schema_version, report.schema_version);
        assert_eq!(saved.artifacts_dir, report.artifacts_dir);
    }
}
