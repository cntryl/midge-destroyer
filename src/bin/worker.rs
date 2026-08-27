use clap::Parser;
use cntryl_midge::{
    CloudProviderConfig, CloudStorageLocation, OpenOptions, TransactionMode, WriteOptions,
};
use midge_destroyer::scenario::{MutationAction, WorkloadLane};
use std::fs::OpenOptions as FsOpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use midge_destroyer::worker_protocol::{LifecycleReport, WorkerLifecycleChannel};
use midge_destroyer::{
    failpoint,
    worker_protocol::{ObservedOutcome, OperationReport, ReportPhase, WorkerCommand},
};

enum WorkerCompletion {
    Complete,
    Interrupted,
    Incomplete,
}

#[derive(Debug, Parser)]
#[command(name = "midge-destroyer-worker", disable_help_subcommand = true)]
struct WorkerArgs {
    #[arg(long)]
    commands: PathBuf,

    #[arg(long)]
    report: PathBuf,

    #[arg(long)]
    db_root: PathBuf,

    #[arg(long)]
    crash_on_step: Option<usize>,

    #[arg(long)]
    crash_after_step: Option<usize>,

    #[arg(long)]
    interrupt_on_step: Option<usize>,

    #[arg(long, default_value = "local")]
    cloud_provider: String,

    #[arg(long, default_value = "destroyer")]
    cloud_prefix: String,

    #[arg(long)]
    verify_commands: Option<PathBuf>,

    #[arg(long)]
    lifecycle_report: PathBuf,

    #[arg(long, default_value = "conservative")]
    lease_profile: String,

    #[arg(long)]
    provider_endpoint: Option<String>,
}

fn main() {
    let args = WorkerArgs::parse();
    let report_file_result = FsOpenOptions::new()
        .create(true)
        .truncate(false)
        .append(true)
        .open(&args.report);

    let mut report_file = match report_file_result {
        Ok(f) => f,
        Err(error) => {
            let _ = write_lifecycle_error(&args, "mutation-report", error.to_string());
            eprintln!("cannot open report file {}: {error}", args.report.display());
            std::process::exit(2);
        }
    };

    let _ = failpoint::activate_sentinels(&args.db_root.join("failpoints"));

    let data = std::fs::read_to_string(&args.commands);
    let raw = match data {
        Ok(raw) => raw,
        Err(error) => {
            let _ = write_lifecycle_error(&args, "command-read", error.to_string());
            std::process::exit(2);
        }
    };

    let commands: Vec<WorkerCommand> = match serde_json::from_str(&raw) {
        Ok(cmds) => cmds,
        Err(err) => {
            let _ = write_lifecycle_error(&args, "command-parse", err.to_string());
            std::process::exit(2);
        }
    };

    match run_engine(&commands, &mut report_file, &args) {
        Ok(WorkerCompletion::Interrupted) => std::process::exit(3),
        Ok(WorkerCompletion::Incomplete) => std::process::exit(4),
        Ok(WorkerCompletion::Complete) => {}
        Err(error) => {
            let _ = write_lifecycle_error(&args, "engine", error);
            std::process::exit(2);
        }
    }

    std::process::exit(0);
}

#[allow(clippy::too_many_lines)]
fn run_engine(
    commands: &[WorkerCommand],
    report_file: &mut std::fs::File,
    args: &WorkerArgs,
) -> Result<WorkerCompletion, String> {
    let total_started = std::time::Instant::now();
    let options_started = std::time::Instant::now();
    let open_options = match args.cloud_provider.as_str() {
        "local" => OpenOptions::local(&args.db_root),
        "sqrzl" => {
            OpenOptions::cloud_simulated(&args.db_root, "midge-destroyer-cloud", "destroyer")
        }
        "s3" => provider_options(
            &args.db_root,
            facade_provider(s3_provider(), args.provider_endpoint.as_deref())?,
            &args.cloud_prefix,
        ),
        "azure" => provider_options(
            &args.db_root,
            facade_provider(azure_provider(), args.provider_endpoint.as_deref())?,
            &args.cloud_prefix,
        ),
        "gcs" => provider_options(
            &args.db_root,
            facade_provider(gcs_provider(), args.provider_endpoint.as_deref())?,
            &args.cloud_prefix,
        ),
        other => return Err(format!("unsupported cloud provider: {other}")),
    };
    let (lease_ttl, lease_skew) = match args.lease_profile.as_str() {
        "conservative" => (Duration::from_secs(30), Duration::from_secs(15)),
        "bounded-failover" => (Duration::from_secs(30), Duration::from_secs(5)),
        other => return Err(format!("unsupported lease profile: {other}")),
    };
    let open_options = open_options
        .lease_ttl(lease_ttl)
        .lease_clock_skew_tolerance(lease_skew)
        .build()
        .map_err(|error| error.to_string())?;
    let options_ms = options_started.elapsed().as_millis();

    let open_started = std::time::Instant::now();
    let mut engine = cntryl_midge::Engine::open(open_options).map_err(|error| error.to_string())?;
    let open_ms = open_started.elapsed().as_millis();
    let cf = engine
        .get_column_family("default")
        .ok_or_else(|| "default column family not found".to_string())?;

    let mutations_started = std::time::Instant::now();
    let mut first_mutation_ms = None;
    let cloud = args.cloud_provider.as_str() != "local";
    let mut index = 0_usize;
    while index < commands.len() {
        let command = &commands[index];
        let mixed_chunk = command.workload_lane != WorkloadLane::Pointwise;
        let crash_during_chunk = mixed_chunk && Some(index) == args.crash_on_step;
        if !mixed_chunk && Some(index) == args.crash_on_step {
            write_lifecycle(
                args,
                LifecycleReport {
                    options_ms,
                    open_ms,
                    mutations_ms: mutations_started.elapsed().as_millis(),
                    first_mutation_ms,
                    verification_ms: 0,
                    shutdown_ms: 0,
                    total_ms: total_started.elapsed().as_millis(),
                    operations_completed: index,
                    interrupted: false,
                    crashed: true,
                },
            )?;
            std::process::exit(1);
        }
        if crash_during_chunk {
            write_lifecycle(
                args,
                LifecycleReport {
                    options_ms,
                    open_ms,
                    mutations_ms: mutations_started.elapsed().as_millis(),
                    first_mutation_ms,
                    verification_ms: 0,
                    shutdown_ms: 0,
                    total_ms: total_started.elapsed().as_millis(),
                    operations_completed: index,
                    interrupted: false,
                    crashed: true,
                },
            )?;
        }
        if Some(index) == args.interrupt_on_step {
            let mutations_ms = mutations_started.elapsed().as_millis();
            let shutdown_started = std::time::Instant::now();
            let shutdown_result = engine.shutdown(graceful_shutdown_timeout(&args.cloud_provider));
            let shutdown_ms = shutdown_started.elapsed().as_millis();
            write_lifecycle(
                args,
                LifecycleReport {
                    options_ms,
                    open_ms,
                    mutations_ms,
                    first_mutation_ms,
                    verification_ms: 0,
                    shutdown_ms,
                    total_ms: total_started.elapsed().as_millis(),
                    operations_completed: index,
                    interrupted: true,
                    crashed: false,
                },
            )?;
            shutdown_result.map_err(|error| error.to_string())?;
            return Ok(WorkerCompletion::Interrupted);
        }

        let operation_started = std::time::Instant::now();
        let (next_index, outcomes) = if mixed_chunk {
            execute_mixed_chunk(&engine, &cf, commands, index, cloud, crash_during_chunk)?
        } else {
            (
                index.saturating_add(1),
                vec![execute_command(&engine, &cf, command, cloud)],
            )
        };
        if first_mutation_ms.is_none() {
            first_mutation_ms = Some(operation_started.elapsed().as_millis());
        }
        if args
            .crash_after_step
            .is_some_and(|step| step >= index && step < next_index)
        {
            write_lifecycle(
                args,
                LifecycleReport {
                    options_ms,
                    open_ms,
                    mutations_ms: mutations_started.elapsed().as_millis(),
                    first_mutation_ms,
                    verification_ms: 0,
                    shutdown_ms: 0,
                    total_ms: total_started.elapsed().as_millis(),
                    operations_completed: next_index,
                    interrupted: false,
                    crashed: true,
                },
            )?;
            std::process::exit(1);
        }
        let mut incomplete_error = None;
        for outcome in outcomes {
            if incomplete_error.is_none() {
                incomplete_error = match &outcome {
                    ObservedOutcome::Failed { error, .. } => Some(error.clone()),
                    ObservedOutcome::Unknown { sequence, .. } => Some(format!(
                        "mutation outcome was unknown at sequence {sequence}"
                    )),
                    ObservedOutcome::Acked { .. } => None,
                };
            }
            emit_error(report_file, ReportPhase::Mutation, outcome)
                .map_err(|error| error.to_string())?;
        }
        if let Some(error) = incomplete_error {
            let _ = write_lifecycle_error(args, "mutation", error);
            return Ok(WorkerCompletion::Incomplete);
        }

        if !mixed_chunk && command.action == MutationAction::Delete {
            std::thread::sleep(Duration::from_millis(2));
        }
        index = next_index;
    }

    let mutations_ms = mutations_started.elapsed().as_millis();
    let verification_started = std::time::Instant::now();
    if let Some(path) = &args.verify_commands {
        let raw = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
        let all_commands: Vec<WorkerCommand> =
            serde_json::from_str(&raw).map_err(|error| error.to_string())?;
        verify_final_state(&engine, &cf, &all_commands, report_file)?;
    }
    let verification_ms = verification_started.elapsed().as_millis();

    let shutdown_started = std::time::Instant::now();
    let shutdown_result = engine.shutdown(graceful_shutdown_timeout(&args.cloud_provider));
    let shutdown_ms = shutdown_started.elapsed().as_millis();
    write_lifecycle(
        args,
        LifecycleReport {
            options_ms,
            open_ms,
            mutations_ms,
            first_mutation_ms,
            verification_ms,
            shutdown_ms,
            total_ms: total_started.elapsed().as_millis(),
            operations_completed: commands.len(),
            interrupted: false,
            crashed: false,
        },
    )?;
    shutdown_result.map_err(|error| error.to_string())?;

    Ok(WorkerCompletion::Complete)
}

fn graceful_shutdown_timeout(cloud_provider: &str) -> Duration {
    if cloud_provider == "local" {
        Duration::from_secs(2)
    } else {
        // Cloud WAL sealing has a 30-second minimum proof budget. Leave
        // caller-side headroom for provider latency and lease fencing.
        Duration::from_secs(45)
    }
}

fn write_lifecycle(args: &WorkerArgs, report: LifecycleReport) -> Result<(), String> {
    std::fs::write(
        &args.lifecycle_report,
        serde_json::to_vec_pretty(&WorkerLifecycleChannel::timing(report))
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn write_lifecycle_error(
    args: &WorkerArgs,
    stage: &str,
    error: impl Into<String>,
) -> Result<(), String> {
    std::fs::write(
        &args.lifecycle_report,
        serde_json::to_vec_pretty(&WorkerLifecycleChannel::error(stage, error))
            .map_err(|serialization_error| serialization_error.to_string())?,
    )
    .map_err(|write_error| write_error.to_string())
}

fn provider_options(
    cache: &std::path::Path,
    provider: CloudProviderConfig,
    prefix: &str,
) -> cntryl_midge::OpenOptionsBuilder {
    OpenOptions::cloud(cache, CloudStorageLocation::new(provider, prefix))
}

fn s3_provider() -> CloudProviderConfig {
    CloudProviderConfig::sqrzl_s3(env_or("MIDGE_DESTROYER_S3_BUCKET", "midge-destroyer"))
}

fn azure_provider() -> CloudProviderConfig {
    CloudProviderConfig::sqrzl_azure(env_or("MIDGE_DESTROYER_AZURE_CONTAINER", "midge-destroyer"))
}

fn gcs_provider() -> CloudProviderConfig {
    CloudProviderConfig::sqrzl_gcs_json(env_or("MIDGE_DESTROYER_GCS_BUCKET", "midge-destroyer"))
}

fn facade_provider(
    provider: CloudProviderConfig,
    endpoint: Option<&str>,
) -> Result<CloudProviderConfig, String> {
    endpoint.map_or(Ok(provider.clone()), |endpoint| {
        provider
            .with_endpoint(endpoint)
            .map_err(|error| error.to_string())
    })
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn verify_final_state(
    engine: &cntryl_midge::Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    commands: &[WorkerCommand],
    report_file: &mut std::fs::File,
) -> Result<(), String> {
    let mut expected = std::collections::BTreeMap::new();
    let mut identities = std::collections::BTreeMap::new();
    for command in commands {
        let value = match command.action {
            MutationAction::Put => command.value.clone(),
            MutationAction::Delete => None,
            MutationAction::Noop => continue,
        };
        expected.insert(command.key.clone(), value);
        identities.insert(
            command.key.clone(),
            (command.operation_id, command.sequence),
        );
    }
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .map_err(|error| error.to_string())?;
    for (key, expected_value) in expected {
        let actual = tx
            .get(key.as_bytes())
            .map_err(|error| error.to_string())?
            .map(|value| String::from_utf8_lossy(&value).into_owned());
        if actual != expected_value {
            let (operation_id, sequence) = identities[&key];
            emit_error(
                report_file,
                ReportPhase::Verification,
                ObservedOutcome::Failed {
                    operation_id,
                    sequence,
                    key: key.clone(),
                    error: format!("recovery verification mismatch: expected {expected_value:?}, got {actual:?}"),
                },
            )
            .map_err(|error| error.to_string())?;
            return Err(format!("recovery verification failed for key {key}"));
        }
        if expected_value.is_some() {
            let (operation_id, sequence) = identities[&key];
            emit_error(
                report_file,
                ReportPhase::Verification,
                ObservedOutcome::Acked {
                    operation_id,
                    sequence,
                    key: key.clone(),
                },
            )
            .map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

fn emit_error(
    report_file: &mut std::fs::File,
    phase: ReportPhase,
    outcome: ObservedOutcome,
) -> std::io::Result<()> {
    let (operation_id, sequence, key) = match &outcome {
        ObservedOutcome::Acked {
            operation_id,
            sequence,
            key,
        }
        | ObservedOutcome::Failed {
            operation_id,
            sequence,
            key,
            ..
        }
        | ObservedOutcome::Unknown {
            operation_id,
            sequence,
            key,
        } => (*operation_id, *sequence, key.clone()),
    };
    let entry = OperationReport {
        operation_id,
        sequence,
        key,
        phase,
        outcome,
    };
    let line = serde_json::to_string(&entry)?;
    report_file.write_all(line.as_bytes())?;
    report_file.write_all(b"\n")?;
    report_file.sync_all()
}

fn execute_mixed_chunk(
    engine: &cntryl_midge::Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    commands: &[WorkerCommand],
    start: usize,
    cloud: bool,
    crash_during_chunk: bool,
) -> Result<(usize, Vec<ObservedOutcome>), String> {
    let workload_batch = commands[start].workload_batch;
    let end = commands[start..]
        .iter()
        .position(|command| command.workload_batch != workload_batch)
        .map_or(commands.len(), |offset| start.saturating_add(offset));
    let chunk = &commands[start..end];
    let batch_commands = chunk
        .iter()
        .filter(|command| command.workload_lane == WorkloadLane::Batch)
        .collect::<Vec<_>>();
    let trickle_commands = chunk
        .iter()
        .filter(|command| command.workload_lane == WorkloadLane::Trickle)
        .collect::<Vec<_>>();
    if batch_commands.is_empty() || trickle_commands.is_empty() {
        return Err(format!(
            "mixed workload batch {workload_batch} must contain batch and trickle lanes"
        ));
    }
    let probe_start = start.saturating_sub(chunk.len().saturating_mul(3));
    let probe_keys = commands[probe_start..end]
        .iter()
        .map(|command| command.key.clone())
        .collect::<Vec<_>>();
    let read_probe_count = chunk.len().saturating_mul(16);
    let participant_count = if crash_during_chunk { 5 } else { 4 };
    let barrier = Arc::new(Barrier::new(participant_count));

    let mut outcomes = std::thread::scope(|scope| -> Result<Vec<ObservedOutcome>, String> {
        let batch_barrier = Arc::clone(&barrier);
        let batch = scope.spawn(move || {
            batch_barrier.wait();
            execute_batch_commands(engine, cf, &batch_commands, cloud)
        });
        let trickle_barrier = Arc::clone(&barrier);
        let trickle = scope.spawn(move || {
            trickle_barrier.wait();
            let mut outcomes = Vec::with_capacity(trickle_commands.len());
            for command in trickle_commands {
                outcomes.push(execute_command(engine, cf, command, cloud));
                std::thread::sleep(Duration::from_millis(1));
            }
            outcomes
        });
        let read_barrier = Arc::clone(&barrier);
        let reader = scope.spawn(move || {
            read_barrier.wait();
            exercise_read_pressure(engine, cf, &probe_keys, read_probe_count)
        });
        let maintenance_barrier = Arc::clone(&barrier);
        let maintenance = scope.spawn(move || {
            maintenance_barrier.wait();
            apply_lsm_maintenance(engine, cf, workload_batch)
        });
        let crash = crash_during_chunk.then(|| {
            let crash_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                crash_barrier.wait();
                std::thread::sleep(Duration::from_millis(2));
                std::process::exit(1);
            })
        });

        let mut outcomes = batch
            .join()
            .map_err(|_| "mixed workload batch lane panicked".to_string())?;
        outcomes.extend(
            trickle
                .join()
                .map_err(|_| "mixed workload trickle lane panicked".to_string())?,
        );
        reader
            .join()
            .map_err(|_| "mixed workload reader lane panicked".to_string())??;
        maintenance
            .join()
            .map_err(|_| "mixed workload maintenance lane panicked".to_string())??;
        if let Some(crash) = crash {
            crash
                .join()
                .map_err(|_| "mixed workload crash lane panicked".to_string())?;
        }
        Ok(outcomes)
    })?;
    outcomes.sort_by_key(outcome_sequence);
    Ok((end, outcomes))
}

fn execute_batch_commands(
    engine: &cntryl_midge::Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    commands: &[&WorkerCommand],
    cloud: bool,
) -> Vec<ObservedOutcome> {
    let mut tx = match engine.begin_tx(cf.id(), TransactionMode::ReadWrite) {
        Ok(tx) => tx,
        Err(error) => return failed_outcomes(commands, &error.to_string()),
    };
    for command in commands {
        let result = match command.action {
            MutationAction::Put => tx.put(
                command.key.clone().into_bytes(),
                command.value.clone().unwrap_or_default().into_bytes(),
                None,
            ),
            MutationAction::Delete => tx.delete(command.key.clone().into_bytes()),
            MutationAction::Noop => Ok(()),
        };
        if let Err(error) = result {
            return failed_outcomes(commands, &error.to_string());
        }
    }
    let durable = commands.iter().any(|command| command.durable);
    match tx.commit(write_options(cloud, durable)) {
        Ok(()) => commands.iter().map(|command| acked(command)).collect(),
        Err(error) => failed_outcomes(commands, &error.to_string()),
    }
}

fn exercise_read_pressure(
    engine: &cntryl_midge::Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    keys: &[String],
    read_probe_count: usize,
) -> Result<(), String> {
    for probe in 0..read_probe_count {
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let key = &keys[probe % keys.len()];
        let _ = tx.get(key.as_bytes()).map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn apply_lsm_maintenance(
    engine: &cntryl_midge::Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    workload_batch: usize,
) -> Result<(), String> {
    if workload_batch == 0 {
        return Ok(());
    }
    engine.flush_cf(cf).map_err(|error| error.to_string())?;
    if workload_batch.is_multiple_of(4) {
        engine.compact_all().map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn failed_outcomes(commands: &[&WorkerCommand], error: &str) -> Vec<ObservedOutcome> {
    commands
        .iter()
        .map(|command| ObservedOutcome::Failed {
            operation_id: command.operation_id,
            sequence: command.sequence,
            key: command.key.clone(),
            error: error.to_owned(),
        })
        .collect()
}

fn acked(command: &WorkerCommand) -> ObservedOutcome {
    ObservedOutcome::Acked {
        operation_id: command.operation_id,
        sequence: command.sequence,
        key: command.key.clone(),
    }
}

fn outcome_sequence(outcome: &ObservedOutcome) -> usize {
    match outcome {
        ObservedOutcome::Acked { sequence, .. }
        | ObservedOutcome::Failed { sequence, .. }
        | ObservedOutcome::Unknown { sequence, .. } => *sequence,
    }
}

fn write_options(cloud: bool, durable: bool) -> WriteOptions {
    match (cloud, durable) {
        (true, true) => WriteOptions::cloud_strict(),
        (true, false) => WriteOptions::cloud_async(),
        (false, true) => WriteOptions::sync(),
        (false, false) => WriteOptions::buffered(),
    }
}

fn execute_command(
    engine: &cntryl_midge::Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    command: &WorkerCommand,
    cloud: bool,
) -> ObservedOutcome {
    let mut tx = match engine.begin_tx(cf.id(), TransactionMode::ReadWrite) {
        Ok(tx) => tx,
        Err(error) => {
            return ObservedOutcome::Failed {
                operation_id: command.operation_id,
                sequence: command.sequence,
                key: command.key.clone(),
                error: error.to_string(),
            };
        }
    };

    let result = match &command.action {
        MutationAction::Put => tx.put(
            command.key.clone().into_bytes(),
            command.value.clone().unwrap_or_default().into_bytes(),
            None,
        ),
        MutationAction::Delete => tx.delete(command.key.clone().into_bytes()),
        MutationAction::Noop => Ok(()),
    };

    if let Err(error) = result {
        return ObservedOutcome::Failed {
            operation_id: command.operation_id,
            sequence: command.sequence,
            key: command.key.clone(),
            error: error.to_string(),
        };
    }

    match tx.commit(write_options(cloud, command.durable)) {
        Ok(()) => acked(command),
        Err(error) => ObservedOutcome::Failed {
            operation_id: command.operation_id,
            sequence: command.sequence,
            key: command.key.clone(),
            error: error.to_string(),
        },
    }
}
