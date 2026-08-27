use clap::Parser;
use cntryl_midge::{
    CloudProviderConfig, CloudStorageLocation, ColumnFamilyHandle, OpenOptions, Query,
    TransactionMode, WriteOptions,
};
use midge_destroyer::scenario::{MutationAction, WorkloadKind, WorkloadLane};
use std::collections::BTreeMap;
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

type ScannedRows = Vec<(Vec<u8>, Vec<u8>)>;

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

fn lease_durations(profile: &str, cloud_provider: &str) -> Result<(Duration, Duration), String> {
    match profile {
        "conservative" => Ok((Duration::from_secs(30), Duration::from_secs(15))),
        "bounded-failover" if cloud_provider == "local" => {
            Ok((Duration::from_secs(10), Duration::from_secs(5)))
        }
        "bounded-failover" => Ok((Duration::from_secs(30), Duration::from_secs(5))),
        other => Err(format!("unsupported lease profile: {other}")),
    }
}

fn runtime_response_timeout(cloud_provider: &str) -> Duration {
    if cloud_provider == "local" {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(180)
    }
}

#[allow(clippy::too_many_lines)]
fn run_engine(
    commands: &[WorkerCommand],
    report_file: &mut std::fs::File,
    args: &WorkerArgs,
) -> Result<WorkerCompletion, String> {
    let verification_commands = args
        .verify_commands
        .as_ref()
        .map(|path| {
            let raw = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
            serde_json::from_str::<Vec<WorkerCommand>>(&raw).map_err(|error| error.to_string())
        })
        .transpose()?;
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
    let (lease_ttl, lease_skew) = lease_durations(&args.lease_profile, &args.cloud_provider)?;
    let open_options = open_options
        .lease_ttl(lease_ttl)
        .lease_clock_skew_tolerance(lease_skew)
        .runtime_response_timeout(runtime_response_timeout(&args.cloud_provider))
        .build()
        .map_err(|error| error.to_string())?;
    let options_ms = options_started.elapsed().as_millis();

    let open_started = std::time::Instant::now();
    let mut engine = cntryl_midge::Engine::open(open_options).map_err(|error| error.to_string())?;
    let open_ms = open_started.elapsed().as_millis();
    let default_cf = engine
        .get_column_family("default")
        .ok_or_else(|| "default column family not found".to_string())?;
    let mut column_families = BTreeMap::from([("default".to_string(), default_cf)]);
    for name in required_column_family_names(commands, verification_commands.as_deref()) {
        let handle = engine
            .get_column_family(&name)
            .map_or_else(|| engine.create_column_family(&name), Ok)
            .map_err(|error| error.to_string())?;
        column_families.insert(name, handle);
    }

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
            execute_workload_chunk(
                &engine,
                &column_families,
                commands,
                index,
                cloud,
                crash_during_chunk,
            )?
        } else {
            let cf = column_family_for(&column_families, command)?;
            (
                index.saturating_add(1),
                vec![execute_command(&engine, cf, command, cloud)],
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
    if let Some(all_commands) = &verification_commands {
        verify_final_state(&engine, &column_families, all_commands, report_file)?;
        validate_workload_invariants(&engine, all_commands)?;
        write_workload_evidence(&engine, args, all_commands)?;
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

fn required_column_family_names(
    commands: &[WorkerCommand],
    verification_commands: Option<&[WorkerCommand]>,
) -> std::collections::BTreeSet<String> {
    commands
        .iter()
        .chain(verification_commands.into_iter().flatten())
        .map(|command| command.column_family.clone())
        .filter(|name| name != "default")
        .collect()
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
    column_families: &BTreeMap<String, ColumnFamilyHandle>,
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
        let identity = (command.column_family.clone(), command.key.clone());
        expected.insert(identity.clone(), value);
        identities.insert(identity, (command.operation_id, command.sequence));
    }
    for ((column_family, key), expected_value) in expected {
        let cf = column_families
            .get(&column_family)
            .ok_or_else(|| format!("verification column family {column_family} is unavailable"))?;
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let actual = tx
            .get(key.as_bytes())
            .map_err(|error| error.to_string())?
            .map(|value| String::from_utf8_lossy(&value).into_owned());
        if actual != expected_value {
            let (operation_id, sequence) = identities[&(column_family.clone(), key.clone())];
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
            let (operation_id, sequence) = identities[&(column_family.clone(), key.clone())];
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

fn column_family_for<'a>(
    column_families: &'a BTreeMap<String, ColumnFamilyHandle>,
    command: &WorkerCommand,
) -> Result<&'a ColumnFamilyHandle, String> {
    column_families
        .get(&command.column_family)
        .ok_or_else(|| format!("column family {} is unavailable", command.column_family))
}

fn execute_workload_chunk(
    engine: &cntryl_midge::Engine,
    column_families: &BTreeMap<String, ColumnFamilyHandle>,
    commands: &[WorkerCommand],
    start: usize,
    cloud: bool,
    crash_during_chunk: bool,
) -> Result<(usize, Vec<ObservedOutcome>), String> {
    let kind = commands[start].workload_kind;
    match kind {
        WorkloadKind::SnapshotPinnedGc => execute_snapshot_pinned_chunk(
            engine,
            column_family_for(column_families, &commands[start])?,
            commands,
            start,
            cloud,
            crash_during_chunk,
        ),
        WorkloadKind::MultiCfHotCold => execute_multi_cf_chunk(
            engine,
            column_families,
            commands,
            start,
            cloud,
            crash_during_chunk,
        ),
        WorkloadKind::Pointwise => Err("pointwise command entered workload chunk".to_string()),
        _ => execute_mixed_chunk(
            engine,
            column_family_for(column_families, &commands[start])?,
            commands,
            start,
            cloud,
            crash_during_chunk,
            kind,
        ),
    }
}

fn execute_mixed_chunk(
    engine: &cntryl_midge::Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    commands: &[WorkerCommand],
    start: usize,
    cloud: bool,
    crash_during_chunk: bool,
    workload_kind: WorkloadKind,
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
    let read_probe_count = mixed_read_probe_count(chunk.len());
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
            exercise_read_pressure(engine, cf, &probe_keys, read_probe_count, workload_kind)
        });
        let maintenance_barrier = Arc::clone(&barrier);
        let maintenance = scope.spawn(move || {
            maintenance_barrier.wait();
            apply_lsm_maintenance(engine, cf, workload_batch, workload_kind)
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

fn workload_chunk_end(commands: &[WorkerCommand], start: usize) -> usize {
    let workload_batch = commands[start].workload_batch;
    commands[start..]
        .iter()
        .position(|command| command.workload_batch != workload_batch)
        .map_or(commands.len(), |offset| start.saturating_add(offset))
}

fn mixed_read_probe_count(chunk_len: usize) -> usize {
    // Mixed workloads verify read/write overlap. Sustained read saturation is
    // owned by the dedicated cold-cache-read-storm scenario; keeping this
    // lane bounded prevents serial emulator RTT from becoming the workload.
    chunk_len.min(8)
}

fn execute_snapshot_pinned_chunk(
    engine: &cntryl_midge::Engine,
    cf: &ColumnFamilyHandle,
    commands: &[WorkerCommand],
    start: usize,
    cloud: bool,
    crash_during_chunk: bool,
) -> Result<(usize, Vec<ObservedOutcome>), String> {
    let end = workload_chunk_end(commands, start);
    let chunk = commands[start..end].iter().collect::<Vec<_>>();
    let snapshot = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .map_err(|error| error.to_string())?;
    let pinned = engine
        .get_runtime_metrics()
        .map_err(|error| error.to_string())?
        .active_snapshots;
    if pinned == 0 {
        return Err("snapshot workload did not register an active snapshot pin".to_string());
    }
    let before = collect_range_scan(&snapshot)?;
    if crash_during_chunk {
        std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(2));
            std::process::exit(1);
        });
    }
    let outcomes = execute_batch_commands(engine, cf, &chunk, cloud);
    engine.flush_cf(cf).map_err(|error| error.to_string())?;
    engine.compact_all().map_err(|error| error.to_string())?;
    let after = collect_range_scan(&snapshot)?;
    if before != after {
        return Err("snapshot changed while overwrite/delete compaction ran".to_string());
    }
    Ok((end, outcomes))
}

fn execute_multi_cf_chunk(
    engine: &cntryl_midge::Engine,
    column_families: &BTreeMap<String, ColumnFamilyHandle>,
    commands: &[WorkerCommand],
    start: usize,
    cloud: bool,
    crash_during_chunk: bool,
) -> Result<(usize, Vec<ObservedOutcome>), String> {
    let end = workload_chunk_end(commands, start);
    let chunk = &commands[start..end];
    let hot_cf = column_families
        .get("hot")
        .ok_or_else(|| "hot column family is unavailable".to_string())?;
    let cold_cf = column_families
        .get("cold")
        .ok_or_else(|| "cold column family is unavailable".to_string())?;
    let hot_commands = chunk
        .iter()
        .filter(|command| command.column_family == "hot")
        .collect::<Vec<_>>();
    let cold_commands = chunk
        .iter()
        .filter(|command| command.column_family == "cold")
        .collect::<Vec<_>>();
    let cold_keys = cold_commands
        .iter()
        .map(|command| command.key.clone())
        .collect::<Vec<_>>();
    let participants = if crash_during_chunk { 5 } else { 4 };
    let barrier = Arc::new(Barrier::new(participants));
    let mut outcomes = std::thread::scope(|scope| -> Result<Vec<ObservedOutcome>, String> {
        let hot_barrier = Arc::clone(&barrier);
        let hot = scope.spawn(move || {
            hot_barrier.wait();
            execute_batch_commands(engine, hot_cf, &hot_commands, cloud)
        });
        let cold_barrier = Arc::clone(&barrier);
        let cold = scope.spawn(move || {
            cold_barrier.wait();
            execute_batch_commands(engine, cold_cf, &cold_commands, cloud)
        });
        let read_barrier = Arc::clone(&barrier);
        let reads = scope.spawn(move || {
            read_barrier.wait();
            exercise_read_pressure(
                engine,
                cold_cf,
                &cold_keys,
                chunk.len().saturating_mul(16),
                WorkloadKind::ScanCompaction,
            )
        });
        let maintenance_barrier = Arc::clone(&barrier);
        let maintenance = scope.spawn(move || {
            maintenance_barrier.wait();
            engine.flush_cf(hot_cf).map_err(|error| error.to_string())?;
            engine
                .flush_cf(cold_cf)
                .map_err(|error| error.to_string())?;
            engine.compact_all().map_err(|error| error.to_string())
        });
        let crash = crash_during_chunk.then(|| {
            let crash_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                crash_barrier.wait();
                std::thread::sleep(Duration::from_millis(2));
                std::process::exit(1);
            })
        });
        let mut outcomes = hot.join().map_err(|_| "hot CF lane panicked".to_string())?;
        outcomes.extend(
            cold.join()
                .map_err(|_| "cold CF lane panicked".to_string())?,
        );
        reads
            .join()
            .map_err(|_| "cold read lane panicked".to_string())??;
        maintenance
            .join()
            .map_err(|_| "multi-CF maintenance lane panicked".to_string())??;
        if let Some(crash) = crash {
            crash
                .join()
                .map_err(|_| "multi-CF crash lane panicked".to_string())?;
        }
        Ok(outcomes)
    })?;
    outcomes.sort_by_key(outcome_sequence);
    Ok((end, outcomes))
}

fn collect_range_scan(tx: &cntryl_midge::Transaction) -> Result<ScannedRows, String> {
    tx.scan(&Query::new())
        .map_err(|error| error.to_string())?
        .map(|row| {
            row.map(|(key, value)| (key.to_vec(), value.to_vec()))
                .map_err(|error| error.to_string())
        })
        .collect()
}

fn validate_workload_invariants(
    engine: &cntryl_midge::Engine,
    commands: &[WorkerCommand],
) -> Result<(), String> {
    if commands.first().map(|command| command.workload_kind)
        != Some(WorkloadKind::DeleteSpaceAmplification)
    {
        return Ok(());
    }
    let mut live = BTreeMap::new();
    for command in commands {
        live.insert(
            (command.column_family.as_str(), command.key.as_str()),
            command
                .value
                .as_ref()
                .map_or(0_u64, |value| value.len() as u64),
        );
    }
    let live_bytes = live.values().copied().sum::<u64>();
    let metrics = engine
        .get_runtime_metrics()
        .map_err(|error| error.to_string())?;
    let hard_bound = live_bytes.saturating_mul(8).saturating_add(1_048_576);
    if metrics.sst_bytes > hard_bound {
        return Err(format!(
            "space amplification remained unbounded after compaction: sst_bytes={} live_bytes={} hard_bound={hard_bound}",
            metrics.sst_bytes, live_bytes
        ));
    }
    Ok(())
}

fn write_workload_evidence(
    engine: &cntryl_midge::Engine,
    args: &WorkerArgs,
    commands: &[WorkerCommand],
) -> Result<(), String> {
    let Some(kind) = commands.first().map(|command| command.workload_kind) else {
        return Ok(());
    };
    if kind == WorkloadKind::Pointwise {
        return Ok(());
    }
    let runtime = engine
        .get_runtime_metrics()
        .map_err(|error| error.to_string())?;
    let read_amplification = engine
        .get_read_amp_metrics()
        .map_err(|error| error.to_string())?;
    let evidence = serde_json::json!({
        "schema_version": "midge-destroyer.workload-evidence/v1",
        "workload_kind": kind,
        "runtime": runtime,
        "read_amplification": {
            "reads_total": read_amplification.reads_total,
            "ssts_touched_total": read_amplification.ssts_touched_total,
            "l0_ssts_touched_total": read_amplification.l0_ssts_touched_total,
            "blocks_read_total": read_amplification.blocks_read_total,
            "avg_ssts_per_read": read_amplification.avg_ssts_per_read,
            "avg_l0_ssts_per_read": read_amplification.avg_l0_ssts_per_read,
            "avg_blocks_per_read": read_amplification.avg_blocks_per_read,
            "l0_overlap_rate": read_amplification.l0_overlap_rate,
            "sst_budget_violation_rate": read_amplification.sst_budget_violation_rate,
            "block_budget_violation_rate": read_amplification.block_budget_violation_rate,
        },
    });
    let evidence_path = args.lifecycle_report.with_extension("workload.json");
    std::fs::write(
        evidence_path,
        serde_json::to_vec_pretty(&evidence).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
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
    workload_kind: WorkloadKind,
) -> Result<(), String> {
    for probe in 0..read_probe_count {
        exercise_read_probe(engine, cf, keys, probe, workload_kind)?;
    }
    Ok(())
}

fn exercise_read_probe(
    engine: &cntryl_midge::Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    keys: &[String],
    probe: usize,
    workload_kind: WorkloadKind,
) -> Result<(), String> {
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .map_err(|error| error.to_string())?;
    let key = &keys[probe % keys.len()];
    let _ = tx.get(key.as_bytes()).map_err(|error| error.to_string())?;
    if matches!(
        workload_kind,
        WorkloadKind::ScanCompaction
            | WorkloadKind::ColdCacheReadStorm
            | WorkloadKind::DeleteSpaceAmplification
    ) && probe.is_multiple_of(keys.len().max(1))
    {
        let rows = collect_range_scan(&tx)?;
        if rows.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err("range scan returned non-increasing keys".to_string());
        }
    }
    Ok(())
}

fn apply_lsm_maintenance(
    engine: &cntryl_midge::Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    workload_batch: usize,
    workload_kind: WorkloadKind,
) -> Result<(), String> {
    if workload_batch == 0 {
        return Ok(());
    }
    engine.flush_cf(cf).map_err(|error| error.to_string())?;
    if should_force_manual_compaction(workload_kind) {
        engine.compact_all().map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn should_force_manual_compaction(workload_kind: WorkloadKind) -> bool {
    matches!(workload_kind, WorkloadKind::DeleteSpaceAmplification)
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

#[cfg(test)]
mod tests {
    use super::{
        lease_durations, mixed_read_probe_count, required_column_family_names,
        runtime_response_timeout, should_force_manual_compaction,
    };
    use midge_destroyer::scenario::{MutationAction, WorkloadKind, WorkloadLane};
    use midge_destroyer::worker_protocol::WorkerCommand;
    use std::time::Duration;

    fn command(column_family: &str) -> WorkerCommand {
        WorkerCommand {
            operation_id: 1,
            sequence: 0,
            action: MutationAction::Put,
            key: "key".to_string(),
            value: Some("value".to_string()),
            durable: true,
            workload_lane: WorkloadLane::Batch,
            workload_batch: 0,
            workload_kind: WorkloadKind::MultiCfHotCold,
            column_family: column_family.to_string(),
        }
    }

    #[test]
    fn should_open_column_families_referenced_only_by_fresh_verifier() {
        // Arrange
        let mutation_commands = Vec::new();
        let verification_commands = vec![command("hot"), command("cold")];

        // Act
        let names = required_column_family_names(
            &mutation_commands,
            Some(verification_commands.as_slice()),
        );

        // Assert
        assert_eq!(
            names,
            std::collections::BTreeSet::from(["cold".to_string(), "hot".to_string()])
        );
    }

    #[test]
    fn should_match_bounded_failover_worker_lease_to_recovery_budget() {
        // Act
        let (ttl, skew) = lease_durations("bounded-failover", "local").expect("bounded profile");

        // Assert
        assert_eq!(ttl.as_millis(), 10_000);
        assert_eq!(skew.as_millis(), 5_000);
        let (cloud_ttl, cloud_skew) =
            lease_durations("bounded-failover", "s3").expect("cloud bounded profile");
        assert_eq!(cloud_ttl.as_millis(), 30_000);
        assert_eq!(cloud_skew.as_millis(), 5_000);
    }

    #[test]
    fn should_bound_read_overlap_in_mixed_workload_chunks() {
        // Assert
        assert_eq!(mixed_read_probe_count(4), 4);
        assert_eq!(mixed_read_probe_count(128), 8);
        assert_eq!(mixed_read_probe_count(256), 8);
    }

    #[test]
    fn should_allow_cloud_maintenance_to_use_qualification_response_window() {
        // Assert
        assert_eq!(runtime_response_timeout("local"), Duration::from_secs(60));
        assert_eq!(runtime_response_timeout("s3"), Duration::from_secs(180));
    }

    #[test]
    fn should_leave_real_world_mixed_workloads_on_background_compaction() {
        // Assert
        assert!(!should_force_manual_compaction(
            WorkloadKind::UuidCompaction
        ));
        assert!(!should_force_manual_compaction(
            WorkloadKind::ColdCacheReadStorm
        ));
        assert!(should_force_manual_compaction(
            WorkloadKind::DeleteSpaceAmplification
        ));
    }
}
