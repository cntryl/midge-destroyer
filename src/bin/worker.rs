use clap::Parser;
use cntryl_midge::{
    CloudProviderConfig, CloudStorageLocation, OpenOptions, TransactionMode, WriteOptions,
};
use midge_destroyer::scenario::MutationAction;
use std::fs::OpenOptions as FsOpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use midge_destroyer::{
    failpoint,
    worker_protocol::{ObservedOutcome, OperationReport, WorkerCommand},
};

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
    interrupt_on_step: Option<usize>,

    #[arg(long, default_value = "local")]
    cloud_provider: String,

    #[arg(long)]
    verify_commands: Option<PathBuf>,
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
            eprintln!("cannot open report file {}: {error}", args.report.display());
            std::process::exit(2);
        }
    };

    let _ = failpoint::activate_sentinels(&args.db_root.join("failpoints"));

    let data = std::fs::read_to_string(&args.commands);
    let raw = match data {
        Ok(raw) => raw,
        Err(error) => {
            let _ = emit_error(
                &mut report_file,
                ObservedOutcome::Failed {
                    operation_id: 0,
                    sequence: 0,
                    key: "all".to_string(),
                    error: error.to_string(),
                },
            );
            std::process::exit(2);
        }
    };

    let commands: Vec<WorkerCommand> = match serde_json::from_str(&raw) {
        Ok(cmds) => cmds,
        Err(err) => {
            let _ = emit_error(
                &mut report_file,
                ObservedOutcome::Failed {
                    operation_id: 0,
                    sequence: 0,
                    key: "all".to_string(),
                    error: err.to_string(),
                },
            );
            std::process::exit(2);
        }
    };

    match run_engine(&commands, &mut report_file, &args) {
        Ok(true) => std::process::exit(3),
        Ok(false) => {}
        Err(error) => {
            let _ = emit_error(
                &mut report_file,
                ObservedOutcome::Failed {
                    operation_id: 0,
                    sequence: 0,
                    key: "all".to_string(),
                    error,
                },
            );
            std::process::exit(2);
        }
    }

    std::process::exit(0);
}

fn run_engine(
    commands: &[WorkerCommand],
    report_file: &mut std::fs::File,
    args: &WorkerArgs,
) -> Result<bool, String> {
    let open_options = match args.cloud_provider.as_str() {
        "local" => OpenOptions::local(&args.db_root),
        "sqrzl" => {
            OpenOptions::cloud_simulated(&args.db_root, "midge-destroyer-cloud", "destroyer")
        }
        "s3" => provider_options(&args.db_root, s3_provider()),
        "azure" => provider_options(&args.db_root, azure_provider()),
        "gcs" => provider_options(&args.db_root, gcs_provider()?),
        other => return Err(format!("unsupported cloud provider: {other}")),
    }
    // Keep production lease fencing semantics, but remove the additional
    // clock-skew grace period for deterministic local recovery campaigns.
    .lease_clock_skew_tolerance(Duration::ZERO)
    .build()
    .map_err(|error| error.to_string())?;

    let mut engine = cntryl_midge::Engine::open(open_options).map_err(|error| error.to_string())?;
    let cf = engine
        .get_column_family("default")
        .ok_or_else(|| "default column family not found".to_string())?;

    for (index, command) in commands.iter().enumerate() {
        if Some(index) == args.crash_on_step {
            std::process::exit(1);
        }
        if Some(index) == args.interrupt_on_step {
            engine
                .shutdown(Duration::from_secs(2))
                .map_err(|error| error.to_string())?;
            return Ok(true);
        }

        let outcome = execute_command(
            &engine,
            &cf,
            command,
            args.cloud_provider.as_str() != "local",
        );
        match &outcome {
            ObservedOutcome::Acked { .. } | ObservedOutcome::Failed { .. } => {
                let report = OperationReport {
                    operation_id: command.operation_id,
                    sequence: command.sequence,
                    key: command.key.clone(),
                    outcome,
                };
                if let Ok(json) = serde_json::to_string(&report) {
                    let _ = report_file.write_all(json.as_bytes());
                    let _ = report_file.write_all(b"\n");
                    let _ = report_file.sync_all();
                }
            }
            ObservedOutcome::Unknown { .. } => {
                return Ok(false);
            }
        }

        if command.action == MutationAction::Delete {
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    if let Some(path) = &args.verify_commands {
        let raw = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
        let all_commands: Vec<WorkerCommand> =
            serde_json::from_str(&raw).map_err(|error| error.to_string())?;
        verify_final_state(&engine, &cf, &all_commands, report_file)?;
    }

    let _ = engine
        .shutdown(Duration::from_millis(500))
        .map_err(|error| error.to_string());

    Ok(false)
}

fn provider_options(
    cache: &std::path::Path,
    provider: CloudProviderConfig,
) -> cntryl_midge::OpenOptionsBuilder {
    let prefix =
        std::env::var("MIDGE_DESTROYER_CLOUD_PREFIX").unwrap_or_else(|_| "destroyer".to_string());
    OpenOptions::cloud(cache, CloudStorageLocation::new(provider, prefix))
}

fn s3_provider() -> CloudProviderConfig {
    CloudProviderConfig::sqrzl_s3(env_or("MIDGE_DESTROYER_S3_BUCKET", "midge-destroyer"))
}

fn azure_provider() -> CloudProviderConfig {
    CloudProviderConfig::sqrzl_azure(env_or("MIDGE_DESTROYER_AZURE_CONTAINER", "midge-destroyer"))
}

fn gcs_provider() -> Result<CloudProviderConfig, String> {
    Ok(CloudProviderConfig::sqrzl_gcs_json(env_or(
        "MIDGE_DESTROYER_GCS_BUCKET",
        "midge-destroyer",
    )))
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
    }
    Ok(())
}

fn emit_error(report_file: &mut std::fs::File, outcome: ObservedOutcome) -> std::io::Result<()> {
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
        outcome,
    };
    let line = serde_json::to_string(&entry)?;
    report_file.write_all(line.as_bytes())?;
    report_file.write_all(b"\n")?;
    report_file.sync_all()
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

    let write_options = if cloud && command.durable {
        WriteOptions::cloud_strict()
    } else if cloud {
        WriteOptions::cloud_async()
    } else if command.durable {
        WriteOptions::sync()
    } else {
        WriteOptions::buffered()
    };

    match tx.commit(write_options) {
        Ok(()) => ObservedOutcome::Acked {
            operation_id: command.operation_id,
            sequence: command.sequence,
            key: command.key.clone(),
        },
        Err(error) => ObservedOutcome::Failed {
            operation_id: command.operation_id,
            sequence: command.sequence,
            key: command.key.clone(),
            error: error.to_string(),
        },
    }
}
