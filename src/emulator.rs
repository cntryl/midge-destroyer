use crate::types::BackendKind;
use anyhow::{Context, Result};
use serde::Serialize;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
struct CommandResult {
    success: bool,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

trait ComposeExecutor: Send + Sync {
    fn execute(&self, args: &[String], environment: &[(String, String)]) -> Result<CommandResult>;
}

struct RealComposeExecutor;

impl ComposeExecutor for RealComposeExecutor {
    fn execute(&self, args: &[String], environment: &[(String, String)]) -> Result<CommandResult> {
        let mut command = Command::new("docker");
        command.args(args);
        command.envs(environment.iter().map(|(name, value)| (name, value)));
        let output = command.output().context("run docker compose")?;
        Ok(CommandResult {
            success: output.status.success(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

struct ComposeTeardown {
    executor: Arc<dyn ComposeExecutor>,
    base_args: Vec<String>,
    environment: Vec<(String, String)>,
    evidence_dir: PathBuf,
}

impl ComposeTeardown {
    fn capture(&self, label: &str) {
        let safe_label = sanitize(label);
        for (suffix, args) in [
            ("ps.json", vec!["ps", "--format", "json"]),
            ("logs.txt", vec!["logs", "--no-color", "--timestamps"]),
        ] {
            let mut command_args = self.base_args.clone();
            command_args.extend(args.into_iter().map(str::to_string));
            if let Ok(output) = self.executor.execute(&command_args, &self.environment) {
                let mut bytes = output.stdout;
                if !output.stderr.is_empty() {
                    bytes.extend_from_slice(b"\n--- stderr ---\n");
                    bytes.extend_from_slice(&output.stderr);
                }
                let _ = std::fs::write(
                    self.evidence_dir.join(format!("{safe_label}-{suffix}")),
                    bytes,
                );
            }
        }
    }
}

impl Drop for ComposeTeardown {
    fn drop(&mut self) {
        self.capture("teardown");
        let mut args = self.base_args.clone();
        args.extend(
            ["down", "--volumes", "--remove-orphans"]
                .into_iter()
                .map(str::to_string),
        );
        let result = self.executor.execute(&args, &self.environment);
        let message = match result {
            Ok(output) if output.success => "compose teardown completed".to_string(),
            Ok(output) => format!(
                "compose teardown failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
            Err(error) => format!("compose teardown failed to run: {error}"),
        };
        let _ = std::fs::write(self.evidence_dir.join("teardown.txt"), message);
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EmulatorEndpoints {
    pub project: String,
    pub backend: BackendKind,
    pub api: String,
    pub health: String,
}

pub struct EmulatorController {
    backend: BackendKind,
    compose_file: PathBuf,
    project: String,
    evidence_dir: PathBuf,
    environment: Vec<(String, String)>,
    executor: Arc<dyn ComposeExecutor>,
    teardown: Option<ComposeTeardown>,
    endpoints: Option<EmulatorEndpoints>,
}

impl EmulatorController {
    /// Prepare an execution-scoped controller for a real Sqrzl facade backend.
    ///
    /// # Errors
    ///
    /// Returns an error when artifact directories or the preserved Compose
    /// configuration cannot be created.
    pub fn for_backend(
        backend: BackendKind,
        execution_dir: &Path,
        execution_id: &str,
    ) -> Result<Option<Self>> {
        let compose_name = match backend {
            BackendKind::S3 => "compose.s3.yml",
            BackendKind::Azure => "compose.azure.yml",
            BackendKind::Gcs => "compose.gcs.yml",
            BackendKind::Local | BackendKind::Sqrzl => return Ok(None),
        };
        let evidence_dir = execution_dir.join("emulator");
        let blobs_dir = evidence_dir.join("blobs");
        std::fs::create_dir_all(&blobs_dir).context("create Sqrzl artifact directories")?;
        let compose_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(compose_name);
        std::fs::copy(&compose_file, evidence_dir.join("compose.source.yml"))
            .context("preserve Sqrzl Compose source")?;
        let project = sanitize(&format!(
            "midge-destroyer-{backend:?}-{}-{execution_id}",
            std::process::id()
        ));
        let environment = vec![(
            "SQRZL_BLOBS_HOST_PATH".to_string(),
            blobs_dir.to_string_lossy().into_owned(),
        )];
        Ok(Some(Self {
            backend,
            compose_file,
            project,
            evidence_dir,
            environment,
            executor: Arc::new(RealComposeExecutor),
            teardown: None,
            endpoints: None,
        }))
    }

    /// Start or restart Sqrzl and return its resolved API origin.
    ///
    /// # Errors
    ///
    /// Returns an error when Compose fails, dynamic ports cannot be resolved,
    /// or the emulator does not pass its health probe.
    pub fn ensure_ready(&mut self, label: &str) -> Result<String> {
        if self.teardown.is_some() {
            if self.probe(label).is_ok() {
                return Ok(self
                    .endpoints
                    .as_ref()
                    .context("running Sqrzl emulator has no resolved endpoint")?
                    .api
                    .clone());
            }
            self.capture(&format!("{label}-unhealthy"));
            self.teardown.take();
            self.endpoints = None;
        }
        self.start()?;
        self.probe(label)?;
        Ok(self
            .endpoints
            .as_ref()
            .context("started Sqrzl emulator has no resolved endpoint")?
            .api
            .clone())
    }

    /// Probe the resolved Sqrzl health endpoint and persist the observation.
    ///
    /// # Errors
    ///
    /// Returns an error for an unresolved endpoint, failed HTTP health probe,
    /// or an artifact-write failure.
    pub fn probe(&self, label: &str) -> Result<()> {
        let endpoints = self
            .endpoints
            .as_ref()
            .context("Sqrzl endpoints are not resolved")?;
        let result = probe_http_health(&endpoints.health);
        let evidence = serde_json::json!({
            "checked_at_unix_ms": unix_millis(),
            "label": label,
            "health_endpoint": endpoints.health,
            "healthy": result.is_ok(),
            "error": result.as_ref().err().map(ToString::to_string),
        });
        std::fs::write(
            self.evidence_dir
                .join(format!("health-{}-{}.json", unix_millis(), sanitize(label))),
            serde_json::to_vec_pretty(&evidence)?,
        )?;
        result
    }

    pub fn capture(&self, label: &str) {
        if let Some(teardown) = &self.teardown {
            teardown.capture(label);
        }
    }

    pub fn mark_unhealthy(&mut self, label: &str) {
        self.capture(label);
        self.teardown.take();
        self.endpoints = None;
    }

    fn start(&mut self) -> Result<()> {
        let base_args = vec![
            "compose".to_string(),
            "-p".to_string(),
            self.project.clone(),
            "-f".to_string(),
            self.compose_file.to_string_lossy().into_owned(),
        ];
        let teardown = ComposeTeardown {
            executor: Arc::clone(&self.executor),
            base_args: base_args.clone(),
            environment: self.environment.clone(),
            evidence_dir: self.evidence_dir.clone(),
        };
        self.teardown = Some(teardown);

        let mut config_args = base_args.clone();
        config_args.push("config".to_string());
        let config = self.executor.execute(&config_args, &self.environment)?;
        std::fs::write(
            self.evidence_dir.join("compose.resolved.yml"),
            &config.stdout,
        )?;
        if !config.success {
            anyhow::bail!(
                "Sqrzl Compose configuration failed: {}",
                String::from_utf8_lossy(&config.stderr)
            );
        }

        let mut up_args = base_args.clone();
        up_args.extend(["up", "-d", "sqrzl"].into_iter().map(str::to_string));
        let up = self.executor.execute(&up_args, &self.environment)?;
        if !up.success {
            anyhow::bail!(
                "Sqrzl Compose start failed: {}",
                String::from_utf8_lossy(&up.stderr)
            );
        }

        let api = self.resolve_port(&base_args, 9000)?;
        let health = self.resolve_port(&base_args, 9001)?;
        let endpoints = EmulatorEndpoints {
            project: self.project.clone(),
            backend: self.backend,
            api: format!("http://{api}"),
            health: format!("http://{health}/healthz"),
        };
        std::fs::write(
            self.evidence_dir.join("endpoints.json"),
            serde_json::to_vec_pretty(&endpoints)?,
        )?;
        self.endpoints = Some(endpoints);

        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if self.probe("startup").is_ok() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        anyhow::bail!("Sqrzl did not become healthy within 30 seconds")
    }

    fn resolve_port(&self, base_args: &[String], container_port: u16) -> Result<String> {
        let mut args = base_args.to_vec();
        args.push("port".to_string());
        args.push("sqrzl".to_string());
        args.push(container_port.to_string());
        let output = self.executor.execute(&args, &self.environment)?;
        if !output.success {
            anyhow::bail!(
                "cannot resolve Sqrzl port {container_port}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        parse_compose_port(&String::from_utf8_lossy(&output.stdout))
    }
}

/// Parse the loopback socket printed by `docker compose port`.
///
/// # Errors
///
/// Returns an error when no address is present or the value is not a socket.
pub fn parse_compose_port(output: &str) -> Result<String> {
    let address = output
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .context("docker compose port returned no address")?;
    let parsed = address
        .parse::<SocketAddr>()
        .with_context(|| format!("invalid docker compose port address: {address}"))?;
    Ok(parsed.to_string())
}

fn probe_http_health(endpoint: &str) -> Result<()> {
    let authority_and_path = endpoint
        .strip_prefix("http://")
        .context("Sqrzl health endpoint must use HTTP")?;
    let (authority, path) = authority_and_path.split_once('/').map_or_else(
        || (authority_and_path, "/".to_string()),
        |(authority, path)| (authority, format!("/{path}")),
    );
    let address = authority.parse::<SocketAddr>()?;
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(500))?;
    stream.set_read_timeout(Some(Duration::from_millis(500)))?;
    stream.set_write_timeout(Some(Duration::from_millis(500)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n"
    )?;
    let mut response = [0_u8; 256];
    let read = stream.read(&mut response)?;
    let status_line = String::from_utf8_lossy(&response[..read]);
    if status_line.starts_with("HTTP/1.1 200") || status_line.starts_with("HTTP/1.0 200") {
        Ok(())
    } else {
        anyhow::bail!("Sqrzl health probe was not HTTP 200: {status_line}")
    }
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .chars()
        .take(63)
        .collect()
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingExecutor {
        commands: Mutex<Vec<Vec<String>>>,
    }

    impl ComposeExecutor for RecordingExecutor {
        fn execute(
            &self,
            args: &[String],
            _environment: &[(String, String)],
        ) -> Result<CommandResult> {
            self.commands
                .lock()
                .expect("record command")
                .push(args.to_vec());
            Ok(CommandResult {
                success: true,
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }

    #[test]
    fn should_parse_loopback_dynamic_compose_port() {
        assert_eq!(
            parse_compose_port("127.0.0.1:49153\n").expect("parse port"),
            "127.0.0.1:49153"
        );
    }

    #[test]
    fn should_run_compose_teardown_when_guard_drops() {
        // Arrange
        let executor = Arc::new(RecordingExecutor::default());
        let evidence = tempfile::tempdir().expect("create evidence directory");
        let guard = ComposeTeardown {
            executor: Arc::clone(&executor) as Arc<dyn ComposeExecutor>,
            base_args: vec![
                "compose".to_string(),
                "-p".to_string(),
                "unique".to_string(),
            ],
            environment: Vec::new(),
            evidence_dir: evidence.path().to_path_buf(),
        };

        // Act
        drop(guard);

        // Assert
        let commands = executor.commands.lock().expect("read commands");
        assert!(commands.iter().any(|command| {
            command.ends_with(&[
                "down".to_string(),
                "--volumes".to_string(),
                "--remove-orphans".to_string(),
            ])
        }));
    }
}
