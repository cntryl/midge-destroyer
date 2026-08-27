use crate::scenario::{MutationAction, WorkloadKind, WorkloadLane};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleReport {
    pub options_ms: u128,
    pub open_ms: u128,
    pub mutations_ms: u128,
    pub first_mutation_ms: Option<u128>,
    pub verification_ms: u128,
    pub shutdown_ms: u128,
    pub total_ms: u128,
    pub operations_completed: usize,
    pub interrupted: bool,
    pub crashed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LifecycleErrorReport {
    pub stage: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLifecycleChannel {
    pub schema_version: String,
    #[serde(default)]
    pub lifecycle: Option<LifecycleReport>,
    #[serde(default)]
    pub errors: Vec<LifecycleErrorReport>,
}

impl WorkerLifecycleChannel {
    #[must_use]
    pub fn timing(lifecycle: LifecycleReport) -> Self {
        Self {
            schema_version: "midge-destroyer.lifecycle/v2".to_string(),
            lifecycle: Some(lifecycle),
            errors: Vec::new(),
        }
    }

    pub fn error(stage: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            schema_version: "midge-destroyer.lifecycle/v2".to_string(),
            lifecycle: None,
            errors: vec![LifecycleErrorReport {
                stage: stage.into(),
                error: error.into(),
            }],
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReportPhase {
    #[default]
    Mutation,
    Verification,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCommand {
    pub operation_id: u64,
    pub sequence: usize,
    pub action: MutationAction,
    pub key: String,
    pub value: Option<String>,
    pub durable: bool,
    #[serde(default)]
    pub workload_lane: WorkloadLane,
    #[serde(default)]
    pub workload_batch: usize,
    #[serde(default)]
    pub workload_kind: WorkloadKind,
    #[serde(default = "default_column_family")]
    pub column_family: String,
}

fn default_column_family() -> String {
    "default".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OperationReport {
    pub operation_id: u64,
    pub sequence: usize,
    pub key: String,
    #[serde(default)]
    pub phase: ReportPhase,
    pub outcome: ObservedOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum ObservedOutcome {
    Acked {
        operation_id: u64,
        sequence: usize,
        key: String,
    },
    Failed {
        operation_id: u64,
        sequence: usize,
        key: String,
        error: String,
    },
    Unknown {
        operation_id: u64,
        sequence: usize,
        key: String,
    },
}

impl ObservedOutcome {
    #[must_use]
    pub fn operation_id(&self) -> u64 {
        match self {
            Self::Acked { operation_id, .. }
            | Self::Failed { operation_id, .. }
            | Self::Unknown { operation_id, .. } => *operation_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OperationReport, WorkerCommand, WorkerLifecycleChannel};
    use crate::scenario::{WorkloadKind, WorkloadLane};

    #[test]
    fn should_keep_lifecycle_errors_out_of_mutation_report_schema() {
        // Arrange
        let lifecycle = WorkerLifecycleChannel::error("engine", "Writer lease held");
        let serialized = serde_json::to_value(lifecycle).expect("serialize lifecycle channel");

        // Act
        let mutation = serde_json::from_value::<OperationReport>(serialized);

        // Assert
        assert!(mutation.is_err());
    }

    #[test]
    fn should_default_legacy_commands_to_pointwise_default_column_family() {
        // Arrange
        let raw = serde_json::json!({
            "operation_id": 1,
            "sequence": 0,
            "action": "put",
            "key": "key",
            "value": "value",
            "durable": true,
            "workload_lane": "pointwise",
            "workload_batch": 0
        });

        // Act
        let command: WorkerCommand = serde_json::from_value(raw).expect("deserialize command");

        // Assert
        assert_eq!(command.workload_kind, WorkloadKind::Pointwise);
        assert_eq!(command.workload_lane, WorkloadLane::Pointwise);
        assert_eq!(command.column_family, "default");
    }
}
