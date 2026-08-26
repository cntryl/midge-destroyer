use crate::scenario::MutationAction;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleReport {
    #[serde(default = "default_lifecycle_schema_version")]
    pub schema_version: String,
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

fn default_lifecycle_schema_version() -> String {
    "midge-destroyer.lifecycle/v1".to_string()
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReportPhase {
    #[default]
    Mutation,
    Verification,
    Lifecycle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCommand {
    pub operation_id: u64,
    pub sequence: usize,
    pub action: MutationAction,
    pub key: String,
    pub value: Option<String>,
    pub durable: bool,
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
    pub fn operation_id(&self) -> u64 {
        match self {
            Self::Acked { operation_id, .. }
            | Self::Failed { operation_id, .. }
            | Self::Unknown { operation_id, .. } => *operation_id,
        }
    }
}
