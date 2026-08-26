use crate::scenario::MutationAction;
use serde::{Deserialize, Serialize};

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
