use crate::worker_protocol::ObservedOutcome;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MutationOutcome {
    Dispatched,
    Acked,
    Failed,
    Unknown,
    Duplicate,
    Missing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub operation_id: u64,
    pub sequence: usize,
    pub classification: MutationOutcome,
    pub key: String,
    pub value: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Ledger {
    pub entries: Vec<LedgerEntry>,
    pub schema_version: String,
}

impl Ledger {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            schema_version: "midge-destroyer.ledger/v1".to_string(),
        }
    }

    pub fn with_entries(entries: Vec<LedgerEntry>) -> Self {
        Self {
            entries,
            schema_version: "midge-destroyer.ledger/v1".to_string(),
        }
    }

    pub fn total(&self) -> usize {
        self.entries.len()
    }

    pub fn by_id(&self) -> HashMap<u64, &LedgerEntry> {
        self.entries.iter().map(|e| (e.operation_id, e)).collect()
    }

    pub fn append_dispatched(&mut self, entry: LedgerEntry) {
        self.entries.push(entry);
    }

    pub fn mark(&mut self, op_id: u64, classification: MutationOutcome) {
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.operation_id == op_id)
        {
            entry.classification = classification;
            return;
        }
        self.entries.push(LedgerEntry {
            operation_id: op_id,
            sequence: 0,
            classification,
            key: String::new(),
            value: String::new(),
        });
    }

    pub fn classify(&mut self, observed: &[ObservedOutcome]) {
        let mut observed_ids = HashSet::new();
        for sample in observed {
            if !observed_ids.insert(sample.operation_id()) {
                self.mark(sample.operation_id(), MutationOutcome::Duplicate);
                continue;
            }

            let outcome = match sample {
                ObservedOutcome::Acked { .. } => MutationOutcome::Acked,
                ObservedOutcome::Failed { .. } => MutationOutcome::Failed,
                ObservedOutcome::Unknown { .. } => MutationOutcome::Unknown,
            };
            self.mark(sample.operation_id(), outcome);
        }

        for entry in &mut self.entries {
            if matches!(entry.classification, MutationOutcome::Dispatched) {
                entry.classification = MutationOutcome::Missing;
            }
        }
    }

    pub fn serialize_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn from_json(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str::<Self>(&raw)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OutcomeClassifier {
    pub expected: usize,
    pub acked: usize,
    pub failed: usize,
    pub unknown: usize,
    pub duplicate: usize,
    pub missing: usize,
}

impl OutcomeClassifier {
    pub fn from_ledger(ledger: &Ledger) -> Self {
        let mut stats = Self {
            expected: ledger.entries.len(),
            acked: 0,
            failed: 0,
            unknown: 0,
            duplicate: 0,
            missing: 0,
        };

        for entry in &ledger.entries {
            match entry.classification {
                MutationOutcome::Acked => stats.acked += 1,
                MutationOutcome::Failed => stats.failed += 1,
                MutationOutcome::Unknown => stats.unknown += 1,
                MutationOutcome::Duplicate => stats.duplicate += 1,
                MutationOutcome::Missing => stats.missing += 1,
                MutationOutcome::Dispatched => {}
            }
        }

        stats
    }

    pub fn is_strictly_safe(&self) -> bool {
        self.failed == 0 && self.unknown == 0 && self.missing == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker_protocol::ObservedOutcome;

    #[test]
    fn should_classify_observed_outcomes() {
        let entries = vec![
            LedgerEntry {
                operation_id: 1,
                sequence: 0,
                classification: MutationOutcome::Dispatched,
                key: "k1".to_string(),
                value: "v1".to_string(),
            },
            LedgerEntry {
                operation_id: 2,
                sequence: 1,
                classification: MutationOutcome::Dispatched,
                key: "k2".to_string(),
                value: "v2".to_string(),
            },
            LedgerEntry {
                operation_id: 3,
                sequence: 2,
                classification: MutationOutcome::Dispatched,
                key: "k3".to_string(),
                value: String::new(),
            },
            LedgerEntry {
                operation_id: 4,
                sequence: 3,
                classification: MutationOutcome::Dispatched,
                key: "k4".to_string(),
                value: String::new(),
            },
        ];

        let mut ledger = Ledger::with_entries(entries);
        ledger.classify(&[
            ObservedOutcome::Acked {
                operation_id: 1,
                sequence: 0,
                key: "k1".to_string(),
            },
            ObservedOutcome::Failed {
                operation_id: 2,
                sequence: 1,
                key: "k2".to_string(),
                error: "boom".to_string(),
            },
            ObservedOutcome::Acked {
                operation_id: 2,
                sequence: 1,
                key: "k2".to_string(),
            },
            ObservedOutcome::Unknown {
                operation_id: 3,
                sequence: 2,
                key: "k3".to_string(),
            },
        ]);

        let classifier = OutcomeClassifier::from_ledger(&ledger);
        assert_eq!(classifier.acked, 1);
        assert_eq!(classifier.failed, 0);
        assert_eq!(classifier.unknown, 1);
        assert_eq!(classifier.duplicate, 1);
        assert_eq!(classifier.missing, 1);
        assert!(!classifier.is_strictly_safe());
        assert_eq!(
            ledger.by_id().get(&4).unwrap().classification,
            MutationOutcome::Missing
        );
    }
}
