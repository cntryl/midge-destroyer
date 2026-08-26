use crate::worker_protocol::{ObservedOutcome, OperationReport, ReportPhase};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            schema_version: "midge-destroyer.ledger/v1".to_string(),
        }
    }

    #[must_use]
    pub fn with_entries(entries: Vec<LedgerEntry>) -> Self {
        Self {
            entries,
            schema_version: "midge-destroyer.ledger/v1".to_string(),
        }
    }

    #[must_use]
    pub fn total(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
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
        self.classify_with_unobserved(observed, MutationOutcome::Missing);
    }

    pub fn classify_after_timeout(&mut self, observed: &[ObservedOutcome]) {
        self.classify_with_unobserved(observed, MutationOutcome::Unknown);
    }

    pub fn classify_reports(&mut self, observed: &[OperationReport]) {
        self.classify_reports_with_unobserved(observed, MutationOutcome::Missing);
    }

    pub fn classify_reports_after_timeout(&mut self, observed: &[OperationReport]) {
        self.classify_reports_with_unobserved(observed, MutationOutcome::Unknown);
    }

    fn classify_reports_with_unobserved(
        &mut self,
        observed: &[OperationReport],
        unobserved: MutationOutcome,
    ) {
        let mut mutation_ids = HashSet::new();
        for report in observed {
            match report.phase {
                ReportPhase::Verification => match &report.outcome {
                    ObservedOutcome::Failed { .. } => {
                        self.mark(report.operation_id, MutationOutcome::Missing);
                    }
                    ObservedOutcome::Unknown { .. } => {
                        self.mark(report.operation_id, MutationOutcome::Unknown);
                    }
                    ObservedOutcome::Acked { .. } => {
                        if let Some(entry) = self
                            .entries
                            .iter_mut()
                            .find(|entry| entry.operation_id == report.operation_id)
                        {
                            if entry.classification != MutationOutcome::Duplicate {
                                entry.classification = MutationOutcome::Acked;
                            }
                        }
                    }
                },
                ReportPhase::Mutation => {
                    let repeated = !mutation_ids.insert(report.operation_id);
                    if let Some(entry) = self
                        .entries
                        .iter_mut()
                        .find(|entry| entry.operation_id == report.operation_id)
                    {
                        match &report.outcome {
                            ObservedOutcome::Acked { .. }
                                if repeated && entry.classification == MutationOutcome::Acked =>
                            {
                                entry.classification = MutationOutcome::Duplicate;
                            }
                            ObservedOutcome::Acked { .. } => {
                                entry.classification = MutationOutcome::Acked;
                            }
                            ObservedOutcome::Failed { .. }
                                if entry.classification != MutationOutcome::Acked =>
                            {
                                entry.classification = MutationOutcome::Failed;
                            }
                            ObservedOutcome::Unknown { .. }
                                if entry.classification == MutationOutcome::Dispatched =>
                            {
                                entry.classification = MutationOutcome::Unknown;
                            }
                            ObservedOutcome::Failed { .. } | ObservedOutcome::Unknown { .. } => {}
                        }
                    }
                }
            }
        }

        for entry in &mut self.entries {
            if matches!(entry.classification, MutationOutcome::Dispatched) {
                entry.classification = unobserved;
            }
        }
    }

    fn classify_with_unobserved(
        &mut self,
        observed: &[ObservedOutcome],
        unobserved: MutationOutcome,
    ) {
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
                entry.classification = unobserved;
            }
        }
    }

    /// Serialize the ledger as pretty JSON.
    ///
    /// # Errors
    ///
    /// Returns an error when a ledger field cannot be serialized.
    pub fn serialize_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Read a ledger from JSON.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or parsed.
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
    #[must_use]
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

    #[must_use]
    pub fn is_strictly_safe(&self) -> bool {
        self.failed == 0 && self.unknown == 0 && self.duplicate == 0 && self.missing == 0
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

    #[test]
    fn should_classify_unobserved_outcomes_as_unknown_after_timeout() {
        // Arrange
        let mut ledger = Ledger::with_entries(vec![LedgerEntry {
            operation_id: 1,
            sequence: 0,
            classification: MutationOutcome::Dispatched,
            key: "k1".to_string(),
            value: "v1".to_string(),
        }]);

        // Act
        ledger.classify_after_timeout(&[]);

        // Assert
        assert_eq!(ledger.entries[0].classification, MutationOutcome::Unknown);
    }

    #[test]
    fn should_prefer_verification_loss_over_mutation_ack() {
        // Arrange
        let mut ledger = Ledger::with_entries(vec![LedgerEntry {
            operation_id: 1,
            sequence: 0,
            classification: MutationOutcome::Dispatched,
            key: "k1".to_string(),
            value: "v1".to_string(),
        }]);
        let reports = vec![
            OperationReport {
                operation_id: 1,
                sequence: 0,
                key: "k1".to_string(),
                phase: ReportPhase::Mutation,
                outcome: ObservedOutcome::Acked {
                    operation_id: 1,
                    sequence: 0,
                    key: "k1".to_string(),
                },
            },
            OperationReport {
                operation_id: 1,
                sequence: 0,
                key: "k1".to_string(),
                phase: ReportPhase::Verification,
                outcome: ObservedOutcome::Failed {
                    operation_id: 1,
                    sequence: 0,
                    key: "k1".to_string(),
                    error: "missing after recovery".to_string(),
                },
            },
        ];

        // Act
        ledger.classify_reports(&reports);

        // Assert
        let classifier = OutcomeClassifier::from_ledger(&ledger);
        assert_eq!(classifier.missing, 1);
        assert_eq!(classifier.duplicate, 0);
        assert!(!classifier.is_strictly_safe());
    }

    #[test]
    fn should_resolve_transient_mutation_failure_given_retry_ack() {
        // Arrange
        let mut ledger = Ledger::with_entries(vec![LedgerEntry {
            operation_id: 1,
            sequence: 0,
            classification: MutationOutcome::Dispatched,
            key: "k1".to_string(),
            value: "v1".to_string(),
        }]);
        let reports = vec![
            OperationReport {
                operation_id: 1,
                sequence: 0,
                key: "k1".to_string(),
                phase: ReportPhase::Mutation,
                outcome: ObservedOutcome::Failed {
                    operation_id: 1,
                    sequence: 0,
                    key: "k1".to_string(),
                    error: "injected failure".to_string(),
                },
            },
            OperationReport {
                operation_id: 1,
                sequence: 0,
                key: "k1".to_string(),
                phase: ReportPhase::Mutation,
                outcome: ObservedOutcome::Acked {
                    operation_id: 1,
                    sequence: 0,
                    key: "k1".to_string(),
                },
            },
        ];

        // Act
        ledger.classify_reports(&reports);

        // Assert
        assert_eq!(ledger.entries[0].classification, MutationOutcome::Acked);
    }

    #[test]
    fn should_accept_verified_put_when_ack_was_lost_with_worker() {
        // Arrange
        let mut ledger = Ledger::with_entries(vec![LedgerEntry {
            operation_id: 1,
            sequence: 0,
            classification: MutationOutcome::Dispatched,
            key: "k1".to_string(),
            value: "v1".to_string(),
        }]);
        let reports = vec![OperationReport {
            operation_id: 1,
            sequence: 0,
            key: "k1".to_string(),
            phase: ReportPhase::Verification,
            outcome: ObservedOutcome::Acked {
                operation_id: 1,
                sequence: 0,
                key: "k1".to_string(),
            },
        }];

        // Act
        ledger.classify_reports(&reports);

        // Assert
        assert_eq!(ledger.entries[0].classification, MutationOutcome::Acked);
    }
}
