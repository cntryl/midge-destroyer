use crate::config::RecoveryBudget;
use crate::ledger::OutcomeClassifier;
use crate::scenario::FaultClass;
use crate::worker_protocol::{LifecycleErrorReport, WorkerLifecycleChannel};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Pass,
    Wobble,
    Bend,
    Break,
    InfrastructureError,
    Skipped,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryOutcome {
    RecoveredBeforeWarning,
    RecoveredBeforeSoftDeadline,
    RecoveredBeforeHardDeadline,
    HardDeadlineExceeded,
    RecoveryFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecoveryEvent {
    pub fault_class: FaultClass,
    pub step: usize,
    pub attempts: usize,
    pub contention_duration_ms: u128,
    pub recovery_latency_ms: Option<u128>,
    pub outcome: RecoveryOutcome,
}

impl RecoveryEvent {
    #[must_use]
    pub fn recovered(
        fault_class: FaultClass,
        step: usize,
        attempts: usize,
        contention_duration_ms: u128,
        recovery_latency_ms: u128,
        budget: RecoveryBudget,
    ) -> Self {
        let outcome = if recovery_latency_ms < u128::from(budget.warning_threshold_ms) {
            RecoveryOutcome::RecoveredBeforeWarning
        } else if recovery_latency_ms <= u128::from(budget.soft_deadline_ms) {
            RecoveryOutcome::RecoveredBeforeSoftDeadline
        } else if recovery_latency_ms <= u128::from(budget.hard_deadline_ms) {
            RecoveryOutcome::RecoveredBeforeHardDeadline
        } else {
            RecoveryOutcome::HardDeadlineExceeded
        };
        Self {
            fault_class,
            step,
            attempts,
            contention_duration_ms,
            recovery_latency_ms: Some(recovery_latency_ms),
            outcome,
        }
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioReport {
    pub schema_version: String,
    pub scenario: String,
    pub seed: u64,
    pub cloud: String,
    pub scale: String,
    pub artifacts_dir: String,
    pub classifier: OutcomeClassifier,
    pub verdict: Verdict,
    pub passed: bool,
    #[serde(default)]
    pub duration_ms: u128,
    pub notes: Vec<String>,
    #[serde(default)]
    pub lifecycle: Option<LifecycleSummary>,
    #[serde(default)]
    pub lifecycle_errors: Vec<LifecycleErrorReport>,
    #[serde(default)]
    pub timed_out: bool,
    #[serde(default)]
    pub recovery_verified: bool,
    #[serde(default)]
    pub verification_incomplete: bool,
    #[serde(default)]
    pub lease_profile: String,
    pub recovery_budget: RecoveryBudget,
    #[serde(default)]
    pub recovery_events: Vec<RecoveryEvent>,
    #[serde(default)]
    pub invariant_violated: Option<String>,
    #[serde(default)]
    pub expected_behavior: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LifecycleSummary {
    pub segments: usize,
    pub total_ms: u128,
    pub open_ms: u128,
    pub mutations_ms: u128,
    pub verification_ms: u128,
    pub shutdown_ms: u128,
}

impl LifecycleSummary {
    #[must_use]
    pub fn from_channels(channels: &[WorkerLifecycleChannel]) -> Self {
        let reports = channels
            .iter()
            .filter_map(|channel| channel.lifecycle.as_ref())
            .collect::<Vec<_>>();
        Self {
            segments: reports.len(),
            total_ms: reports.iter().map(|report| report.total_ms).sum(),
            open_ms: reports.iter().map(|report| report.open_ms).sum(),
            mutations_ms: reports.iter().map(|report| report.mutations_ms).sum(),
            verification_ms: reports.iter().map(|report| report.verification_ms).sum(),
            shutdown_ms: reports.iter().map(|report| report.shutdown_ms).sum(),
        }
    }
}

#[must_use]
pub fn classify_verdict(
    classifier: &OutcomeClassifier,
    recovery_events: &[RecoveryEvent],
    verification_incomplete: bool,
) -> Verdict {
    if verification_incomplete
        || classifier.failed > 0
        || classifier.unknown > 0
        || classifier.duplicate > 0
        || classifier.missing > 0
        || recovery_events.iter().any(|event| {
            matches!(
                event.outcome,
                RecoveryOutcome::HardDeadlineExceeded | RecoveryOutcome::RecoveryFailed
            )
        })
    {
        return Verdict::Break;
    }
    if recovery_events
        .iter()
        .any(|event| matches!(event.outcome, RecoveryOutcome::RecoveredBeforeHardDeadline))
    {
        return Verdict::Bend;
    }
    if recovery_events
        .iter()
        .any(|event| matches!(event.outcome, RecoveryOutcome::RecoveredBeforeSoftDeadline))
    {
        return Verdict::Wobble;
    }
    Verdict::Pass
}

#[must_use]
pub fn infer_invariant_violation(
    classifier: &OutcomeClassifier,
    recovery_events: &[RecoveryEvent],
    verification_incomplete: bool,
) -> Option<String> {
    if classifier.duplicate > 0 {
        Some("an operation was observed more than once".to_string())
    } else if classifier.missing > 0 {
        Some("expected final state did not survive recovery".to_string())
    } else if classifier.failed > 0 {
        Some("an operation failed before scenario completion".to_string())
    } else if classifier.unknown > 0 {
        Some("recovery did not establish a complete final state".to_string())
    } else if recovery_events.iter().any(|event| {
        matches!(
            event.outcome,
            RecoveryOutcome::HardDeadlineExceeded | RecoveryOutcome::RecoveryFailed
        )
    }) || verification_incomplete
    {
        Some("recovery did not complete by the hard observation deadline".to_string())
    } else {
        None
    }
}

pub fn mark_infrastructure_error(report: &mut ScenarioReport, reason: impl Into<String>) {
    report.verdict = Verdict::InfrastructureError;
    report.passed = false;
    report.invariant_violated = None;
    report.notes.push(reason.into());
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuiteReport {
    pub schema_version: String,
    pub execution_id: String,
    pub preset: String,
    pub backend: String,
    pub seed: u64,
    pub artifacts_dir: String,
    pub scenario_count: usize,
    pub pass_count: usize,
    pub wobble_count: usize,
    pub bend_count: usize,
    pub break_count: usize,
    pub infrastructure_error_count: usize,
    pub skipped_count: usize,
    pub results: Vec<ScenarioReport>,
}

impl SuiteReport {
    #[must_use]
    pub fn new(
        execution_id: String,
        preset: String,
        backend: String,
        seed: u64,
        artifacts_dir: String,
        results: Vec<ScenarioReport>,
    ) -> Self {
        let count = |verdict| {
            results
                .iter()
                .filter(|report| report.verdict == verdict)
                .count()
        };
        Self {
            schema_version: "midge-destroyer.suite-manifest/v3".to_string(),
            execution_id,
            preset,
            backend,
            seed,
            artifacts_dir,
            scenario_count: results.len(),
            pass_count: count(Verdict::Pass),
            wobble_count: count(Verdict::Wobble),
            bend_count: count(Verdict::Bend),
            break_count: count(Verdict::Break),
            infrastructure_error_count: count(Verdict::InfrastructureError),
            skipped_count: count(Verdict::Skipped),
            results,
        }
    }

    /// Serialize the suite manifest as pretty JSON.
    ///
    /// # Errors
    ///
    /// Returns an error when a report field cannot be serialized.
    pub fn to_json_pretty(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierReport {
    pub schema_version: String,
    pub scenario: String,
    pub cloud: String,
    pub artifacts_dir: String,
    pub seeds_per_scale: usize,
    pub first_wobble: Option<ScenarioReport>,
    pub first_bend: Option<ScenarioReport>,
    pub first_break: Option<ScenarioReport>,
    pub runs: Vec<ScenarioReport>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classifier() -> OutcomeClassifier {
        OutcomeClassifier {
            expected: 1,
            acked: 1,
            ..OutcomeClassifier::default()
        }
    }

    fn event(outcome: RecoveryOutcome) -> RecoveryEvent {
        RecoveryEvent {
            fault_class: FaultClass::ProcessKill,
            step: 1,
            attempts: 1,
            contention_duration_ms: 0,
            recovery_latency_ms: Some(1),
            outcome,
        }
    }

    #[test]
    fn should_classify_recovery_latency_verdicts() {
        assert_eq!(
            classify_verdict(
                &classifier(),
                &[event(RecoveryOutcome::RecoveredBeforeWarning)],
                false
            ),
            Verdict::Pass
        );
        assert_eq!(
            classify_verdict(
                &classifier(),
                &[event(RecoveryOutcome::RecoveredBeforeSoftDeadline)],
                false
            ),
            Verdict::Wobble
        );
        assert_eq!(
            classify_verdict(
                &classifier(),
                &[event(RecoveryOutcome::RecoveredBeforeHardDeadline)],
                false
            ),
            Verdict::Bend
        );
        assert_eq!(
            classify_verdict(
                &classifier(),
                &[event(RecoveryOutcome::HardDeadlineExceeded)],
                false
            ),
            Verdict::Break
        );
    }

    #[test]
    fn should_attribute_emulator_outage_to_infrastructure() {
        // Arrange
        let budget = RecoveryBudget {
            warning_threshold_ms: 1,
            soft_deadline_ms: 2,
            hard_deadline_ms: 4,
        };
        let mut report = ScenarioReport {
            schema_version: "midge-destroyer.report/v3".to_string(),
            scenario: "test".to_string(),
            seed: 1,
            cloud: "S3".to_string(),
            scale: "Small".to_string(),
            artifacts_dir: "artifacts".to_string(),
            classifier: classifier(),
            verdict: Verdict::Break,
            passed: false,
            duration_ms: 0,
            notes: Vec::new(),
            lifecycle: None,
            lifecycle_errors: Vec::new(),
            timed_out: false,
            recovery_verified: false,
            verification_incomplete: true,
            lease_profile: "conservative".to_string(),
            recovery_budget: budget,
            recovery_events: Vec::new(),
            invariant_violated: Some("recovery failed".to_string()),
            expected_behavior: "safety_preserved".to_string(),
        };

        // Act
        mark_infrastructure_error(&mut report, "emulator health probe failed");

        // Assert
        assert_eq!(report.verdict, Verdict::InfrastructureError);
        assert!(report.invariant_violated.is_none());
    }
}
