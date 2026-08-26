use crate::ledger::OutcomeClassifier;
use crate::worker_protocol::LifecycleReport;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioReport {
    pub schema_version: String,
    pub scenario: String,
    pub seed: u64,
    pub cloud: String,
    pub scale: String,
    pub artifacts_dir: String,
    pub classifier: OutcomeClassifier,
    pub passed: bool,
    #[serde(default)]
    pub duration_ms: u128,
    pub notes: Vec<String>,
    #[serde(default)]
    pub lifecycle: Option<LifecycleSummary>,
    #[serde(default)]
    pub timed_out: bool,
    #[serde(default)]
    pub recovery_verified: bool,
    #[serde(default)]
    pub verification_incomplete: bool,
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
    pub fn from_reports(reports: &[LifecycleReport]) -> Self {
        Self {
            segments: reports.len(),
            total_ms: reports.iter().map(|r| r.total_ms).sum(),
            open_ms: reports.iter().map(|r| r.open_ms).sum(),
            mutations_ms: reports.iter().map(|r| r.mutations_ms).sum(),
            verification_ms: reports.iter().map(|r| r.verification_ms).sum(),
            shutdown_ms: reports.iter().map(|r| r.shutdown_ms).sum(),
        }
    }
}

impl ScenarioReport {
    pub fn new(
        scenario: String,
        seed: u64,
        cloud: String,
        scale: String,
        artifacts_dir: String,
        classifier: OutcomeClassifier,
        notes: Vec<String>,
    ) -> Self {
        let passed = classifier.acked + classifier.missing == classifier.expected
            && classifier.failed == 0
            && classifier.unknown == 0;
        Self {
            schema_version: "midge-destroyer.report/v1".to_string(),
            scenario,
            seed,
            cloud,
            scale,
            artifacts_dir,
            classifier,
            passed,
            duration_ms: 0,
            notes,
            lifecycle: None,
            timed_out: false,
            recovery_verified: false,
            verification_incomplete: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuiteReport {
    pub schema_version: String,
    pub preset: String,
    pub scenario_count: usize,
    pub pass_count: usize,
    pub fail_count: usize,
    pub results: Vec<ScenarioReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierReport {
    pub schema_version: String,
    pub scenario: String,
    pub cloud: String,
    pub seeds_per_scale: usize,
    pub first_wobble: Option<ScenarioReport>,
    pub first_break: Option<ScenarioReport>,
    pub runs: Vec<ScenarioReport>,
}

impl SuiteReport {
    pub fn new(preset: String, results: Vec<ScenarioReport>) -> Self {
        let pass_count = results.iter().filter(|r| r.passed).count();
        let fail_count = results.len().saturating_sub(pass_count);
        Self {
            schema_version: "midge-destroyer.suite-report/v1".to_string(),
            preset,
            scenario_count: results.len(),
            pass_count,
            fail_count,
            results,
        }
    }

    pub fn to_json_pretty(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}
