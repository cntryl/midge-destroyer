//! Midge Destroyer: external adversarial correctness and recovery harness.

pub mod cli;
pub mod config;
pub mod failpoint;
pub mod ledger;
pub mod report;
pub mod runner;
pub mod scenario;
pub mod types;
pub mod worker_protocol;

pub use config::{RunScale, ScenarioConfig, SuiteConfig, SuitePreset};
pub use ledger::{Ledger, LedgerEntry, OutcomeClassifier};
pub use runner::{collect_reports, run_frontier, run_scenario, run_suite, RunArtifact, RunResult};
pub use scenario::{FaultClass, Scenario};
pub use types::{BackendConfig, ScenarioMetadata};
