use crate::config::{RunScale, SuitePreset};
use crate::ledger::{LedgerEntry, MutationOutcome};
use crate::types::BackendKind;
use rand::rngs::SmallRng;
use rand::{prelude::IndexedRandom, prelude::SliceRandom, SeedableRng};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FaultClass {
    ProcessKill,
    ForcedReopen,
    StaleCacheCleanup,
    DroppedWrite,
    WalTruncationRace,
    ManifestInterruption,
    SstCorruption,
    CompactionRace,
    LeaseStalenessWindow,
    ProviderLatencySpike,
    RegionPartition,
    StrictAsyncDurabilityFlip,
    ExactWalPathFault,
    ManifestCheckpointCut,
    FlushCompactionBarrierFault,
    LeaseRenewalCut,
    MigrationBoundaryFault,
    AckBeforeReportCrash,
    CloudCacheLoss,
}

impl FaultClass {
    #[must_use]
    pub fn expected_behavior(&self) -> FaultExpectation {
        match self {
            Self::ProcessKill
            | Self::ForcedReopen
            | Self::StaleCacheCleanup
            | Self::DroppedWrite
            | Self::LeaseStalenessWindow
            | Self::RegionPartition
            | Self::WalTruncationRace
            | Self::ManifestInterruption
            | Self::CompactionRace
            | Self::StrictAsyncDurabilityFlip
            | Self::FlushCompactionBarrierFault
            | Self::LeaseRenewalCut
            | Self::MigrationBoundaryFault => FaultExpectation::TemporarilyUnavailable,
            Self::SstCorruption
            | Self::ExactWalPathFault
            | Self::ManifestCheckpointCut
            | Self::ProviderLatencySpike
            | Self::AckBeforeReportCrash
            | Self::CloudCacheLoss => FaultExpectation::SafetyPreserved,
        }
    }

    #[must_use]
    pub fn all() -> &'static [Self] {
        const FAULTS: &[FaultClass] = &[
            FaultClass::ProcessKill,
            FaultClass::ForcedReopen,
            FaultClass::StaleCacheCleanup,
            FaultClass::DroppedWrite,
            FaultClass::WalTruncationRace,
            FaultClass::ManifestInterruption,
            FaultClass::SstCorruption,
            FaultClass::CompactionRace,
            FaultClass::LeaseStalenessWindow,
            FaultClass::ProviderLatencySpike,
            FaultClass::RegionPartition,
            FaultClass::StrictAsyncDurabilityFlip,
            FaultClass::ExactWalPathFault,
            FaultClass::ManifestCheckpointCut,
            FaultClass::FlushCompactionBarrierFault,
            FaultClass::LeaseRenewalCut,
            FaultClass::MigrationBoundaryFault,
            FaultClass::AckBeforeReportCrash,
            FaultClass::CloudCacheLoss,
        ];
        FAULTS
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FaultExpectation {
    SafetyPreserved,
    TemporarilyUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendApplicability {
    Any,
    LocalOnly,
    CloudOnly,
}

impl BackendApplicability {
    fn includes(self, backend: BackendKind) -> bool {
        match self {
            Self::Any => true,
            Self::LocalOnly => backend == BackendKind::Local,
            Self::CloudOnly => backend.is_cloud(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ScenarioDefinition {
    pub name: &'static str,
    pub applicability: BackendApplicability,
    pub required_feature: Option<&'static str>,
    pub expected_behavior: FaultExpectation,
    pub smoke: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioAvailability {
    Runnable,
    Skipped { reason: &'static str },
}

#[derive(Debug, Clone, Copy)]
pub struct ScenarioSelection {
    pub definition: &'static ScenarioDefinition,
    pub availability: ScenarioAvailability,
}

const SCENARIO_CATALOG: &[ScenarioDefinition] = &[
    ScenarioDefinition {
        name: "smoke-local",
        applicability: BackendApplicability::LocalOnly,
        required_feature: None,
        expected_behavior: FaultExpectation::SafetyPreserved,
        smoke: true,
    },
    ScenarioDefinition {
        name: "sqrzl-visibility",
        applicability: BackendApplicability::CloudOnly,
        required_feature: None,
        expected_behavior: FaultExpectation::TemporarilyUnavailable,
        smoke: true,
    },
    ScenarioDefinition {
        name: "recovery-crash-loop",
        applicability: BackendApplicability::Any,
        required_feature: None,
        expected_behavior: FaultExpectation::TemporarilyUnavailable,
        smoke: false,
    },
    ScenarioDefinition {
        name: "lease-takeover-latency",
        applicability: BackendApplicability::Any,
        required_feature: None,
        expected_behavior: FaultExpectation::TemporarilyUnavailable,
        smoke: false,
    },
    ScenarioDefinition {
        name: "ack-kill-window",
        applicability: BackendApplicability::Any,
        required_feature: None,
        expected_behavior: FaultExpectation::SafetyPreserved,
        smoke: false,
    },
    ScenarioDefinition {
        name: "cloud-cache-loss",
        applicability: BackendApplicability::CloudOnly,
        required_feature: None,
        expected_behavior: FaultExpectation::SafetyPreserved,
        smoke: false,
    },
    ScenarioDefinition {
        name: "manifest-race",
        applicability: BackendApplicability::Any,
        required_feature: None,
        expected_behavior: FaultExpectation::TemporarilyUnavailable,
        smoke: false,
    },
    ScenarioDefinition {
        name: "sst-corruption",
        applicability: BackendApplicability::Any,
        required_feature: None,
        expected_behavior: FaultExpectation::SafetyPreserved,
        smoke: false,
    },
    ScenarioDefinition {
        name: "wal-sync-ack-cut",
        applicability: BackendApplicability::Any,
        required_feature: Some("failpoint-tier"),
        expected_behavior: FaultExpectation::SafetyPreserved,
        smoke: false,
    },
    ScenarioDefinition {
        name: "manifest-sync-failure",
        applicability: BackendApplicability::Any,
        required_feature: Some("failpoint-tier"),
        expected_behavior: FaultExpectation::SafetyPreserved,
        smoke: false,
    },
    ScenarioDefinition {
        name: "compaction-commit-cut",
        applicability: BackendApplicability::Any,
        required_feature: Some("failpoint-tier"),
        expected_behavior: FaultExpectation::TemporarilyUnavailable,
        smoke: false,
    },
    ScenarioDefinition {
        name: "wal-prune-cut",
        applicability: BackendApplicability::CloudOnly,
        required_feature: Some("failpoint-tier"),
        expected_behavior: FaultExpectation::TemporarilyUnavailable,
        smoke: false,
    },
    ScenarioDefinition {
        name: "lease-renewal-failure",
        applicability: BackendApplicability::Any,
        required_feature: Some("failpoint-tier"),
        expected_behavior: FaultExpectation::TemporarilyUnavailable,
        smoke: false,
    },
    ScenarioDefinition {
        name: "flush-barrier",
        applicability: BackendApplicability::Any,
        required_feature: Some("failpoint-tier"),
        expected_behavior: FaultExpectation::TemporarilyUnavailable,
        smoke: false,
    },
];

#[must_use]
pub fn scenario_definition(name: &str) -> Option<&'static ScenarioDefinition> {
    SCENARIO_CATALOG
        .iter()
        .find(|definition| definition.name == name)
}

#[must_use]
pub fn suite_scenarios(
    preset: SuitePreset,
    backend: BackendKind,
    failpoints_enabled: bool,
) -> Vec<ScenarioSelection> {
    SCENARIO_CATALOG
        .iter()
        .filter(|definition| definition.applicability.includes(backend))
        .filter(|definition| match preset {
            SuitePreset::Smoke => definition.smoke,
            SuitePreset::Standard | SuitePreset::Soak => true,
        })
        .map(|definition| {
            let availability = match definition.required_feature {
                Some(_) if !failpoints_enabled => ScenarioAvailability::Skipped {
                    reason: "requires failpoint-tier feature",
                },
                _ => ScenarioAvailability::Runnable,
            };
            ScenarioSelection {
                definition,
                availability,
            }
        })
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MutationAction {
    Put,
    Delete,
    Noop,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationOp {
    pub id: u64,
    pub sequence: usize,
    pub action: MutationAction,
    pub key: String,
    pub value: Option<String>,
    pub durable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScenarioFault {
    pub step: usize,
    pub class: FaultClass,
    pub expect_unstable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Scenario {
    pub name: String,
    pub seed: u64,
    pub scale: RunScale,
    pub operations: Vec<MutationOp>,
    pub faults: Vec<ScenarioFault>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterministicPlan {
    pub scenario: Scenario,
    pub max_runtime: Duration,
}

impl Scenario {
    #[must_use]
    pub fn new(name: &str, seed: u64, scale: RunScale) -> Self {
        let key_count = workload_key_count(scale);
        let mut ops = Vec::new();
        for i in 0..scale.ops() {
            let action = if i >= key_count && i % 11 == 0 {
                MutationAction::Delete
            } else {
                MutationAction::Put
            };
            let is_put = matches!(action, MutationAction::Put);
            let durable = i % 4 != 0;
            ops.push(MutationOp {
                id: seed.wrapping_mul(10_007).wrapping_add(i as u64),
                sequence: i,
                action,
                key: format!("k{seed:016x}-{:04}", i % key_count),
                value: if is_put {
                    Some(format!("v{seed:016x}-{i:04}"))
                } else {
                    None
                },
                durable,
            });
        }

        Self {
            name: name.to_string(),
            seed,
            scale,
            operations: ops,
            faults: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_faults(mut self, faults: Vec<ScenarioFault>) -> Self {
        self.faults = faults;
        self
    }

    #[must_use]
    pub fn append_fault(self, step: usize, class: FaultClass) -> Self {
        let mut faults = self.faults;
        faults.push(ScenarioFault {
            step,
            class,
            expect_unstable: matches!(
                class.expected_behavior(),
                FaultExpectation::TemporarilyUnavailable
            ),
        });
        Self { faults, ..self }
    }
}

impl DeterministicPlan {
    #[must_use]
    pub fn from_seed(name: &str, seed: u64, scale: RunScale) -> Self {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut scenario = Scenario::new(name, seed, scale);

        let fault_count = scenario_fault_count(name, scale);
        let mut candidates: Vec<usize> = if name == "ack-kill-window" {
            final_durable_puts(&scenario.operations)
        } else {
            (0..scale.ops()).collect()
        };
        candidates.shuffle(&mut rng);
        let mut faults = Vec::with_capacity(fault_count.min(candidates.len()));

        for step in candidates.iter().take(fault_count.min(candidates.len())) {
            let Some(class) = fault_catalog(name).choose(&mut rng).copied() else {
                continue;
            };
            faults.push(ScenarioFault {
                step: *step,
                class,
                expect_unstable: matches!(
                    class.expected_behavior(),
                    FaultExpectation::TemporarilyUnavailable
                ),
            });
        }

        scenario.faults = faults;

        Self {
            scenario,
            max_runtime: Duration::from_millis(scale.max_runtime_ms()),
        }
    }

    #[must_use]
    pub fn to_expected_ledger(&self) -> Vec<LedgerEntry> {
        self.scenario
            .operations
            .iter()
            .map(|op| LedgerEntry {
                operation_id: op.id,
                sequence: op.sequence,
                classification: MutationOutcome::Dispatched,
                key: op.key.clone(),
                value: op.value.clone().unwrap_or_default(),
            })
            .collect()
    }
}

fn workload_key_count(scale: RunScale) -> usize {
    scale.concurrency().saturating_mul(4)
}

fn final_durable_puts(operations: &[MutationOp]) -> Vec<usize> {
    let mut final_by_key = std::collections::BTreeMap::new();
    for (index, operation) in operations.iter().enumerate() {
        final_by_key.insert(operation.key.as_str(), index);
    }
    final_by_key
        .into_values()
        .filter(|index| {
            let operation = &operations[*index];
            operation.durable && operation.action == MutationAction::Put
        })
        .collect()
}

fn scenario_fault_count(name: &str, scale: RunScale) -> usize {
    if name == "smoke-local" {
        0
    } else if matches!(
        name,
        "recovery-crash-loop" | "lease-takeover-latency" | "ack-kill-window" | "cloud-cache-loss"
    ) || scenario_definition(name)
        .is_some_and(|definition| definition.required_feature.is_some())
    {
        scale.concurrency()
    } else {
        default_fault_count(scale)
    }
}

fn default_fault_count(scale: RunScale) -> usize {
    scale.ops().saturating_mul(15).saturating_add(50) / 100
}

fn fault_catalog(name: &str) -> &'static [FaultClass] {
    match name {
        "recovery-crash-loop" => &[FaultClass::ProcessKill, FaultClass::ForcedReopen],
        "lease-takeover-latency" => &[
            FaultClass::ProcessKill,
            FaultClass::LeaseStalenessWindow,
            FaultClass::RegionPartition,
            FaultClass::ProviderLatencySpike,
        ],
        "ack-kill-window" => &[FaultClass::AckBeforeReportCrash],
        "cloud-cache-loss" => &[FaultClass::CloudCacheLoss],
        "wal-sync-ack-cut" => &[FaultClass::ExactWalPathFault],
        "manifest-sync-failure" => &[FaultClass::ManifestCheckpointCut],
        "compaction-commit-cut" => &[FaultClass::FlushCompactionBarrierFault],
        "wal-prune-cut" => &[FaultClass::CompactionRace],
        "lease-renewal-failure" => &[FaultClass::LeaseRenewalCut],
        "smoke-local" => &[],
        "dupe-dispatch" => &[FaultClass::ProcessKill, FaultClass::DroppedWrite],
        "flush-barrier" => &[
            FaultClass::FlushCompactionBarrierFault,
            FaultClass::CompactionRace,
        ],
        "manifest-race" => &[FaultClass::ManifestInterruption],
        "sst-corruption" => &[FaultClass::SstCorruption],
        "sqrzl-visibility" => &[
            FaultClass::ProviderLatencySpike,
            FaultClass::RegionPartition,
            FaultClass::LeaseStalenessWindow,
        ],
        _ => FaultClass::all(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_replay_same_plan_for_same_seed() {
        let first = DeterministicPlan::from_seed("repro", 0xFEED_BABE, RunScale::Small);
        let second = DeterministicPlan::from_seed("repro", 0xFEED_BABE, RunScale::Small);
        assert_eq!(first.scenario, second.scenario);
    }

    #[test]
    fn should_vary_plan_when_seed_changes() {
        let first = DeterministicPlan::from_seed("seeded", 1, RunScale::Small);
        let second = DeterministicPlan::from_seed("seeded", 2, RunScale::Small);
        assert_ne!(first.scenario.faults, second.scenario.faults);
    }

    #[test]
    fn should_create_expected_fault_density() {
        let plan = DeterministicPlan::from_seed("density", 9, RunScale::Small);
        let expected = default_fault_count(RunScale::Small);
        assert_eq!(plan.scenario.faults.len(), expected);
    }

    #[test]
    fn should_only_generate_process_faults_for_recovery_crash_loop() {
        let plan = DeterministicPlan::from_seed("recovery-crash-loop", 1, RunScale::Small);
        assert!(plan.scenario.faults.iter().all(|fault| matches!(
            fault.class,
            FaultClass::ProcessKill | FaultClass::ForcedReopen
        )));
        assert_eq!(plan.scenario.faults.len(), 1);
    }

    #[test]
    fn should_target_durable_puts_for_ack_kill_window() {
        // Arrange
        let plan = DeterministicPlan::from_seed("ack-kill-window", 1, RunScale::Medium);

        // Act
        let targeted = plan
            .scenario
            .faults
            .iter()
            .map(|fault| (&fault.class, &plan.scenario.operations[fault.step]));

        // Assert
        assert_eq!(plan.scenario.faults.len(), RunScale::Medium.concurrency());
        for (class, operation) in targeted {
            assert_eq!(*class, FaultClass::AckBeforeReportCrash);
            assert!(operation.durable);
            assert_eq!(operation.action, MutationAction::Put);
            assert!(plan
                .scenario
                .operations
                .iter()
                .skip(operation.sequence.saturating_add(1))
                .all(|later| later.key != operation.key));
        }
    }

    #[test]
    fn should_reuse_bounded_keyspace_when_generating_workload() {
        // Arrange
        let scenario = Scenario::new("recovery-crash-loop", 7, RunScale::Small);

        // Act
        let distinct_keys = scenario
            .operations
            .iter()
            .map(|operation| operation.key.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        // Assert
        assert_eq!(distinct_keys.len(), workload_key_count(RunScale::Small));
        assert!(scenario.operations.len() > distinct_keys.len());
    }

    #[test]
    fn should_delete_existing_keys_when_generating_workload() {
        // Arrange
        let scenario = Scenario::new("recovery-crash-loop", 7, RunScale::Small);

        // Act
        let delete_targets_existing_value =
            scenario
                .operations
                .iter()
                .enumerate()
                .any(|(index, operation)| {
                    operation.action == MutationAction::Delete
                        && scenario.operations[..index].iter().any(|earlier| {
                            earlier.key == operation.key && earlier.action == MutationAction::Put
                        })
                });

        // Assert
        assert!(delete_targets_existing_value);
    }

    #[test]
    fn should_bound_failpoint_faults_by_scale_concurrency() {
        // Act
        let plan = DeterministicPlan::from_seed("lease-renewal-failure", 1, RunScale::Medium);

        // Assert
        assert_eq!(plan.scenario.faults.len(), RunScale::Medium.concurrency());
    }

    #[test]
    fn should_include_every_applicable_black_box_scenario_in_standard_suite() {
        // Act
        let local = suite_scenarios(SuitePreset::Standard, BackendKind::Local, false);
        let s3 = suite_scenarios(SuitePreset::Standard, BackendKind::S3, false);
        let local_names = local
            .iter()
            .map(|selection| selection.definition.name)
            .collect::<Vec<_>>();
        let s3_names = s3
            .iter()
            .map(|selection| selection.definition.name)
            .collect::<Vec<_>>();

        // Assert
        assert!(local_names.contains(&"lease-takeover-latency"));
        assert!(!local_names.contains(&"cloud-cache-loss"));
        assert!(s3_names.contains(&"lease-takeover-latency"));
        assert!(s3_names.contains(&"cloud-cache-loss"));
        assert!(s3_names.contains(&"sqrzl-visibility"));
    }

    #[test]
    fn should_report_failpoint_scenarios_as_skipped_when_feature_is_disabled() {
        // Act
        let selections = suite_scenarios(SuitePreset::Standard, BackendKind::Local, false);

        // Assert
        assert!(selections.iter().any(|selection| {
            selection.definition.name == "wal-sync-ack-cut"
                && matches!(selection.availability, ScenarioAvailability::Skipped { .. })
        }));
    }
}
