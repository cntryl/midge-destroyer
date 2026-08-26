use crate::config::RunScale;
use crate::ledger::{LedgerEntry, MutationOutcome};
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
            Self::SstCorruption | Self::ExactWalPathFault | Self::ManifestCheckpointCut => {
                FaultExpectation::SafetyPreserved
            }
            Self::ProviderLatencySpike | Self::AckBeforeReportCrash | Self::CloudCacheLoss => {
                FaultExpectation::SafetyPreserved
            }
        }
    }

    pub fn all() -> &'static [Self] {
        use FaultClass::*;
        const FAULTS: &[FaultClass] = &[
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
        ];
        FAULTS
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FaultExpectation {
    SafetyPreserved,
    TemporarilyUnavailable,
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
    pub fn new(name: &str, seed: u64, scale: RunScale) -> Self {
        let mut ops = Vec::new();
        for i in 0..scale.ops() {
            let action = if i % 11 == 0 {
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
                key: format!("k{seed:016x}-{i:04}"),
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

    pub fn with_faults(mut self, faults: Vec<ScenarioFault>) -> Self {
        self.faults = faults;
        self
    }

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
    pub fn from_seed(name: &str, seed: u64, scale: RunScale) -> Self {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut scenario = Scenario::new(name, seed, scale);

        let fault_count = match (name, scale) {
            ("smoke-local", _) => 0,
            ("recovery-crash-loop", RunScale::Small) => 1,
            ("recovery-crash-loop", RunScale::Medium) => 2,
            ("recovery-crash-loop", RunScale::Large) => 4,
            ("recovery-crash-loop", RunScale::XLarge) => 8,
            ("ack-kill-window" | "cloud-cache-loss", RunScale::Small) => 1,
            ("ack-kill-window" | "cloud-cache-loss", RunScale::Medium) => 2,
            ("ack-kill-window" | "cloud-cache-loss", RunScale::Large) => 4,
            ("ack-kill-window" | "cloud-cache-loss", RunScale::XLarge) => 8,
            _ => (scale.ops() as f64 * 0.15).round() as usize,
        };
        let mut candidates: Vec<usize> = if name == "ack-kill-window" {
            scenario
                .operations
                .iter()
                .enumerate()
                .filter(|(_, operation)| {
                    operation.durable && operation.action == MutationAction::Put
                })
                .map(|(index, _)| index)
                .collect()
        } else {
            (0..scale.ops()).collect()
        };
        candidates.shuffle(&mut rng);
        let mut faults = Vec::with_capacity(fault_count.min(candidates.len()));

        for step in candidates.iter().take(fault_count.min(candidates.len())) {
            let class = *fault_catalog(name).choose(&mut rng).expect("fault catalog");
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

fn fault_catalog(name: &str) -> &'static [FaultClass] {
    use FaultClass::*;
    match name {
        "recovery-crash-loop" => &[ProcessKill, ForcedReopen],
        "ack-kill-window" => &[AckBeforeReportCrash],
        "cloud-cache-loss" => &[CloudCacheLoss],
        "wal-sync-ack-cut" => &[ExactWalPathFault],
        "manifest-sync-failure" => &[ManifestCheckpointCut],
        "compaction-commit-cut" => &[FlushCompactionBarrierFault],
        "wal-prune-cut" => &[CompactionRace],
        "lease-renewal-failure" => &[LeaseRenewalCut],
        "smoke-local" => &[],
        "dupe-dispatch" => &[ProcessKill, DroppedWrite],
        "flush-barrier" => &[FlushCompactionBarrierFault, CompactionRace],
        "manifest-race" => &[ManifestInterruption],
        "sst-corruption" => &[SstCorruption],
        "sqrzl-visibility" => &[ProviderLatencySpike, RegionPartition, LeaseStalenessWindow],
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
        let expected = (RunScale::Small.ops() as f64 * 0.15).round() as usize;
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
        for (class, operation) in targeted {
            assert_eq!(*class, FaultClass::AckBeforeReportCrash);
            assert!(operation.durable);
            assert_eq!(operation.action, MutationAction::Put);
        }
    }
}
