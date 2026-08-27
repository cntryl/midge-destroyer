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
        name: "uuid-compaction-pressure",
        applicability: BackendApplicability::Any,
        required_feature: None,
        expected_behavior: FaultExpectation::TemporarilyUnavailable,
        smoke: false,
    },
    ScenarioDefinition {
        name: "scan-compaction-starvation",
        applicability: BackendApplicability::Any,
        required_feature: None,
        expected_behavior: FaultExpectation::TemporarilyUnavailable,
        smoke: false,
    },
    ScenarioDefinition {
        name: "snapshot-pinned-gc-pressure",
        applicability: BackendApplicability::Any,
        required_feature: None,
        expected_behavior: FaultExpectation::SafetyPreserved,
        smoke: false,
    },
    ScenarioDefinition {
        name: "multi-cf-hot-cold-interference",
        applicability: BackendApplicability::Any,
        required_feature: None,
        expected_behavior: FaultExpectation::TemporarilyUnavailable,
        smoke: false,
    },
    ScenarioDefinition {
        name: "delete-space-amplification",
        applicability: BackendApplicability::Any,
        required_feature: None,
        expected_behavior: FaultExpectation::SafetyPreserved,
        smoke: false,
    },
    ScenarioDefinition {
        name: "cold-cache-read-storm",
        applicability: BackendApplicability::CloudOnly,
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

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadLane {
    #[default]
    Pointwise,
    Batch,
    Trickle,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadKind {
    #[default]
    Pointwise,
    UuidCompaction,
    ScanCompaction,
    SnapshotPinnedGc,
    MultiCfHotCold,
    DeleteSpaceAmplification,
    ColdCacheReadStorm,
}

impl WorkloadKind {
    fn from_scenario_name(name: &str) -> Option<Self> {
        match name {
            "scan-compaction-starvation" => Some(Self::ScanCompaction),
            "snapshot-pinned-gc-pressure" => Some(Self::SnapshotPinnedGc),
            "multi-cf-hot-cold-interference" => Some(Self::MultiCfHotCold),
            "delete-space-amplification" => Some(Self::DeleteSpaceAmplification),
            "cold-cache-read-storm" => Some(Self::ColdCacheReadStorm),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationOp {
    pub id: u64,
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
        if name == "uuid-compaction-pressure" {
            return Self {
                name: name.to_string(),
                seed,
                scale,
                operations: mixed_lsm_operations(seed, scale),
                faults: Vec::new(),
            };
        }
        if let Some(kind) = WorkloadKind::from_scenario_name(name) {
            return Self {
                name: name.to_string(),
                seed,
                scale,
                operations: adversarial_lsm_operations(seed, scale, kind),
                faults: Vec::new(),
            };
        }
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
                workload_lane: WorkloadLane::Pointwise,
                workload_batch: 0,
                workload_kind: WorkloadKind::Pointwise,
                column_family: default_column_family(),
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
        let mut candidates: Vec<usize> = match name {
            "ack-kill-window" => final_durable_puts(&scenario.operations),
            "uuid-compaction-pressure"
            | "scan-compaction-starvation"
            | "snapshot-pinned-gc-pressure"
            | "multi-cf-hot-cold-interference"
            | "delete-space-amplification"
            | "cold-cache-read-storm" => mixed_lsm_fault_boundaries(scale),
            _ => (0..scenario.operations.len()).collect(),
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

fn mixed_lsm_operation_count(scale: RunScale) -> usize {
    scale.ops().saturating_mul(16)
}

fn mixed_lsm_chunk_size(scale: RunScale) -> usize {
    scale.concurrency().saturating_mul(32)
}

fn mixed_lsm_fault_boundaries(scale: RunScale) -> Vec<usize> {
    let chunk_size = mixed_lsm_chunk_size(scale);
    (chunk_size..mixed_lsm_operation_count(scale))
        .step_by(chunk_size)
        .collect()
}

fn mixed_lsm_operations(seed: u64, scale: RunScale) -> Vec<MutationOp> {
    let operation_count = mixed_lsm_operation_count(scale);
    let chunk_size = mixed_lsm_chunk_size(scale);
    let batch_width = chunk_size.saturating_mul(3) / 4;
    let mut batch_live = Vec::<String>::new();
    let mut trickle_live = Vec::<String>::new();
    let mut operations = Vec::with_capacity(operation_count);

    for sequence in 0..operation_count {
        let workload_batch = sequence / chunk_size;
        let workload_lane = if sequence % chunk_size < batch_width {
            WorkloadLane::Batch
        } else {
            WorkloadLane::Trickle
        };
        let live = match workload_lane {
            WorkloadLane::Batch => &mut batch_live,
            WorkloadLane::Trickle => &mut trickle_live,
            WorkloadLane::Pointwise => unreachable!("mixed workload has two explicit lanes"),
        };
        let action = if sequence > chunk_size && sequence % 17 == 0 && !live.is_empty() {
            MutationAction::Delete
        } else {
            MutationAction::Put
        };
        let key = if action == MutationAction::Delete {
            let index = deterministic_index(seed, sequence, live.len());
            live.swap_remove(index)
        } else if sequence % 7 == 0 && !live.is_empty() {
            live[deterministic_index(seed.rotate_left(9), sequence, live.len())].clone()
        } else {
            let domain = match workload_lane {
                WorkloadLane::Batch => 0xBA7C_0000_0000_0001,
                WorkloadLane::Trickle => 0x71CC_1E00_0000_0002,
                WorkloadLane::Pointwise => unreachable!("mixed workload has two explicit lanes"),
            };
            let key = deterministic_uuid(seed ^ domain, sequence as u64);
            live.push(key.clone());
            key
        };
        let value = (action == MutationAction::Put).then(|| {
            let payload_bytes = [512_usize, 2_048, 8_192][sequence % 3];
            format!("v{seed:016x}-{sequence:08x}-{}", "x".repeat(payload_bytes))
        });
        let durable = match workload_lane {
            WorkloadLane::Batch => !workload_batch.is_multiple_of(3),
            WorkloadLane::Trickle => sequence % 3 != 0,
            WorkloadLane::Pointwise => false,
        };
        operations.push(MutationOp {
            id: seed.wrapping_mul(10_007).wrapping_add(sequence as u64),
            sequence,
            action,
            key,
            value,
            durable,
            workload_lane,
            workload_batch,
            workload_kind: WorkloadKind::UuidCompaction,
            column_family: default_column_family(),
        });
    }
    operations
}

fn adversarial_lsm_operations(
    seed: u64,
    scale: RunScale,
    workload_kind: WorkloadKind,
) -> Vec<MutationOp> {
    let operation_count = mixed_lsm_operation_count(scale);
    let chunk_size = mixed_lsm_chunk_size(scale);
    let key_count = chunk_size.saturating_mul(2).max(1);
    (0..operation_count)
        .map(|sequence| {
            let workload_batch = sequence / chunk_size;
            let slot = sequence % key_count;
            let delete_frequency = if workload_kind == WorkloadKind::DeleteSpaceAmplification {
                3
            } else {
                13
            };
            let action = if sequence >= key_count && sequence.is_multiple_of(delete_frequency) {
                MutationAction::Delete
            } else {
                MutationAction::Put
            };
            let column_family = if workload_kind == WorkloadKind::MultiCfHotCold {
                if sequence % 5 == 0 {
                    "cold"
                } else {
                    "hot"
                }
            } else {
                "default"
            };
            MutationOp {
                id: seed.wrapping_mul(10_007).wrapping_add(sequence as u64),
                sequence,
                action: action.clone(),
                key: format!("w{seed:016x}-{slot:08x}"),
                value: (action == MutationAction::Put).then(|| {
                    let size = if workload_kind == WorkloadKind::DeleteSpaceAmplification {
                        8_192
                    } else {
                        1_024
                    };
                    format!("v{sequence:08x}-{}", "x".repeat(size))
                }),
                durable: !sequence.is_multiple_of(4),
                workload_lane: if sequence % chunk_size < chunk_size.saturating_mul(3) / 4 {
                    WorkloadLane::Batch
                } else {
                    WorkloadLane::Trickle
                },
                workload_batch,
                workload_kind,
                column_family: column_family.to_string(),
            }
        })
        .collect()
}

fn deterministic_index(seed: u64, sequence: usize, len: usize) -> usize {
    let mixed = splitmix64(seed.wrapping_add(sequence as u64));
    usize::try_from(mixed).unwrap_or(usize::MAX) % len
}

fn deterministic_uuid(seed: u64, sequence: u64) -> String {
    let high = splitmix64(seed.wrapping_add(sequence.wrapping_mul(2)));
    let low = splitmix64(seed.wrapping_add(sequence.wrapping_mul(2).wrapping_add(1)));
    let bits = (u128::from(high) << 64) | u128::from(low);
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        bits >> 96,
        (bits >> 80) & 0xffff,
        (bits >> 64) & 0xffff,
        (bits >> 48) & 0xffff,
        bits & 0xffff_ffff_ffff
    )
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
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
        "recovery-crash-loop"
            | "lease-takeover-latency"
            | "uuid-compaction-pressure"
            | "scan-compaction-starvation"
            | "snapshot-pinned-gc-pressure"
            | "multi-cf-hot-cold-interference"
            | "delete-space-amplification"
            | "cold-cache-read-storm"
            | "ack-kill-window"
            | "cloud-cache-loss"
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
        "recovery-crash-loop"
        | "uuid-compaction-pressure"
        | "scan-compaction-starvation"
        | "snapshot-pinned-gc-pressure"
        | "multi-cf-hot-cold-interference"
        | "delete-space-amplification" => &[FaultClass::ProcessKill, FaultClass::ForcedReopen],
        "cold-cache-read-storm" => &[FaultClass::CloudCacheLoss, FaultClass::ProviderLatencySpike],
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
    fn should_generate_unsorted_uuid_pressure_workload() {
        // Arrange
        let scenario = Scenario::new("uuid-compaction-pressure", 77, RunScale::Small);

        // Act
        let keys = scenario
            .operations
            .iter()
            .map(|operation| operation.key.clone())
            .collect::<Vec<_>>();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();

        // Assert
        assert_eq!(scenario.operations.len(), RunScale::Small.ops() * 16);
        assert_ne!(keys, sorted_keys, "generated UUID keys must not be sorted");
        assert!(keys.iter().all(|key| {
            key.len() == 36
                && key.chars().enumerate().all(|(index, character)| {
                    [8, 13, 18, 23].contains(&index) && character == '-'
                        || ![8, 13, 18, 23].contains(&index) && character.is_ascii_hexdigit()
                })
        }));
        assert!(scenario
            .operations
            .iter()
            .any(|operation| operation.workload_lane == WorkloadLane::Batch));
        assert!(scenario
            .operations
            .iter()
            .any(|operation| operation.workload_lane == WorkloadLane::Trickle));
        assert!(scenario.operations.iter().any(|operation| {
            operation
                .value
                .as_ref()
                .is_some_and(|value| value.len() >= 8_192)
        }));
    }

    #[test]
    fn should_align_uuid_pressure_faults_to_completed_chunks() {
        // Arrange
        let scale = RunScale::Large;
        let chunk_size = mixed_lsm_chunk_size(scale);

        // Act
        let plan = DeterministicPlan::from_seed("uuid-compaction-pressure", 41, scale);

        // Assert
        assert_eq!(plan.scenario.faults.len(), scale.concurrency());
        assert!(plan.scenario.faults.iter().all(|fault| {
            fault.step > 0
                && fault.step % chunk_size == 0
                && matches!(
                    fault.class,
                    FaultClass::ProcessKill | FaultClass::ForcedReopen
                )
        }));
    }

    #[test]
    fn should_include_process_kill_in_uuid_pressure_seed_window() {
        // Act
        let seed = (0..64).find(|seed| {
            DeterministicPlan::from_seed("uuid-compaction-pressure", *seed, RunScale::Small)
                .scenario
                .faults
                .iter()
                .any(|fault| fault.class == FaultClass::ProcessKill)
        });

        // Assert
        assert_eq!(seed, Some(0));
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
        assert!(local_names.contains(&"uuid-compaction-pressure"));
        assert!(!local_names.contains(&"cloud-cache-loss"));
        assert!(s3_names.contains(&"lease-takeover-latency"));
        assert!(s3_names.contains(&"uuid-compaction-pressure"));
        assert!(s3_names.contains(&"cloud-cache-loss"));
        assert!(s3_names.contains(&"sqrzl-visibility"));
        for name in [
            "scan-compaction-starvation",
            "snapshot-pinned-gc-pressure",
            "multi-cf-hot-cold-interference",
            "delete-space-amplification",
        ] {
            assert!(local_names.contains(&name), "local suite omitted {name}");
            assert!(s3_names.contains(&name), "cloud suite omitted {name}");
        }
        assert!(!local_names.contains(&"cold-cache-read-storm"));
        assert!(s3_names.contains(&"cold-cache-read-storm"));
    }

    #[test]
    fn should_encode_distinct_production_workload_intent_for_new_scenarios() {
        // Arrange
        let cases = [
            ("scan-compaction-starvation", WorkloadKind::ScanCompaction),
            (
                "snapshot-pinned-gc-pressure",
                WorkloadKind::SnapshotPinnedGc,
            ),
            (
                "multi-cf-hot-cold-interference",
                WorkloadKind::MultiCfHotCold,
            ),
            (
                "delete-space-amplification",
                WorkloadKind::DeleteSpaceAmplification,
            ),
            ("cold-cache-read-storm", WorkloadKind::ColdCacheReadStorm),
        ];

        // Act and Assert
        for (name, kind) in cases {
            let scenario = Scenario::new(name, 17, RunScale::Small);
            assert!(!scenario.operations.is_empty());
            assert!(scenario
                .operations
                .iter()
                .all(|operation| operation.workload_kind == kind));
            assert!(scenario
                .operations
                .iter()
                .any(|operation| operation.action == MutationAction::Delete));
        }
    }

    #[test]
    fn should_target_hot_and_cold_column_families_in_interference_workload() {
        // Act
        let scenario = Scenario::new("multi-cf-hot-cold-interference", 19, RunScale::Small);
        let families = scenario
            .operations
            .iter()
            .map(|operation| operation.column_family.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        // Assert
        assert_eq!(families, std::collections::BTreeSet::from(["cold", "hot"]));
        let hot = scenario
            .operations
            .iter()
            .filter(|operation| operation.column_family == "hot")
            .count();
        let cold = scenario.operations.len().saturating_sub(hot);
        assert!(hot >= cold.saturating_mul(3));
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
