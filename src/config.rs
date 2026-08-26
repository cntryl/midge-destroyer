use crate::types::BackendKind;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RunScale {
    Small,
    Medium,
    Large,
    XLarge,
}

impl RunScale {
    pub fn concurrency(&self) -> usize {
        match self {
            Self::Small => 1,
            Self::Medium => 2,
            Self::Large => 4,
            Self::XLarge => 8,
        }
    }

    pub fn ops(&self) -> usize {
        match self {
            Self::Small => 24,
            Self::Medium => 80,
            Self::Large => 300,
            Self::XLarge => 1_000,
        }
    }

    pub fn max_runtime_ms(&self) -> u64 {
        match self {
            Self::Small => 10_000,
            Self::Medium => 45_000,
            Self::Large => 120_000,
            Self::XLarge => 300_000,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SuitePreset {
    Smoke,
    Standard,
    Soak,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuiteConfig {
    pub preset: SuitePreset,
    pub scale: RunScale,
    pub cloud: bool,
    pub max_scenarios: usize,
    pub max_failures: usize,
}

impl SuiteConfig {
    pub fn from_preset(preset: &SuitePreset, cloud: bool) -> Self {
        match preset {
            SuitePreset::Smoke => Self {
                preset: *preset,
                scale: RunScale::Small,
                cloud,
                max_scenarios: 3,
                max_failures: 1,
            },
            SuitePreset::Standard => Self {
                preset: *preset,
                scale: RunScale::Medium,
                cloud,
                max_scenarios: 6,
                max_failures: 2,
            },
            SuitePreset::Soak => Self {
                preset: *preset,
                scale: RunScale::Large,
                cloud,
                max_scenarios: 12,
                max_failures: 5,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioConfig {
    pub scenario: String,
    pub seed: u64,
    pub cloud: BackendKind,
    pub scale: RunScale,
    pub max_runtime_ms: u64,
    pub fault_window_ms: u64,
    pub cloud_only_manual: bool,
    pub continue_on_failure: bool,
}

impl ScenarioConfig {
    pub fn default_seed() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos() as u64)
    }

    pub fn derived_seed(seed: Option<u64>) -> u64 {
        seed.unwrap_or_else(Self::default_seed)
    }

    pub fn runtime_for_scale(scale: &RunScale) -> Duration {
        Duration::from_millis(scale.max_runtime_ms())
    }
}
