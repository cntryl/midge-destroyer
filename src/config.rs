use crate::types::BackendKind;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum LeaseProfile {
    #[default]
    Conservative,
    BoundedFailover,
}

impl LeaseProfile {
    #[must_use]
    pub fn ttl_ms(self) -> u64 {
        30_000
    }

    #[must_use]
    pub fn skew_ms(self) -> u64 {
        match self {
            Self::Conservative => 15_000,
            Self::BoundedFailover => 5_000,
        }
    }

    #[must_use]
    pub fn as_arg(self) -> &'static str {
        match self {
            Self::Conservative => "conservative",
            Self::BoundedFailover => "bounded-failover",
        }
    }

    #[must_use]
    pub fn recovery_budget(self, requested_hard_deadline_ms: u64) -> RecoveryBudget {
        let soft_deadline_ms = self
            .ttl_ms()
            .saturating_add(self.skew_ms())
            .saturating_add(5_000);
        RecoveryBudget {
            warning_threshold_ms: soft_deadline_ms.saturating_mul(80) / 100,
            soft_deadline_ms,
            hard_deadline_ms: requested_hard_deadline_ms.max(soft_deadline_ms.saturating_mul(2)),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecoveryBudget {
    pub warning_threshold_ms: u64,
    pub soft_deadline_ms: u64,
    pub hard_deadline_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RunScale {
    Small,
    Medium,
    Large,
    XLarge,
}

impl RunScale {
    #[must_use]
    pub fn concurrency(self) -> usize {
        match self {
            Self::Small => 1,
            Self::Medium => 2,
            Self::Large => 4,
            Self::XLarge => 8,
        }
    }

    #[must_use]
    pub fn ops(self) -> usize {
        match self {
            Self::Small => 24,
            Self::Medium => 80,
            Self::Large => 300,
            Self::XLarge => 1_000,
        }
    }

    #[must_use]
    pub fn max_runtime_ms(self) -> u64 {
        match self {
            Self::Small => 10_000,
            Self::Medium => 45_000,
            Self::Large => 120_000,
            Self::XLarge => 300_000,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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
    pub cloud: BackendKind,
    #[serde(default)]
    pub lease_profile: LeaseProfile,
}

impl SuiteConfig {
    #[must_use]
    pub fn from_preset(preset: SuitePreset, cloud: BackendKind) -> Self {
        let scale = match preset {
            SuitePreset::Smoke => RunScale::Small,
            SuitePreset::Standard => RunScale::Medium,
            SuitePreset::Soak => RunScale::Large,
        };
        Self {
            preset,
            scale,
            cloud,
            lease_profile: LeaseProfile::Conservative,
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
    pub recovery_timeout_ms: u64,
    pub fault_window_ms: u64,
    pub cloud_only_manual: bool,
    pub continue_on_failure: bool,
    #[serde(default)]
    pub lease_profile: LeaseProfile,
    #[serde(default)]
    pub provider_endpoint: Option<String>,
}

impl ScenarioConfig {
    #[must_use]
    pub fn default_seed() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
            })
    }

    #[must_use]
    pub fn derived_seed(seed: Option<u64>) -> u64 {
        seed.unwrap_or_else(Self::default_seed)
    }

    #[must_use]
    pub fn runtime_for_scale(scale: RunScale) -> Duration {
        Duration::from_millis(scale.max_runtime_ms())
    }

    #[must_use]
    pub fn recovery_budget(&self) -> RecoveryBudget {
        self.lease_profile.recovery_budget(self.recovery_timeout_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::{LeaseProfile, RecoveryBudget};

    #[test]
    fn should_preserve_conservative_recovery_budget() {
        // Arrange
        let requested_timeout_ms = 60_000;

        // Act
        let budget = LeaseProfile::Conservative.recovery_budget(requested_timeout_ms);

        // Assert
        assert_eq!(
            budget,
            RecoveryBudget {
                warning_threshold_ms: 40_000,
                soft_deadline_ms: 50_000,
                hard_deadline_ms: 100_000,
            }
        );
    }

    #[test]
    fn should_preserve_bounded_failover_recovery_budget() {
        // Arrange
        let requested_timeout_ms = 120_000;

        // Act
        let budget = LeaseProfile::BoundedFailover.recovery_budget(requested_timeout_ms);

        // Assert
        assert_eq!(
            budget,
            RecoveryBudget {
                warning_threshold_ms: 32_000,
                soft_deadline_ms: 40_000,
                hard_deadline_ms: 120_000,
            }
        );
    }
}
