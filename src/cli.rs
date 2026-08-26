use crate::config::{LeaseProfile, RunScale, ScenarioConfig, SuiteConfig, SuitePreset};
use crate::types::BackendKind;
use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Clone, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum CloudArg {
    Local,
    Sqrzl,
    S3,
    Azure,
    Gcs,
}

impl From<CloudArg> for BackendKind {
    fn from(value: CloudArg) -> Self {
        match value {
            CloudArg::Local => BackendKind::Local,
            CloudArg::Sqrzl => BackendKind::Sqrzl,
            CloudArg::S3 => BackendKind::S3,
            CloudArg::Azure => BackendKind::Azure,
            CloudArg::Gcs => BackendKind::Gcs,
        }
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum PresetArg {
    Smoke,
    Standard,
    Soak,
}

impl From<PresetArg> for SuitePreset {
    fn from(value: PresetArg) -> Self {
        match value {
            PresetArg::Smoke => Self::Smoke,
            PresetArg::Standard => Self::Standard,
            PresetArg::Soak => Self::Soak,
        }
    }
}

#[derive(Debug, Clone, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum ScaleArg {
    Small,
    Medium,
    Large,
    XLarge,
}

#[derive(Debug, Clone, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum LeaseProfileArg {
    Conservative,
    BoundedFailover,
}

impl From<LeaseProfileArg> for LeaseProfile {
    fn from(value: LeaseProfileArg) -> Self {
        match value {
            LeaseProfileArg::Conservative => Self::Conservative,
            LeaseProfileArg::BoundedFailover => Self::BoundedFailover,
        }
    }
}

impl From<ScaleArg> for RunScale {
    fn from(value: ScaleArg) -> Self {
        match value {
            ScaleArg::Small => RunScale::Small,
            ScaleArg::Medium => RunScale::Medium,
            ScaleArg::Large => RunScale::Large,
            ScaleArg::XLarge => RunScale::XLarge,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "midge-destroyer",
    version,
    about = "Midge-specific adversarial recoverability harness"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Base artifact path for all runs.
    #[arg(global = true, long, default_value = "artifacts")]
    pub artifacts_root: String,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run one named scenario.
    Run(RunArgs),

    /// Run a preset suite of scenarios.
    Suite(SuiteArgs),

    /// Aggregate all existing run reports.
    Report(ReportArgs),

    /// Escalate workload scales and stop at the first wobble/break frontier.
    Frontier(FrontierArgs),
}

#[derive(Debug, Args)]
pub struct FrontierArgs {
    pub scenario: String,
    #[arg(long, value_enum, default_value_t = CloudArg::Local)]
    pub cloud: CloudArg,
    #[arg(long, default_value_t = 8)]
    pub seeds: usize,
    #[arg(long, default_value_t = 1)]
    pub seed_start: u64,
    #[arg(long, default_value_t = 60)]
    pub recovery_timeout_secs: u64,
    #[arg(long, value_enum, default_value_t = ScaleArg::XLarge)]
    pub max_scale: ScaleArg,
    #[arg(long, value_enum, default_value_t = LeaseProfileArg::Conservative)]
    pub lease_profile: LeaseProfileArg,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    pub scenario: String,

    #[arg(long, value_enum, default_value_t = CloudArg::Local)]
    pub cloud: CloudArg,

    #[arg(long)]
    pub seed: Option<u64>,

    #[arg(long, value_enum, default_value_t = ScaleArg::Medium)]
    pub scale: ScaleArg,

    #[arg(long)]
    pub continue_on_failure: bool,

    #[arg(long, default_value_t = 60)]
    pub recovery_timeout_secs: u64,

    #[arg(long, value_enum, default_value_t = LeaseProfileArg::Conservative)]
    pub lease_profile: LeaseProfileArg,
}

#[derive(Debug, Args)]
pub struct SuiteArgs {
    #[arg(value_enum)]
    pub preset: PresetArg,

    #[arg(long, value_enum, default_value_t = CloudArg::Local)]
    pub cloud: CloudArg,

    #[arg(long)]
    pub report_json: bool,

    #[arg(long)]
    pub max_scenarios: Option<usize>,

    #[arg(long)]
    pub seed: Option<u64>,

    #[arg(long, default_value_t = 60)]
    pub recovery_timeout_secs: u64,

    #[arg(long, value_enum, default_value_t = LeaseProfileArg::Conservative)]
    pub lease_profile: LeaseProfileArg,
}

#[derive(Debug, Args)]
pub struct ReportArgs {
    #[arg(long, default_value = "artifacts")]
    pub artifacts_root: String,

    #[arg(long)]
    pub report_json: bool,
}

#[derive(Debug, Clone)]
pub struct ParsedRunConfig {
    pub scenario: String,
    pub config: ScenarioConfig,
}

impl RunArgs {
    #[must_use]
    pub fn to_config(&self) -> ParsedRunConfig {
        let scale = RunScale::from(self.scale.clone());
        let seed = ScenarioConfig::derived_seed(self.seed);
        ParsedRunConfig {
            scenario: self.scenario.clone(),
            config: ScenarioConfig {
                scenario: self.scenario.clone(),
                seed,
                cloud: self.cloud.clone().into(),
                scale,
                max_runtime_ms: scale.max_runtime_ms(),
                recovery_timeout_ms: self.recovery_timeout_secs.saturating_mul(1_000),
                fault_window_ms: 250,
                cloud_only_manual: matches!(self.cloud, CloudArg::Sqrzl),
                continue_on_failure: self.continue_on_failure,
                lease_profile: self.lease_profile.clone().into(),
                provider_endpoint: None,
            },
        }
    }
}

impl SuiteArgs {
    #[must_use]
    pub fn build_config(&self) -> SuiteConfig {
        let cloud = self.cloud.clone().into();
        let preset = self.preset.clone();
        let mut config = SuiteConfig::from_preset(preset.into(), cloud);
        config.lease_profile = self.lease_profile.clone().into();
        config
    }
}
