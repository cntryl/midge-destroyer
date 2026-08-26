use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioMetadata {
    pub schema_version: &'static str,
    pub scenario_name: String,
    pub seed: u64,
    pub scale: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendConfig {
    pub cloud: bool,
    pub scope: String,
    pub sqrzl_simulation: bool,
}

impl BackendConfig {
    pub fn local_root(seed: u64, scale: usize) -> String {
        format!("artifacts/db/local-{seed}-{scale}")
    }

    pub fn sqrzl_root(seed: u64, scale: usize) -> String {
        format!("artifacts/db/sqrzl-{seed}-{scale}")
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    Local,
    Sqrzl,
    S3,
    Azure,
    Gcs,
}

impl BackendKind {
    pub fn is_cloud(self) -> bool {
        !matches!(self, BackendKind::Local)
    }

    pub fn as_arg(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Sqrzl => "sqrzl",
            Self::S3 => "s3",
            Self::Azure => "azure",
            Self::Gcs => "gcs",
        }
    }

    pub fn requires_manual_opt_in(self) -> bool {
        matches!(self, Self::Sqrzl)
    }
}
