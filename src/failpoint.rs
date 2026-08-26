use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailpointSentinel {
    pub name: String,
    pub payload: String,
    pub consumed: bool,
}

pub fn write_sentinel(dir: &Path, name: &str, payload: &str) -> std::io::Result<PathBuf> {
    let path = dir.join(format!("sentinel-{name}.json"));
    let body = FailpointSentinel {
        name: name.to_string(),
        payload: payload.to_string(),
        consumed: false,
    };
    fs::write(
        &path,
        serde_json::to_vec_pretty(&body).expect("serialize failpoint sentinel"),
    )?;
    Ok(path)
}

pub fn list_sentinels(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    Ok(std::fs::read_dir(dir)?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("sentinel-") && name.ends_with(".json"))
        })
        .collect())
}

#[cfg(feature = "failpoint-tier")]
pub fn activate_sentinels(dir: &Path) -> Vec<(String, String)> {
    let mut activations = Vec::new();
    let Ok(entries) = list_sentinels(dir) else {
        return activations;
    };

    for path in entries {
        let Ok(raw) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(mut record) = serde_json::from_str::<FailpointSentinel>(&raw) else {
            continue;
        };
        activations.push((record.name.clone(), record.payload.clone()));
        record.consumed = true;
        if let Ok(serialized) = serde_json::to_vec_pretty(&record) {
            let _ = std::fs::write(&path, serialized);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(feature = "failpoint-tier")]
    {
        for (name, payload) in &activations {
            let point = name.to_string();
            let _ = fail::cfg(&point, payload);
        }
    }

    activations
}

#[cfg(not(feature = "failpoint-tier"))]
pub fn activate_sentinels(_dir: &Path) -> Vec<(String, String)> {
    Vec::new()
}
