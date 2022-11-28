use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    #[serde(rename = "schemaVersion")]
    pub schema_version: u32,

    #[serde(rename = "mediaType")]
    pub media_type: String,

    pub config: ManifestConfig,

    #[serde(default)]
    pub manifests: Option<Vec<ManifestEntry>>,

    #[serde(default)]
    pub layers: Option<Vec<LayerEntry>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestConfig {
    #[serde(rename = "mediaType")]
    pub media_type: String,

    pub size: u64,

    pub digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    #[serde(rename = "mediaType")]
    pub media_type: String,

    pub size: u32,

    pub digest: String,

    #[serde(default)]
    pub platform: Option<Platform>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerEntry {
    #[serde(rename = "mediaType")]
    pub media_type: String,

    pub size: u32,

    pub digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Platform {
    pub architecture: String,
    pub os: String,
    #[serde(default)]
    pub features: Option<Vec<String>>,
}
