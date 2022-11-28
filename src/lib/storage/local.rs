use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    path::PathBuf,
    pin::Pin,
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sync_wrapper::SyncWrapper;
use uuid::Uuid;

use super::{
    base::{ImageLayerInfo, Result, Storage, UploadContainer},
    types::manifest::Manifest,
    Error, ManifestInfo, UpdateManifestDetails, UploadDetails, UploadStatus,
};

pub struct LocalStorage {
    pub path: PathBuf,
}

impl LocalStorage {
    pub fn new<S>(path: S) -> LocalStorage
    where
        S: AsRef<OsStr>,
    {
        LocalStorage {
            path: PathBuf::from(path.as_ref()),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct UploadState {
    name: String,
    uuid: String,
    created_at: u64,
}

impl LocalStorage {
    fn get_upload_file_path(&self, name: &String, uuid: &String) -> PathBuf {
        let mut path = self.path.clone();
        path.push("uploads");
        path.push(name);
        path.push(uuid);

        path
    }

    fn get_layer_file_path(&self, name: &String, digest: &String) -> PathBuf {
        let mut path = self.path.clone();
        path.push("layers");
        path.push(name);
        path.push(digest);

        path
    }

    fn get_manifest_file_path(&self, name: &String, reference: &String) -> PathBuf {
        let mut path = self.path.clone();
        path.push("manifests");
        path.push(name);
        path.push(reference);

        path
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn get_image_layer_info(
        &self,
        name: String,
        digest: String,
    ) -> Result<Option<ImageLayerInfo>> {
        let path = self.get_layer_file_path(&name, &digest);

        if !path.is_file() {
            return Ok(None);
        }

        let metadata = path.metadata()?;

        Ok(Some(ImageLayerInfo {
            size: metadata.len(),
        }))
    }

    async fn create_upload_container(&self, name: String) -> Result<UploadContainer> {
        let uuid = Uuid::new_v4().to_string();
        let path = self.get_upload_file_path(&name, &uuid);

        let parent = path.parent().unwrap();
        if let Err(e) = fs::create_dir_all(&parent) {
            return Err(Error::from(format!(
                "Failed to create upload container directory '{}': {}",
                parent.display(),
                e,
            )));
        }

        if let Err(e) = fs::write(&path, "") {
            return Err(Error::from(format!(
                "Failed to create upload container file '{}': {}",
                path.display(),
                e,
            )));
        }

        let state = UploadState {
            name,
            uuid: uuid.clone(),
            created_at: SystemTime::now().elapsed().unwrap_or_default().as_secs(),
        };

        match serde_json::to_string(&state) {
            Ok(state_json) => Ok(UploadContainer {
                uuid,
                state: base64::encode(state_json),
            }),
            Err(e) => Err(Error::from(format!(
                "Failed to serialize upload container state: {}",
                e
            ))),
        }
    }

    async fn check_upload_container_validity(&self, name: String, uuid: String) -> bool {
        let path = self.get_upload_file_path(&name, &uuid);
        path.exists() && path.is_file()
    }

    async fn write_upload_container(
        &self,
        name: String,
        uuid: String,
        mut stream: SyncWrapper<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>>,
        _range: (u64, u64),
    ) -> Result<UploadStatus> {
        let path = self.get_upload_file_path(&name, &uuid);
        let mut file = OpenOptions::new().append(true).open(path)?;

        let inner = stream.get_mut();

        while let Some(bytes) = inner.next().await {
            match bytes {
                Ok(bytes) => {
                    file.write_all(&bytes)?;
                }
                Err(e) => {
                    return Err(Error::from(e.to_string()));
                }
            }
        }

        let metadata = file.metadata()?;
        Ok(UploadStatus {
            size: metadata.len(),
        })
    }

    async fn close_upload_container(&self, name: String, uuid: String) -> Result<UploadDetails> {
        let path = self.get_upload_file_path(&name, &uuid);
        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);

        let mut hasher = Sha256::new();
        let mut buffer = [0; 1024];

        loop {
            let n = reader.read(&mut buffer)?;
            if n == 0 {
                break;
            }

            hasher.update(&buffer[..n]);
        }

        let hash = hex::encode(hasher.finalize());
        let digest = format!("sha256:{}", hash);

        let layer_path = self.get_layer_file_path(&name, &digest);
        fs::create_dir_all(layer_path.parent().unwrap())?;

        fs::rename(path, layer_path)?;

        Ok(UploadDetails { digest })
    }

    async fn get_manifest(&self, name: String, reference: String) -> Result<ManifestInfo> {
        let path = self.get_manifest_file_path(&name, &reference);

        if !path.is_file() {
            return Err(Error::from("Manifest not found"));
        }

        let manifest_content = fs::read_to_string(path)?;
        let manifest: Manifest = serde_json::from_str(&manifest_content)?;

        let mut hasher = Sha256::new();
        hasher.update(&manifest_content);
        let hash = hex::encode(hasher.finalize());
        let digest = format!("sha256:{}", hash);

        Ok(ManifestInfo { manifest, digest })
    }

    async fn update_manifest(
        &self,
        name: String,
        reference: String,
        manifest: Manifest,
    ) -> Result<UpdateManifestDetails> {
        let path = self.get_manifest_file_path(&name, &reference);

        fs::create_dir_all(path.parent().unwrap())?;
        let mut file = File::create(path)?;

        let json = serde_json::to_string(&manifest)?;
        file.write_all(json.as_bytes())?;

        let mut hasher = Sha256::new();
        hasher.update(json.as_bytes());

        let hash = hex::encode(hasher.finalize());
        let digest = format!("sha256:{}", hash);

        Ok(UpdateManifestDetails { digest })
    }

    async fn delete_manifest(&self, name: String, reference: String) -> Result<()> {
        let path = self.get_manifest_file_path(&name, &reference);

        if !path.is_file() {
            return Err(Error::from("Manifest not found"));
        }

        fs::remove_file(path)?;

        Ok(())
    }
}
