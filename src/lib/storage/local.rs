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

use crate::utils;

use super::{
    base::{ImageLayerInfo, Result, Storage, UploadContainer},
    types::manifest::Manifest,
    Error, ManifestDetails, ManifestSummary, UpdateManifestDetails, UploadDetails, UploadStatus,
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
        ["uploads", name, uuid].iter().collect()
    }

    fn get_layer_file_path(&self, name: &String, digest: &String) -> PathBuf {
        ["layers", name, digest].iter().collect()
    }

    fn get_manifest_file_path(&self, name: &String, reference: &String) -> PathBuf {
        ["manifests", name, reference].iter().collect()
    }

    fn create_symlink(&self, target: &PathBuf, path: &PathBuf) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            if let Err(e) = symlink(target, path) {
                return Err(Box::new(e));
            }
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::symlink_file;
            if let Err(e) = symlink_file(target, path) {
                return Err(Box::new(e));
            }
        }

        Ok(())
    }

    fn is_sha256_digest(&self, digest: &String) -> bool {
        digest.starts_with("sha256:")
            && digest.len() == 64
            && digest.chars().all(|c| c.is_ascii_hexdigit())
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

    async fn get_layer(
        &self,
        name: String,
        digest: String,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>> {
        let path = self.get_layer_file_path(&name, &digest);

        if !path.is_file() {
            return Err(Error::from("layer not found"));
        }

        let file = File::open(&path)?;
        let reader = BufReader::new(file);

        let stream = futures::stream::iter(reader.bytes().map(|b| {
            b.map(|b| Bytes::from(vec![b]))
                .map_err(|e| Box::new(e) as Error)
        }));

        Ok(Box::pin(stream))
    }

    async fn create_upload_container(&self, name: String) -> Result<UploadContainer> {
        let uuid = Uuid::new_v4().to_string();
        let path = self.get_upload_file_path(&name, &uuid);

        let parent = path.parent().unwrap();
        if let Err(e) = fs::create_dir_all(parent) {
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

    async fn check_upload_container_validity(&self, name: String, uuid: String) -> Result<bool> {
        let path = self.get_upload_file_path(&name, &uuid);
        Ok(path.exists() && path.is_file())
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

    async fn get_manifest_summary(
        &self,
        name: String,
        reference: String,
    ) -> Result<ManifestSummary> {
        let mut path = self.get_manifest_file_path(&name, &reference);
        if path.is_symlink() && self.is_sha256_digest(&reference) {
            path = path.read_link()?;
        }

        if !path.is_file() {
            return Err(Error::from("Manifest not found"));
        }

        let manifest_content = fs::read_to_string(&path)?;

        let mut hasher = Sha256::new();
        hasher.update(&manifest_content);
        let hash = hex::encode(hasher.finalize());
        let digest = format!("sha256:{}", hash);

        let size = path.metadata()?.len();

        Ok(ManifestSummary { digest, size })
    }

    async fn get_manifest(&self, name: String, reference: String) -> Result<ManifestDetails> {
        let mut path = self.get_manifest_file_path(&name, &reference);
        if path.is_symlink() && self.is_sha256_digest(&reference) {
            path = path.read_link()?;
        }

        if !path.is_file() {
            return Err(Error::from("Manifest not found"));
        }

        let manifest_content = fs::read_to_string(&path)?;
        let manifest: Manifest = serde_json::from_str(&manifest_content)?;

        let mut hasher = Sha256::new();
        hasher.update(&manifest_content);
        let hash = hex::encode(hasher.finalize());
        let digest = format!("sha256:{}", hash);

        Ok(ManifestDetails { manifest, digest })
    }

    async fn update_manifest(
        &self,
        name: String,
        reference: String,
        manifest: Manifest,
    ) -> Result<UpdateManifestDetails> {
        let json = utils::to_json_normalized(&manifest)?;

        let mut path = self.get_manifest_file_path(&name, &reference);
        if path.is_symlink() && self.is_sha256_digest(&reference) {
            path = path.read_link()?;
        }

        let parent = path.parent().unwrap();
        fs::create_dir_all(parent)?;
        fs::write(&path, &json)?;

        let mut hasher = Sha256::new();
        hasher.update(json.as_bytes());
        let hash = hex::encode(hasher.finalize());
        let digest = format!("sha256:{}", hash);

        let symlink_path = parent.join(&digest);
        if symlink_path.exists() {
            fs::remove_file(&symlink_path)?;
        }

        self.create_symlink(&path, &symlink_path)?;

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
