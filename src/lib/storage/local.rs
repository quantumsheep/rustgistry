use std::{ffi::OsStr, fs, path::PathBuf, pin::Pin, time::SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};
use tokio_util::codec::{BytesCodec, FramedRead};
use uuid::Uuid;

use crate::utils;

use super::{
    base::{ImageLayerInfo, Result, Storage, UploadContainer},
    is_sha256_digest,
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

        let stream = File::open(&path).await.map(|file| {
            FramedRead::new(file, BytesCodec::new()).map(|bytes| match bytes {
                Ok(bytes) => Ok(bytes.freeze()),
                Err(e) => Err(Error::from(format!("Failed to read layer file: {}", e))),
            })
        })?;

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
        mut stream: Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
        _range: (u64, u64),
    ) -> Result<UploadStatus> {
        let path = self.get_upload_file_path(&name, &uuid);
        let mut file = OpenOptions::new().append(true).open(path).await?;

        while let Some(bytes) = stream.next().await {
            file.write_all(&bytes?).await?;
        }

        file.flush().await?;

        let metadata = file.metadata().await?;
        Ok(UploadStatus {
            size: metadata.len(),
        })
    }

    async fn close_upload_container(&self, name: String, uuid: String) -> Result<UploadDetails> {
        let path = self.get_upload_file_path(&name, &uuid);

        let mut hasher = Sha256::new();

        File::open(&path)
            .await
            .map(|file| FramedRead::new(file, BytesCodec::new()))?
            .for_each(|bytes| {
                if let Ok(values) = bytes {
                    hasher.update(&values);
                }

                std::future::ready(())
            })
            .await;

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
        if path.is_symlink() && is_sha256_digest(&reference) {
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
        if path.is_symlink() && is_sha256_digest(&reference) {
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
        if path.is_symlink() && is_sha256_digest(&reference) {
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

#[tokio::test]
async fn test_upload_layer() -> Result<()> {
    use std::sync::Arc;

    let temp_dir = tempfile::tempdir()?;
    let temp_dir_path = temp_dir.path();
    let storage = Arc::new(LocalStorage::new(temp_dir_path));

    super::tests::test_upload_layer(storage).await
}
