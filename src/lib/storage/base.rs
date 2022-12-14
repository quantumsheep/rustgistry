use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use sync_wrapper::SyncWrapper;

use super::types::manifest::Manifest;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct ImageLayerInfo {
    pub size: u64,
}

#[derive(Clone, Debug)]
pub struct UploadContainer {
    pub uuid: String,
    pub state: String,
}

#[derive(Clone, Debug)]
pub struct UploadStatus {
    pub size: u64,
}

#[derive(Clone, Debug)]
pub struct UploadDetails {
    pub digest: String,
}

#[derive(Clone, Debug)]
pub struct ManifestSummary {
    pub digest: String,
    pub size: u64,
}

#[derive(Clone, Debug)]
pub struct ManifestDetails {
    pub manifest: Manifest,
    pub digest: String,
}

#[derive(Clone, Debug)]
pub struct UpdateManifestDetails {
    pub digest: String,
}

#[async_trait]
pub trait Storage: Sync + Send {
    async fn get_image_layer_info(
        &self,
        name: String,
        digest: String,
    ) -> Result<Option<ImageLayerInfo>>;

    async fn get_layer(
        &self,
        name: String,
        digest: String,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>>;

    async fn create_upload_container(&self, name: String) -> Result<UploadContainer>;

    async fn check_upload_container_validity(&self, name: String, uuid: String) -> Result<bool>;

    async fn write_upload_container(
        &self,
        name: String,
        uuid: String,
        mut stream: SyncWrapper<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>>,
        range: (u64, u64),
    ) -> Result<UploadStatus>;

    async fn close_upload_container(&self, name: String, uuid: String) -> Result<UploadDetails>;

    async fn get_manifest_summary(
        &self,
        name: String,
        reference: String,
    ) -> Result<ManifestSummary>;

    async fn get_manifest(&self, name: String, reference: String) -> Result<ManifestDetails>;

    async fn update_manifest(
        &self,
        name: String,
        reference: String,
        manifest: Manifest,
    ) -> Result<UpdateManifestDetails>;

    async fn delete_manifest(&self, name: String, reference: String) -> Result<()>;
}

pub fn is_sha256_digest(digest: &String) -> bool {
    digest.starts_with("sha256:")
        && digest.len() == 71
        && digest[7..].chars().all(|c| c.is_ascii_hexdigit())
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt;
    use rand::Rng;
    use sync_wrapper::SyncWrapper;

    use super::{is_sha256_digest, Result, Storage};

    pub async fn test_upload_layer(storage: Arc<dyn Storage>) -> Result<()> {
        let name = "test".to_string();

        let upload_container = storage.create_upload_container(name.clone()).await?;
        let uuid = upload_container.uuid;

        fn rand_bytes(size: usize) -> Bytes {
            let mut bytes = vec![0; size];
            rand::thread_rng().fill(&mut bytes[..]);
            Bytes::from(bytes)
        }

        let chunk_size = rand::thread_rng().gen_range(100..=1024);
        let chunk_count = rand::thread_rng().gen_range(5..=10);

        let stream = futures::stream::iter(0..chunk_count).map(move |_| Ok(rand_bytes(chunk_size)));

        let upload_status = storage
            .write_upload_container(
                name.clone(),
                uuid.clone(),
                SyncWrapper::new(Box::pin(stream)),
                (0, 0),
            )
            .await?;

        assert_eq!(upload_status.size, (chunk_size * chunk_count) as u64);

        let upload_details = storage
            .close_upload_container(name.clone(), uuid.clone())
            .await?;

        assert!(is_sha256_digest(&upload_details.digest));

        Ok(())
    }
}
