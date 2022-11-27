use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use sync_wrapper::SyncWrapper;

use super::types::manifest::Manifest;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub struct ImageLayerInfo {
    pub size: u64,
}

pub struct UploadContainer {
    pub uuid: String,
    pub state: String,
}

pub struct UploadStatus {
    pub size: u64,
}

pub struct UploadDetails {
    pub digest: String,
}

pub struct ManifestInfo {
    pub manifest: Manifest,
    pub digest: String,
}

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

    async fn create_upload_container(&self, name: String) -> Result<UploadContainer>;

    async fn check_upload_container_validity(&self, name: String, uuid: String) -> bool;

    async fn write_upload_container(
        &self,
        name: String,
        uuid: String,
        mut stream: SyncWrapper<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>>,
        range: (u64, u64),
    ) -> Result<UploadStatus>;

    async fn close_upload_container(&self, name: String, uuid: String) -> Result<UploadDetails>;

    async fn get_manifest(&self, name: String, reference: String) -> Result<ManifestInfo>;

    async fn update_manifest(
        &self,
        name: String,
        reference: String,
        manifest: Manifest,
    ) -> Result<UpdateManifestDetails>;

    async fn delete_manifest(&self, name: String, reference: String) -> Result<()>;
}
