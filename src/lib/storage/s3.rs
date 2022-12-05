use std::{pin::Pin, time::SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    GetObjectError, GetObjectRequest, HeadObjectError, HeadObjectRequest, PutObjectRequest,
    S3Client, StreamingBody, S3,
};
use serde::{Deserialize, Serialize};
use sync_wrapper::SyncWrapper;
use tokio::io::AsyncWriteExt;
use tokio_util::codec;
use uuid::Uuid;

use super::{
    base::{ImageLayerInfo, Result, Storage, UploadContainer},
    types::manifest::Manifest,
    Error, ManifestDetails, ManifestSummary, UpdateManifestDetails, UploadDetails, UploadStatus,
};

pub struct S3Storage {
    pub bucket: String,
    pub region: Region,
    client: S3Client,
}

impl S3Storage {
    pub fn new<S>(bucket: S, region: Region) -> S3Storage
    where
        S: AsRef<str>,
    {
        let client = S3Client::new(region.clone());

        S3Storage {
            bucket: bucket.as_ref().to_owned(),
            region,
            client,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct UploadState {
    name: String,
    uuid: String,
    created_at: u64,
}

#[async_trait]
impl Storage for S3Storage {
    async fn get_image_layer_info(
        &self,
        name: String,
        digest: String,
    ) -> Result<Option<ImageLayerInfo>> {
        let key = format!("layers/{}/{}", name, digest);

        let request = GetObjectRequest {
            bucket: self.bucket.clone(),
            key: key.clone(),
            ..Default::default()
        };

        let result = self.client.get_object(request).await;
        let result = match result {
            Ok(output) => output,
            Err(e) => {
                if let RusotoError::Service(GetObjectError::NoSuchKey(_)) = e {
                    return Ok(None);
                } else {
                    return Err(Error::from(e.to_string()));
                }
            }
        };

        Ok(Some(ImageLayerInfo {
            size: result.content_length.unwrap_or(0) as u64,
        }))
    }

    async fn get_layer(
        &self,
        name: String,
        digest: String,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>> {
        let key = format!("layers/{}/{}", name, digest);

        let request = GetObjectRequest {
            bucket: self.bucket.clone(),
            key: key.clone(),
            ..Default::default()
        };

        let result = self.client.get_object(request).await;
        let result = match result {
            Ok(output) => output,
            Err(RusotoError::Service(GetObjectError::NoSuchKey(_))) => {
                return Ok(Box::pin(futures::stream::empty()))
            }
            Err(e) => return Err(Box::new(e)),
        };

        let body = result
            .body
            .ok_or_else(|| Error::from("Missing body in response"))?;

        Ok(Box::pin(body.map(|b| match b {
            Ok(b) => Ok(b),
            Err(e) => Err(Error::from(format!("Failed to read data: {}", e))),
        })))
    }

    async fn create_upload_container(&self, name: String) -> Result<UploadContainer> {
        let uuid = Uuid::new_v4().to_string();
        let created_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let key = format!("uploads/{}/{}", name, uuid);

        let state = UploadState {
            name: name.clone(),
            uuid: uuid.clone(),
            created_at,
        };

        let request = PutObjectRequest {
            bucket: self.bucket.clone(),
            key: key.clone(),
            body: None,
            ..Default::default()
        };

        match self.client.put_object(request).await {
            Ok(_) => (),
            Err(e) => return Err(Box::new(e)),
        }

        let state_json = serde_json::to_string(&state)?;
        Ok(UploadContainer {
            uuid,
            state: state_json,
        })
    }

    async fn check_upload_container_validity(&self, name: String, uuid: String) -> Result<bool> {
        let key = format!("uploads/{}/{}", name, uuid);

        let request = HeadObjectRequest {
            bucket: self.bucket.clone(),
            key: key.clone(),
            ..Default::default()
        };

        match self.client.head_object(request).await {
            Ok(_) => Ok(true),
            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => Ok(false),
            Err(e) => Err(Box::new(e)),
        }
    }

    async fn write_upload_container(
        &self,
        name: String,
        uuid: String,
        mut stream: SyncWrapper<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>>,
        _range: (u64, u64),
    ) -> Result<UploadStatus> {
        let key = format!("uploads/{}/{}", name, uuid);

        let tmp_file = tempfile::NamedTempFile::new()?;

        let inner = stream.get_mut();
        let mut writer = tokio::fs::File::from_std(tmp_file.reopen()?);
        while let Some(bytes) = inner.next().await {
            let bytes = bytes?;
            writer.write_all(&bytes).await?;
        }

        let tokio_file = tokio::fs::File::from_std(tmp_file.reopen()?);
        let byte_stream =
            codec::FramedRead::new(tokio_file, codec::BytesCodec::new()).map(|r| match r {
                Ok(b) => Ok(b.freeze()),
                Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
            });

        let request = PutObjectRequest {
            bucket: self.bucket.clone(),
            key: key.clone(),
            body: Some(StreamingBody::new(byte_stream)),
            ..Default::default()
        };

        self.client.put_object(request).await?;
        tmp_file.close()?;

        let request = HeadObjectRequest {
            bucket: self.bucket.clone(),
            key: key.clone(),
            ..Default::default()
        };

        let result = self.client.head_object(request).await?;
        Ok(UploadStatus {
            size: result.content_length.unwrap_or(0) as u64,
        })
    }

    async fn close_upload_container(&self, _name: String, _uuidd: String) -> Result<UploadDetails> {
        todo!()
    }

    async fn get_manifest_summary(
        &self,
        _name: String,
        _reference: String,
    ) -> Result<ManifestSummary> {
        todo!()
    }

    async fn get_manifest(&self, _name: String, _reference: String) -> Result<ManifestDetails> {
        todo!()
    }

    async fn update_manifest(
        &self,
        _name: String,
        _reference: String,
        _manifest: Manifest,
    ) -> Result<UpdateManifestDetails> {
        todo!()
    }

    async fn delete_manifest(&self, _name: String, _reference: String) -> Result<()> {
        todo!()
    }
}
