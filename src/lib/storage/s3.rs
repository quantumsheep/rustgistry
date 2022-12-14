use std::{path::PathBuf, pin::Pin, time::SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    CopyObjectRequest, DeleteObjectRequest, GetObjectError, GetObjectRequest, HeadObjectError,
    HeadObjectRequest, PutObjectRequest, S3Client, StreamingBody, S3,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::utils;

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

    fn get_upload_file_path(&self, name: &String, uuid: &String) -> String {
        ["uploads", name, uuid]
            .iter()
            .collect::<PathBuf>()
            .to_str()
            .unwrap()
            .to_owned()
    }

    fn get_layer_file_path(&self, name: &String, digest: &String) -> String {
        ["layers", name, digest]
            .iter()
            .collect::<PathBuf>()
            .to_str()
            .unwrap()
            .to_owned()
    }

    fn get_manifest_file_path(&self, name: &String, reference: &String) -> String {
        ["manifests", name, reference]
            .iter()
            .collect::<PathBuf>()
            .to_str()
            .unwrap()
            .to_owned()
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
        let key = self.get_layer_file_path(&name, &digest);

        let result = self
            .client
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key: key.clone(),
                ..Default::default()
            })
            .await;
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
        let key = self.get_layer_file_path(&name, &digest);

        let result = self
            .client
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key: key.clone(),
                ..Default::default()
            })
            .await;
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

        let key = self.get_upload_file_path(&name, &uuid);

        match self
            .client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key: key.clone(),
                body: None,
                ..Default::default()
            })
            .await
        {
            Ok(_) => (),
            Err(e) => return Err(Box::new(e)),
        }

        let state = UploadState {
            name: name.clone(),
            uuid: uuid.clone(),
            created_at,
        };

        let state_json = serde_json::to_string(&state)?;
        Ok(UploadContainer {
            uuid,
            state: state_json,
        })
    }

    async fn check_upload_container_validity(&self, name: String, uuid: String) -> Result<bool> {
        let key = self.get_upload_file_path(&name, &uuid);

        match self
            .client
            .head_object(HeadObjectRequest {
                bucket: self.bucket.clone(),
                key: key.clone(),
                ..Default::default()
            })
            .await
        {
            Ok(_) => Ok(true),
            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => Ok(false),
            Err(e) => Err(Box::new(e)),
        }
    }

    async fn write_upload_container(
        &self,
        name: String,
        uuid: String,
        stream: Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
        _range: (u64, u64),
    ) -> Result<UploadStatus> {
        let key = self.get_upload_file_path(&name, &uuid);

        let tmp_file = tempfile::NamedTempFile::new()?;

        let byte_stream = stream.map(move |b| match b {
            Ok(b) => Ok(b),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
        });

        self.client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key: key.clone(),
                body: Some(StreamingBody::new(byte_stream)),
                ..Default::default()
            })
            .await?;
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

    async fn close_upload_container(&self, name: String, uuid: String) -> Result<UploadDetails> {
        let key = self.get_upload_file_path(&name, &uuid);

        let result = self
            .client
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key: key.clone(),
                ..Default::default()
            })
            .await?;

        let mut hasher = Sha256::new();

        let mut stream = result
            .body
            .ok_or_else(|| Error::from("Missing body in response"))?;
        while let Some(chunk) = stream.next().await {
            let bytes = chunk?;
            hasher.update(&bytes);
        }

        let hash = hex::encode(hasher.finalize());
        let digest = format!("sha256:{}", hash);

        let layer_key = self.get_layer_file_path(&name, &digest);

        self.client
            .copy_object(CopyObjectRequest {
                bucket: self.bucket.clone(),
                copy_source: format!("{}/{}", self.bucket, key),
                key: layer_key.clone(),
                ..Default::default()
            })
            .await?;

        self.client
            .delete_object(DeleteObjectRequest {
                bucket: self.bucket.clone(),
                key: key.clone(),
                ..Default::default()
            })
            .await?;

        Ok(UploadDetails { digest })
    }

    async fn get_manifest_summary(
        &self,
        name: String,
        reference: String,
    ) -> Result<ManifestSummary> {
        let key = self.get_manifest_file_path(&name, &reference);

        let result = self
            .client
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key: key.clone(),
                ..Default::default()
            })
            .await?;

        let mut stream = result
            .body
            .ok_or_else(|| Error::from("Missing body in response"))?;

        let mut manifest_content = String::new();
        while let Some(chunk) = stream.next().await {
            let bytes = chunk?;
            manifest_content.push_str(&String::from_utf8(bytes.to_vec())?);
        }

        let mut hasher = Sha256::new();
        hasher.update(&manifest_content);
        let hash = hex::encode(hasher.finalize());
        let digest = format!("sha256:{}", hash);

        let size = result.content_length.unwrap_or(0) as u64;

        Ok(ManifestSummary { digest, size })
    }

    async fn get_manifest(&self, name: String, reference: String) -> Result<ManifestDetails> {
        let key = self.get_manifest_file_path(&name, &reference);

        let result = self
            .client
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key: key.clone(),
                ..Default::default()
            })
            .await?;

        let mut stream = result
            .body
            .ok_or_else(|| Error::from("Missing body in response"))?;

        let mut manifest_content = String::new();
        while let Some(chunk) = stream.next().await {
            let bytes = chunk?;
            manifest_content.push_str(&String::from_utf8(bytes.to_vec())?);
        }

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

        let mut hasher = Sha256::new();
        hasher.update(&json.as_bytes());
        let hash = hex::encode(hasher.finalize());
        let digest = format!("sha256:{}", hash);

        let key = self.get_manifest_file_path(&name, &reference);

        // fs::write(&path, &json)?;
        self.client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key: key.clone(),
                body: Some(json.into_bytes().into()),
                ..Default::default()
            })
            .await?;

        self.client
            .copy_object(CopyObjectRequest {
                bucket: self.bucket.clone(),
                copy_source: format!("{}/{}", self.bucket, key),
                key: key.clone(),
                ..Default::default()
            })
            .await?;

        Ok(UpdateManifestDetails { digest })
    }

    async fn delete_manifest(&self, name: String, reference: String) -> Result<()> {
        let key = self.get_manifest_file_path(&name, &reference);

        self.client
            .delete_object(DeleteObjectRequest {
                bucket: self.bucket.clone(),
                key: key.clone(),
                ..Default::default()
            })
            .await?;

        Ok(())
    }
}
