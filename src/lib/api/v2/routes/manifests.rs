use axum::{
    extract::Path,
    response::{IntoResponse, Response},
    Extension, Json,
};
use hyper::{Body, StatusCode};
use serde::Serialize;

use crate::{
    api::v2::{
        errors::{RegistryError, RegistryErrorCode},
        state::SharedState,
    },
    storage::types::manifest::Manifest,
};

pub async fn get_manifest(
    Path((name, reference)): Path<(String, String)>,
    Extension(state): Extension<SharedState>,
) -> impl IntoResponse {
    let manifest_info_result = state
        .storage
        .get_manifest(name.clone(), reference.clone())
        .await;
    if let Err(e) = manifest_info_result {
        eprintln!("{}", e);
        return RegistryError::new(StatusCode::NOT_FOUND, RegistryErrorCode::ManifestUnknown)
            .into_response();
    }

    let manifest_info = manifest_info_result.unwrap();
    match serde_json::to_string(&manifest_info.manifest) {
        Ok(json) => Response::builder()
            .header("Docker-Content-Digest", &manifest_info.digest)
            .header("Content-Type", &manifest_info.manifest.media_type)
            .body(json)
            .unwrap()
            .into_response(),
        Err(e) => {
            eprintln!("{}", e);
            RegistryError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                RegistryErrorCode::ManifestInvalid,
            )
            .into_response()
        }
    }
}

#[derive(Serialize)]
struct PutManifestResponse {}

pub async fn put_manifest(
    Path((name, reference)): Path<(String, String)>,
    Extension(state): Extension<SharedState>,
    Json(manifest): Json<Manifest>,
) -> impl IntoResponse {
    let update_manifest_result = state
        .storage
        .update_manifest(name, reference, manifest)
        .await;

    match update_manifest_result {
        Ok(details) => Response::builder()
            .header("Docker-Content-Digest", &details.digest)
            .status(StatusCode::CREATED)
            .body(Body::empty())
            .unwrap()
            .into_response(),
        Err(e) => {
            eprintln!("{}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
