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
    utils,
};

pub async fn get_manifest_info(
    Path((name, reference)): Path<(String, String)>,
    Extension(state): Extension<SharedState>,
) -> impl IntoResponse {
    match state
        .storage
        .get_manifest_summary(name.clone(), reference.clone())
        .await
    {
        Err(e) => {
            eprintln!("{}", e);
            RegistryError::new(StatusCode::NOT_FOUND, RegistryErrorCode::ManifestUnknown)
                .into_response()
        }
        Ok(_) => Response::builder()
            // .header("Docker-Content-Digest", &manifest_summary.digest)
            // .header("Content-Length", manifest_summary.size.to_string())
            .body(Body::empty())
            .unwrap()
            .into_response(),
    }
}

pub async fn get_manifest(
    Path((name, reference)): Path<(String, String)>,
    Extension(state): Extension<SharedState>,
) -> impl IntoResponse {
    let manifest_details_result = state
        .storage
        .get_manifest(name.clone(), reference.clone())
        .await;
    if let Err(e) = manifest_details_result {
        eprintln!("{}", e);
        return RegistryError::new(StatusCode::NOT_FOUND, RegistryErrorCode::ManifestUnknown)
            .into_response();
    }

    let manifest_details = manifest_details_result.unwrap();
    match utils::to_json_normalized(&manifest_details.manifest) {
        Ok(json) => Response::builder()
            .header("Docker-Content-Digest", &manifest_details.digest)
            .header("Content-Type", &manifest_details.manifest.media_type)
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
