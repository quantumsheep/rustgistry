use axum::{
    extract::{BodyStream, Host, Path, Query},
    http::Uri,
    response::{IntoResponse, Response},
    Extension,
};
use futures::StreamExt;
use hyper::{Body, HeaderMap, StatusCode};
use serde::Deserialize;
use sync_wrapper::SyncWrapper;

use crate::api::v2::errors::{RegistryError, RegistryErrorCode};
use crate::{api::v2::state::SharedState, storage::Error};

pub async fn start_upload_process(
    uri: Uri,
    Host(hostname): Host,
    Path(name): Path<String>,
    Extension(state): Extension<SharedState>,
) -> impl IntoResponse {
    let upload_info_result = state.storage.create_upload_container(name.clone()).await;
    if let Err(e) = upload_info_result {
        eprintln!("{}", e);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let upload_info = upload_info_result.unwrap();

    Response::builder()
        .header("Docker-Upload-UUID", &upload_info.uuid)
        .header(
            "Location",
            format!(
                "{}://{}/v2/{}/blobs/uploads/{}?_state={}",
                uri.scheme_str().unwrap_or("http"),
                hostname,
                name,
                upload_info.uuid,
                upload_info.state,
            ),
        )
        .header("Range", "0-0")
        .status(StatusCode::ACCEPTED)
        .body(Body::empty())
        .unwrap()
        .into_response()
}

#[derive(Deserialize)]
pub struct MonolithicUploadQuery {
    pub _state: String,
    #[serde(default)]
    pub digest: Option<String>,
}

pub async fn receive_upload_monolithic(
    uri: Uri,
    Host(hostname): Host,
    Path((name, uuid)): Path<(String, String)>,
    query: Query<MonolithicUploadQuery>,
    headers: HeaderMap,
    Extension(state): Extension<SharedState>,
    mut body: BodyStream,
) -> impl IntoResponse {
    let validity_result = state
        .storage
        .check_upload_container_validity(name.clone(), uuid.clone())
        .await;

    match validity_result {
        Ok(false) => {
            return RegistryError::new(StatusCode::NOT_FOUND, RegistryErrorCode::BlobUploadInvalid)
                .into_response()
        }
        Err(e) => {
            eprintln!("{}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        _ => {}
    }

    let content_length = match headers.get("Content-Length") {
        Some(v) => match v.to_str() {
            Ok(v) => match v.parse::<usize>() {
                Ok(v) => v,
                Err(_) => {
                    return RegistryError::new(
                        StatusCode::BAD_REQUEST,
                        RegistryErrorCode::BlobUploadInvalid,
                    )
                    .into_response()
                }
            },
            Err(_) => 0,
        },
        None => 0,
    };

    if content_length > 0 {
        let buffer =
            futures::stream::poll_fn(move |cx| body.poll_next_unpin(cx)).map(|chunk| match chunk {
                Ok(chunk) => Ok(chunk),
                Err(e) => Err(Error::from(e)),
            });

        if let Err(e) = state
            .storage
            .write_upload_container(
                name.clone(),
                uuid.clone(),
                SyncWrapper::new(Box::pin(buffer)),
                (0, content_length as u64),
            )
            .await
        {
            eprintln!("{}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    match state
        .storage
        .close_upload_container(name.clone(), uuid.clone())
        .await
    {
        Ok(details) => {
            if let Some(digest) = &query.digest {
                if *digest != details.digest {
                    return RegistryError::new(
                        StatusCode::BAD_REQUEST,
                        RegistryErrorCode::DigestInvalid,
                    )
                    .into_response();
                }
            }

            Response::builder()
                .status(StatusCode::CREATED)
                .header("Docker-Content-Digest", &details.digest)
                .header(
                    "Location",
                    format!(
                        "{}://{}/v2/{}/blobs/{}",
                        uri.scheme_str().unwrap_or("http"),
                        hostname,
                        name,
                        details.digest,
                    ),
                )
                .body(Body::empty())
                .unwrap()
                .into_response()
        }
        Err(e) => {
            eprintln!("{}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct ChunkedUploadQuery {
    pub _state: String,
}

pub async fn receive_upload_chunked(
    Path((name, uuid)): Path<(String, String)>,
    _query: Query<ChunkedUploadQuery>,
    Extension(state): Extension<SharedState>,
    mut body: BodyStream,
) -> impl IntoResponse {
    let validity_result = state
        .storage
        .check_upload_container_validity(name.clone(), uuid.clone())
        .await;

    match validity_result {
        Ok(false) => {
            return RegistryError::new(StatusCode::NOT_FOUND, RegistryErrorCode::BlobUploadInvalid)
                .into_response()
        }
        Err(e) => {
            eprintln!("{}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        _ => {}
    }

    let buffer =
        futures::stream::poll_fn(move |cx| body.poll_next_unpin(cx)).map(|chunk| match chunk {
            Ok(chunk) => Ok(chunk),
            Err(e) => Err(Error::from(e)),
        });

    let status_result = state
        .storage
        .write_upload_container(name, uuid, SyncWrapper::new(Box::pin(buffer)), (1, 2))
        .await;

    if let Err(e) = status_result {
        eprintln!("{}", e);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let status = status_result.unwrap();

    let response = Response::builder()
        .status(StatusCode::ACCEPTED)
        .header("Range", format!("0-{}", status.size))
        .body(Body::empty())
        .unwrap();

    response.into_response()
}

pub async fn exists(
    Path((name, digest)): Path<(String, String)>,
    Extension(state): Extension<SharedState>,
) -> impl IntoResponse {
    let layer_info_result = state
        .storage
        .get_image_layer_info(name, digest.clone())
        .await;
    if let Err(e) = layer_info_result {
        eprintln!("{}", e);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let layer_info_option = layer_info_result.unwrap();

    match layer_info_option {
        Some(layer_info) => Response::builder()
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", layer_info.size.to_string())
            .header("Docker-Content-Digest", &digest)
            .header("Etag", format!("\"{}\"", digest))
            .header("Content-Type", "application/octet-stream")
            .body(Body::empty())
            .unwrap()
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub async fn get_layer(
    Path((name, digest)): Path<(String, String)>,
    Extension(state): Extension<SharedState>,
) -> impl IntoResponse {
    let layer_result = state.storage.get_layer(name, digest.clone()).await;
    if let Err(e) = layer_result {
        eprintln!("{}", e);
        return StatusCode::NOT_FOUND.into_response();
    }

    let layer_stream = layer_result.unwrap();

    Response::builder()
        .header("Accept-Ranges", "bytes")
        .header("Content-Length", "0")
        .header("Docker-Content-Digest", &digest)
        .header("Etag", format!("\"{}\"", digest))
        .header("Content-Type", "application/octet-stream")
        .body(Body::wrap_stream(layer_stream))
        .unwrap()
        .into_response()
}
