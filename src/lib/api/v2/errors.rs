use std::collections::HashMap;

use axum::{
    response::{IntoResponse, Response},
    Json,
};
use hyper::StatusCode;
use lazy_static::lazy_static;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RegistryErrorCode {
    BlobUnknown,
    BlobUploadInvalid,
    BlobUploadUnknown,
    DigestInvalid,
    ManifestBlobUnknown,
    ManifestInvalid,
    ManifestUnknown,
    ManifestUnverified,
    NameInvalid,
    NameUnknown,
    PaginationNumberInvalid,
    RangeInvalid,
    SizeInvalid,
    TagInvalid,
    Unauthorized,
    Denied,
    Unsupported,
}

lazy_static! {
    static ref REGISTRY_ERROR_RAW_CODES: HashMap<RegistryErrorCode, &'static str> = {
        let mut m = HashMap::new();
        m.insert(RegistryErrorCode::BlobUnknown, "BLOB_UNKNOWN");
        m.insert(RegistryErrorCode::BlobUploadInvalid, "BLOB_UPLOAD_INVALID");
        m.insert(RegistryErrorCode::BlobUploadUnknown, "BLOB_UPLOAD_UNKNOWN");
        m.insert(RegistryErrorCode::DigestInvalid, "DIGEST_INVALID");
        m.insert(
            RegistryErrorCode::ManifestBlobUnknown,
            "MANIFEST_BLOB_UNKNOWN",
        );
        m.insert(RegistryErrorCode::ManifestInvalid, "MANIFEST_INVALID");
        m.insert(RegistryErrorCode::ManifestUnknown, "MANIFEST_UNKNOWN");
        m.insert(RegistryErrorCode::ManifestUnverified, "MANIFEST_UNVERIFIED");
        m.insert(RegistryErrorCode::NameInvalid, "NAME_INVALID");
        m.insert(RegistryErrorCode::NameUnknown, "NAME_UNKNOWN");
        m.insert(
            RegistryErrorCode::PaginationNumberInvalid,
            "PAGINATION_NUMBER_INVALID",
        );
        m.insert(RegistryErrorCode::RangeInvalid, "RANGE_INVALID");
        m.insert(RegistryErrorCode::SizeInvalid, "SIZE_INVALID");
        m.insert(RegistryErrorCode::TagInvalid, "TAG_INVALID");
        m.insert(RegistryErrorCode::Unauthorized, "UNAUTHORIZED");
        m.insert(RegistryErrorCode::Denied, "DENIED");
        m.insert(RegistryErrorCode::Unsupported, "UNSUPPORTED");
        m
    };
}

lazy_static! {
    static ref REGISTRY_ERROR_MESSAGES: HashMap<RegistryErrorCode, &'static str> = {
        let mut m = HashMap::new();
        m.insert(RegistryErrorCode::BlobUnknown, "blob unknown to registry");
        m.insert(RegistryErrorCode::BlobUploadInvalid, "blob upload invalid");
        m.insert(
            RegistryErrorCode::BlobUploadUnknown,
            "blob upload unknown to registry",
        );
        m.insert(
            RegistryErrorCode::DigestInvalid,
            "provided digest did not match uploaded content",
        );
        m.insert(
            RegistryErrorCode::ManifestBlobUnknown,
            "blob unknown to registry",
        );
        m.insert(RegistryErrorCode::ManifestInvalid, "manifest invalid");
        m.insert(RegistryErrorCode::ManifestUnknown, "manifest unknown");
        m.insert(
            RegistryErrorCode::ManifestUnverified,
            "manifest failed signature verification",
        );
        m.insert(RegistryErrorCode::NameInvalid, "invalid repository name");
        m.insert(
            RegistryErrorCode::NameUnknown,
            "repository name not known to registry",
        );
        m.insert(
            RegistryErrorCode::PaginationNumberInvalid,
            "invalid number of results requested",
        );
        m.insert(RegistryErrorCode::RangeInvalid, "invalid content range");
        m.insert(
            RegistryErrorCode::SizeInvalid,
            "provided length did not match content length",
        );
        m.insert(
            RegistryErrorCode::TagInvalid,
            "manifest tag did not match URI",
        );
        m.insert(RegistryErrorCode::Unauthorized, "authentication required");
        m.insert(
            RegistryErrorCode::Denied,
            "requested access to the resource is denied",
        );
        m.insert(
            RegistryErrorCode::Unsupported,
            "The operation is unsupported.",
        );
        m
    };
}

#[derive(Serialize)]
struct RegistryErrorResponse {
    errors: Vec<RegistryErrorResponseError>,
}

#[derive(Serialize)]
struct RegistryErrorResponseError {
    code: String,
    message: String,
}

pub struct RegistryError {
    status: StatusCode,
    code: RegistryErrorCode,
}

impl RegistryError {
    pub fn new(status: StatusCode, code: RegistryErrorCode) -> RegistryError {
        RegistryError { status, code }
    }
}

impl IntoResponse for RegistryError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(RegistryErrorResponse {
                errors: vec![RegistryErrorResponseError {
                    code: REGISTRY_ERROR_RAW_CODES[&self.code].to_string(),
                    message: REGISTRY_ERROR_MESSAGES[&self.code].to_string(),
                }],
            }),
        )
            .into_response()
    }
}
