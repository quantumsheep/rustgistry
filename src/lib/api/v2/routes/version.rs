use axum::{response::IntoResponse, Json};
use hyper::StatusCode;
use serde::Serialize;

#[derive(Serialize)]
struct GetVersionResponse {}

pub async fn get_version() -> impl IntoResponse {
    (StatusCode::OK, Json(GetVersionResponse {}))
}
