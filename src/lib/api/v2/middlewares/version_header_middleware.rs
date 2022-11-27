use axum::{
    body::BoxBody,
    http::HeaderValue,
    middleware::Next,
    response::{IntoResponse, Response},
};
use hyper::Request;

pub async fn version_header_middleware(
    request: Request<BoxBody>,
    next: Next<BoxBody>,
) -> Result<impl IntoResponse, Response> {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        "Docker-Distribution-Api-Version",
        HeaderValue::from_static("rustgistry/2"),
    );

    Ok(response)
}
