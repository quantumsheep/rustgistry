mod errors;
mod middlewares;
mod routes;
mod state;

use std::{
    error::Error,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use axum::{
    body, middleware,
    routing::{get, head, patch, post, put, IntoMakeService},
    Extension, Router, Server,
};
use hyper::{server::conn::AddrIncoming, Body};
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tower_http::ServiceBuilderExt;

use crate::storage::Storage;

use self::state::SharedState;

pub struct ApiV2 {
    addr: SocketAddr,
    storage: Arc<dyn Storage>,

    server: Option<Server<AddrIncoming, IntoMakeService<Router<Body>>>>,
}

impl ApiV2 {
    pub fn new(host: Ipv4Addr, port: u16, storage: Arc<dyn Storage>) -> ApiV2 {
        ApiV2 {
            addr: SocketAddr::from((host, port)),
            storage,
            server: None,
        }
    }

    pub async fn listen(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let app_state = SharedState::new(self.storage.clone());

        tracing_subscriber::fmt::init();

        let router = Router::new()
            .route("/v2", get(routes::version::get_version))
            .route(
                "/v2/:name/manifests/:reference",
                head(routes::manifests::get_manifest_info),
            )
            .route(
                "/v2/:name/manifests/:reference",
                get(routes::manifests::get_manifest),
            )
            .route(
                "/v2/:name/manifests/:reference",
                put(routes::manifests::put_manifest),
            )
            .route(
                "/v2/:name/blobs/uploads/",
                post(routes::blobs::start_upload_process),
            )
            .route(
                "/v2/:name/blobs/uploads/:uuid",
                put(routes::blobs::receive_upload_monolithic),
            )
            .route(
                "/v2/:name/blobs/uploads/:uuid",
                patch(routes::blobs::receive_upload_chunked),
            )
            .route("/v2/:name/blobs/:digest", head(routes::blobs::exists))
            .route("/v2/:name/blobs/:digest", get(routes::blobs::get_layer))
            .layer(Extension(app_state))
            .layer(
                ServiceBuilder::new()
                    .map_request_body(body::boxed)
                    .layer(middleware::from_fn(middlewares::version_header_middleware)),
            )
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::new().include_headers(true)),
            );

        let server = axum::Server::bind(&self.addr).serve(router.into_make_service());
        self.server = Some(server);

        self.server.as_mut().unwrap().await?;

        Ok(())
    }

    pub async fn graceful_shutdown(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(server) = self.server.take() {
            let graceful = server.with_graceful_shutdown(async {
                tokio::signal::ctrl_c()
                    .await
                    .expect("failed to install CTRL+C signal handler");
            });

            graceful.await?;
        }

        Err("Server not running".into())
    }
}
