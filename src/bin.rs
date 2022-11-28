use std::env;
use std::error::Error;
use std::net::Ipv4Addr;
use std::sync::Arc;

use clap::Parser;
use rustgistry::api::v2::ApiV2;
use rustgistry::storage::LocalStorage;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Host to listen on
    #[arg(long, default_value = "0.0.0.0")]
    host: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();

    let storage_type = env::var("STORAGE_TYPE").unwrap_or_else(|_| "local".to_string());

    let mut storage = None;

    if storage_type == "local" {
        let storage_path =
            env::var("STORAGE_PATH").unwrap_or_else(|_| "/var/lib/rustgistry".to_string());
        storage = Some(LocalStorage::new(storage_path));
    }

    if storage.is_none() {
        panic!("Invalid storage type");
    }

    let mut api = ApiV2::new(
        args.host.parse::<Ipv4Addr>()?,
        args.port,
        Arc::new(storage.unwrap()),
    );
    let server = api.listen();

    println!("Listening on http://{}:{}", args.host, args.port);

    server.await?;

    Ok(())
}
