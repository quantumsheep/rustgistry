use std::error::Error;
use std::net::Ipv4Addr;

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

    let storage = LocalStorage::new("/var/lib/rustgistry");
    let mut api = ApiV2::new(args.host.parse::<Ipv4Addr>()?, args.port, storage);
    let server = api.listen();

    println!("Listening on http://{}:{}", args.host, args.port);

    server.await?;

    Ok(())
}
