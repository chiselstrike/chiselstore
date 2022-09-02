use anyhow::Result;
use chiselstore::{Config, Database};
use std::sync::Arc;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "gouged")]
struct Opt {
    /// The ID of this server.
    #[structopt(short, long)]
    id: usize,
    /// The IDs of peers.
    #[structopt(short, long, required = false)]
    peers: Vec<usize>,
}

/// Node authority (host and port) in the cluster.
fn node_authority(id: usize) -> (&'static str, u16) {
    let host = "127.0.0.1";
    let port = 50000 + (id as u16);
    (host, port)
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();
    let opt = Opt::from_args();
    let config = Config {
        id: opt.id,
        peers: opt.peers,
        node_addr: Arc::new(node_authority),
    };
    let db = Database::new(config);
    db.run().await?;
    Ok(())
}
