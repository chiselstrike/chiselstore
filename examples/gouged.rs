use anyhow::Result;
use chiselstore::rpc::proto::rpc_server::RpcServer;
use chiselstore::{
    rpc::{RpcService, RpcTransport},
    StoreServer,
};
use std::sync::Arc;
use structopt::StructOpt;
use tonic::transport::Server;

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

/// Node address in cluster.
fn node_addr(id: usize) -> String {
    let port = 50000 + id;
    format!("http://127.0.0.1:{}", port)
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    let port = 50000 + opt.id;
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();
    let transport = RpcTransport::new(Box::new(node_addr));
    let server = StoreServer::start(opt.id, opt.peers, transport)?;
    let server = Arc::new(server);
    let f = {
        let server = server.clone();
        tokio::task::spawn(async move {
            server.start_blocking();
        })
    };
    let rpc = RpcService::new(server);
    let g = tokio::task::spawn(async move {
        println!("RPC listening to {} ...", addr);
        let ret = Server::builder()
            .add_service(RpcServer::new(rpc))
            .serve(addr)
            .await;
        ret
    });
    let results = tokio::try_join!(f, g)?;
    results.1?;
    Ok(())
}
