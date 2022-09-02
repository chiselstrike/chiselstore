//! ChiselStore database API.

use crate::rpc::proto::rpc_server::RpcServer;
use crate::{
    rpc::{NodeAddrFn, RpcService, RpcTransport},
    StoreServer,
};
use anyhow::Result;
use derivative::Derivative;
use std::sync::Arc;
use tonic::transport::Server;

/// ChiselStore database configuration.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Config {
    /// The ID of this ChiselStore node.
    pub id: usize,
    /// The peers of in the ChiselStore cluster.
    pub peers: Vec<usize>,
    /// Node address lookup function.
    #[derivative(Debug = "ignore")]
    pub node_addr: Arc<NodeAddrFn>,
}

/// ChiselStore database API.
#[derive(Debug)]
pub struct Database {
    /// Configuration of this ChiselStore database.
    config: Config,
}

impl Database {
    /// Creates a new `Database` object.
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Runs the database main loop.
    pub async fn run(&self) -> Result<()> {
        let (host, port) = (*self.config.node_addr)(self.config.id);
        let rpc_listen_addr = format!("{}:{}", host, port).parse().unwrap();
        let transport = RpcTransport::new(self.config.node_addr.clone());
        let server = StoreServer::start(self.config.id, self.config.peers.clone(), transport)?;
        let server = Arc::new(server);
        let f = {
            let server = server.clone();
            tokio::task::spawn_blocking(move || {
                server.run();
            })
        };
        let rpc = RpcService::new(server);
        let g = tokio::task::spawn(async move {
            println!("RPC listening to {} ...", rpc_listen_addr);
            let ret = Server::builder()
                .add_service(RpcServer::new(rpc))
                .serve(rpc_listen_addr)
                .await;
            ret
        });
        let results = tokio::try_join!(f, g)?;
        results.1?;
        Ok(())
    }
}
