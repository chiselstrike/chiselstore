use anyhow::Result;
use chiselstore::rpc::proto::rpc_server::RpcServer;
use chiselstore::{
    rpc::{RpcService, RpcTransport},
    StoreServer,
};

pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
use proto::{Consistency, Query};
use std::net::SocketAddr;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::oneshot;
use tonic::transport::Server;

pub struct ReplicaPeers {
    peers: Vec<u64>,
}

impl ReplicaPeers {
    pub fn new() -> Self {
        Self { peers: Vec::new() }
    }

    pub fn add_peer(&mut self, id: u64) {
        self.peers.push(id);
    }
}

/// Node authority (host and port) in the cluster.
fn node_authority(id: usize) -> (&'static str, u16) {
    let host = "127.0.0.1";
    let port = 50000 + (id as u16);
    (host, port)
}

/// Node RPC address in cluster.
fn node_rpc_addr(id: usize) -> String {
    let (host, port) = node_authority(id);
    format!("http://{}:{}", host, port)
}

pub struct SPReplica {
    halt_sender: tokio::sync::oneshot::Sender<()>,
    recv_sender: tokio::task::JoinHandle<Result<()>>,
    ble_sender: tokio::task::JoinHandle<Result<()>>,
    trans_sender: tokio::task::JoinHandle<Result<()>>,
}

impl SPReplica {
    pub fn new(id: u64) {
        let (halt_sender, halt_receiver) = oneshot::channel::<()>();
        let (host, port) = node_authority(id as usize);
        let rpc_listen_addr: SocketAddr = format!("{}:{}", host, port).parse().unwrap();
        let transport = RpcTransport::new(Box::new(node_rpc_addr));
    }
}
