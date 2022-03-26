use anyhow::Result;
use chiselstore::rpc::proto::rpc_server::RpcServer;
use chiselstore::rpc::proto::QueryResults;
use chiselstore::server;
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
use tokio::sync::oneshot;
use tonic::transport::Server;

fn node_authority(id: usize) -> (&'static str, u16) {
    let host = "127.0.0.1";
    let port = 50000 + (id as u16);
    (host, port)
}

fn node_rpc_addr(id: usize) -> String {
    let (host, port) = node_authority(id);
    format!("http://{}:{}", host, port)
}

#[derive(Clone)]
pub struct SPCluster {
    cluster: Vec<SPReplica>,
    // cluster_ids: Vec<u32>,
}

impl SPCluster {
    pub fn new(nr: u32) -> Self {
        let mut cluster = Vec::new();
        let cluster_ids: Vec<u32> = (1..nr).collect();

        for i in 0..nr {
            let mut peers = Vec::new();
            cluster_ids.iter().for_each(|id| {
                if (i + 1) != *id {
                    peers.push(i as u64);
                }
            });
            let sp_replica = SPReplica::new((i + 1) as u64, peers);
            cluster.push(sp_replica);
        }

        Self { cluster }
    }

    pub fn get_replica_with_id(&self, id: u64) -> &SPReplica {
        &self.cluster[(id - 1) as usize]
    }

    pub async fn halt_replica_with_id(&mut self, id: u64) {
        self.cluster.remove((id - 1) as usize).halt_replica().await;
    }

    pub async fn halt_all_replicas(&mut self) {
        while let Some(rep) = self.cluster.pop() {
            self.halt_replica_with_id(rep.replica_id).await;
        }
    }
}

pub struct SPReplica {
    replica_id: u64,
    server: Arc<StoreServer<RpcTransport>>,
    halt_sender: tokio::sync::oneshot::Sender<()>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl SPReplica {
    pub fn new(replica_id: u64, peers: Vec<u64>) -> Self {
        let (halt_sender, halt_receiver) = oneshot::channel::<()>();
        let (host, port) = node_authority(replica_id as usize);
        let rpc_listen_addr: SocketAddr = format!("{}:{}", host, port).parse().unwrap();
        let transport = RpcTransport::new(Box::new(node_rpc_addr));
        let server = StoreServer::start(replica_id, peers, transport).unwrap();
        let server = Arc::new(server);
        let mut handles = Vec::new();

        let store_server = server.clone();
        handles.push(tokio::task::spawn(async move {
            println!("spawning s");
            store_server.run();
            println!("s finished");
        }));

        let rpc = RpcService::new(server.clone());
        handles.push(tokio::task::spawn(async move {
            println!("RPC listening to {} ...", rpc_listen_addr);
            let ret = Server::builder()
                .add_service(RpcServer::new(rpc))
                .serve(rpc_listen_addr)
                .await;
            ret.unwrap()
        }));

        let server_to_halt = server.clone();
        tokio::task::spawn(async move {
            halt_receiver.await.unwrap();
            server_to_halt.run();
        });

        Self {
            replica_id,
            server,
            halt_sender,
            handles,
        }
    }

    pub fn replica_is_leader(&self) -> bool {
        let leader = self.server.get_leader();
        leader == self.replica_id
    }

    pub async fn halt_replica(self) {
        self.halt_sender.send(()).unwrap();
        for h in self.handles {
            h.await.unwrap();
        }
    }

    pub async fn execute_query(&self, stmt: String, consistency: Consistency) -> Vec<String> {
        let addr = format!("http://127.0.0.1:5000{}", self.replica_id);
        let mut client = RpcClient::connect(addr).await.unwrap();
        let query = tonic::Request::new(Query {
            sql: stmt,
            consistency: consistency as i32,
        });
        let response = client.execute(query).await.unwrap();
        let response = response.into_inner();
        let mut rows = Vec::new();
        for res in response.rows {
            rows.extend(res.values);
        }
        rows
    }
}
