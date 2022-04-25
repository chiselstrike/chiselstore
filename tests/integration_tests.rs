use chiselstore::rpc::proto::rpc_server::RpcServer;
use chiselstore::{
    rpc::{RpcService, RpcTransport},
    StoreServer,
};
use futures_util::FutureExt;
use std::sync::Arc;

pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
use proto::{Consistency, Query};
use std::net::SocketAddr;
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
pub struct SPReplica {
    replica_id: u64,
    server: Arc<StoreServer<RpcTransport>>,
    halt_sender: oneshot::Sender<()>,
    store_server_msg_event_handler: tokio::task::JoinHandle<()>,
    store_server_ble_handler: tokio::task::JoinHandle<()>,
    rpc_handler: tokio::task::JoinHandle<()>,
    rpc_tx: oneshot::Sender<()>,
}

pub fn make_cluster(nr: u64) -> Vec<SPReplica> {
    let mut cluster = Vec::new();
    let cluster_ids: Vec<u64> = (1..(nr + 1)).collect();

    for i in 1..(nr + 1) {
        let peers: Vec<u64> = cluster_ids
            .clone()
            .into_iter()
            .filter(|peer_id| (*peer_id != i))
            .collect();
        assert_eq!(peers.len(), (nr - 1) as usize);

        let sp_replica = SPReplica::new(i as u64, peers);
        cluster.push(sp_replica);
    }

    cluster
}

pub async fn execute_query(replica_id: u64, stmt: String, consistency: Consistency) -> Vec<String> {
    let addr = format!("http://127.0.0.1:5000{}", replica_id);
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

impl SPReplica {
    pub fn new(replica_id: u64, peers: Vec<u64>) -> Self {
        let (halt_sender, halt_receiver) = oneshot::channel();
        let (host, port) = node_authority(replica_id as usize);
        let rpc_listen_addr: SocketAddr = format!("{}:{}", host, port).parse().unwrap();
        let transport = RpcTransport::new(Box::new(node_rpc_addr));
        let server = StoreServer::start(replica_id, peers, transport).unwrap();
        let server = Arc::new(server);

        let store_server_msg_event = server.clone();
        let store_server_msg_event_handler = tokio::task::spawn(async move {
            store_server_msg_event.start_msg_event_loop();
        });

        let store_server_ble = server.clone();
        let store_server_ble_handler = tokio::task::spawn(async move {
            store_server_ble.start_ble_event_loop();
        });

        let rpc = RpcService::new(server.clone());
        let (rpc_tx, rpc_rx) = oneshot::channel::<()>();
        let rpc_handler = tokio::task::spawn(async move {
            let ret = Server::builder()
                .add_service(RpcServer::new(rpc))
                .serve_with_shutdown(rpc_listen_addr, rpc_rx.map(drop))
                .await;
            ret.unwrap()
        });

        let server_to_halt = server.clone();
        tokio::task::spawn(async move {
            halt_receiver.await.unwrap();
            server_to_halt.halt(true);
        });

        Self {
            replica_id,
            server,
            halt_sender,
            store_server_msg_event_handler,
            store_server_ble_handler,
            rpc_handler,
            rpc_tx,
        }
    }

    pub fn replica_is_leader(&mut self) -> bool {
        let server = self.server.clone();
        let leader = server.get_cluster_leader();
        leader == self.replica_id
    }

    pub async fn halt_replica(self) {
        self.halt_sender.send(()).unwrap();
        self.store_server_msg_event_handler.await.unwrap();
        self.store_server_ble_handler.await.unwrap();
        self.rpc_tx.send(()).unwrap();
        self.rpc_handler.await.unwrap();
    }

    pub fn get_replica_id(&self) -> u64 {
        self.replica_id
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_database_connection() {
    let cluster = make_cluster(2);

    tokio::task::spawn(async {
        let response = execute_query(1, String::from("SELECT 1"), Consistency::RelaxedReads).await;
        assert_eq!(response.len(), 1);
        assert_eq!(response[0], "1");
    })
    .await
    .unwrap();

    for c in cluster {
        c.halt_replica().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_consistency_relaxed() {
    let cluster = make_cluster(3);

    let replica_one = tokio::task::spawn(async {
        println!("---- CREATE TABLE ----");
        execute_query(
            1,
            String::from("CREATE TABLE IF NOT EXISTS test_consistency (i integer PRIMARY KEY);"),
            Consistency::RelaxedReads,
        )
        .await;

        println!("---- INSERT ----");
        execute_query(
            1,
            String::from("INSERT INTO test_consistency VALUES(50);"),
            Consistency::RelaxedReads,
        )
        .await;
    });

    replica_one.await.unwrap();

    let res_one = execute_query(
        1,
        String::from("SELECT i FROM test_consistency;"),
        Consistency::RelaxedReads,
    )
    .await;

    println!("res_one -> {:?}", res_one);
    let res_two = execute_query(
        2,
        String::from("SELECT i FROM test_consistency;"),
        Consistency::RelaxedReads,
    )
    .await;

    println!("res_two -> {:?}", res_two);
    let res_three = execute_query(
        3,
        String::from("SELECT i FROM test_consistency;"),
        Consistency::RelaxedReads,
    )
    .await;

    println!("res_three -> {:?}", res_three);

    assert_eq!(res_one.len(), 1);
    assert_eq!(res_two.len(), 1);
    assert_eq!(res_three.len(), 1);
    assert_eq!(res_one[0], res_two[0]);
    assert_eq!(res_one[0], res_three[0]);

    for c in cluster {
        println!("{} -> halting", c.get_replica_id());
        c.halt_replica().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_consistency_strong() {
    let cluster = make_cluster(3);
    let replica_one = tokio::task::spawn(async {
        execute_query(
            1,
            String::from("CREATE TABLE IF NOT EXISTS test_consistency (i integer PRIMARY KEY)"),
            Consistency::Strong,
        )
        .await;

        execute_query(
            1,
            String::from("INSERT INTO test_consistency VALUES(50)"),
            Consistency::Strong,
        )
        .await;
    });

    replica_one.await.unwrap();

    let res_one = execute_query(
        1,
        String::from("SELECT i FROM test_consistency"),
        Consistency::RelaxedReads,
    )
    .await;

    let res_two = execute_query(
        2,
        String::from("SELECT i FROM test_consistency"),
        Consistency::RelaxedReads,
    )
    .await;

    let res_three = execute_query(
        3,
        String::from("SELECT i FROM test_consistency"),
        Consistency::RelaxedReads,
    )
    .await;

    assert_eq!(res_one.len(), 1);
    assert_eq!(res_two.len(), 1);
    assert_eq!(res_three.len(), 1);
    assert_eq!(res_one[0], res_two[0]);
    assert_eq!(res_one[0], res_three[0]);

    for c in cluster {
        c.halt_replica().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replicas_read_and_write_and_shutdown_one_replica() {
    let mut cluster = make_cluster(3);

    let query_handler = tokio::task::spawn(async {
        execute_query(
            1,
            String::from("CREATE TABLE IF NOT EXISTS test_consistency (i integer PRIMARY KEY)"),
            Consistency::Strong,
        )
        .await;

        execute_query(
            1,
            String::from("INSERT INTO test_consistency VALUES(50)"),
            Consistency::Strong,
        )
        .await;
    });

    query_handler.await.unwrap();

    let mut idx = 0;
    for i in 0..cluster.len() {
        if !cluster[i as usize].replica_is_leader() {
            idx = i;
        }
    }

    let follower = cluster.remove(idx);
    follower.halt_replica().await;

    let replica_one = cluster.pop().unwrap();
    let replica_two = cluster.pop().unwrap();

    let replica_one_id = replica_one.get_replica_id();
    let replica_two_id = replica_two.get_replica_id();

    let res_one = execute_query(
        replica_one_id,
        String::from("SELECT i FROM test_consistency"),
        Consistency::RelaxedReads,
    )
    .await;

    let res_two = execute_query(
        replica_two_id,
        String::from("SELECT i FROM test_consistency"),
        Consistency::RelaxedReads,
    )
    .await;

    assert_eq!(res_one.len(), 1);
    assert_eq!(res_two.len(), 1);
    assert_eq!(res_one[0], res_two[0]);

    replica_one.halt_replica().await;
    replica_two.halt_replica().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replicas_read_and_write_and_shutdown_leader() {
    let mut cluster = make_cluster(3);

    let query_handler = tokio::task::spawn(async {
        execute_query(
            1,
            String::from("CREATE TABLE IF NOT EXISTS test_consistency (i integer PRIMARY KEY)"),
            Consistency::RelaxedReads,
        )
        .await;

        execute_query(
            1,
            String::from("INSERT INTO test_consistency VALUES(50)"),
            Consistency::RelaxedReads,
        )
        .await;
    });

    query_handler.await.unwrap();

    let mut idx = 0;
    for i in 0..cluster.len() {
        if cluster[i as usize].replica_is_leader() {
            idx = i;
        }
    }

    let follower = cluster.remove(idx);
    follower.halt_replica().await;

    let replica_one = cluster.pop().unwrap();
    let replica_two = cluster.pop().unwrap();

    let replica_one_id = replica_one.get_replica_id();
    let replica_two_id = replica_two.get_replica_id();

    let res_one = execute_query(
        replica_one_id,
        String::from("SELECT i FROM test_consistency"),
        Consistency::RelaxedReads,
    )
    .await;

    let res_two = execute_query(
        replica_two_id,
        String::from("SELECT i FROM test_consistency"),
        Consistency::RelaxedReads,
    )
    .await;

    assert_eq!(res_one.len(), 1);
    assert_eq!(res_two.len(), 1);
    assert_eq!(res_one[0], res_two[0]);

    replica_one.halt_replica().await;
    replica_two.halt_replica().await;
}
