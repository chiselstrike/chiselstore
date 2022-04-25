//! ChiselStore RPC module.

use crate::rpc::proto::rpc_server::Rpc;
use crate::{Consistency, SequencePaxosStoreTransport, StoreCommand, StoreServer};
use async_mutex::Mutex;
use async_trait::async_trait;
use crossbeam::queue::ArrayQueue;
use derivative::Derivative;
use omnipaxos_core::{ballot_leader_election as ble, messages, storage, util};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;

#[derive(Debug)]
struct ConnectionPool {
    connections: ArrayQueue<RpcClient<tonic::transport::Channel>>,
}

struct Connection {
    conn: RpcClient<tonic::transport::Channel>,
    pool: Arc<ConnectionPool>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.pool.replenish(self.conn.clone())
    }
}

impl ConnectionPool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: ArrayQueue::new(16),
        })
    }

    async fn connection<S: ToString>(&self, addr: S) -> RpcClient<tonic::transport::Channel> {
        let addr = addr.to_string();
        match self.connections.pop() {
            Some(x) => x,
            None => RpcClient::connect(addr).await.unwrap(),
        }
    }

    fn replenish(&self, conn: RpcClient<tonic::transport::Channel>) {
        let _ = self.connections.push(conn);
    }
}

#[derive(Debug, Clone)]
struct Connections(Arc<Mutex<HashMap<String, Arc<ConnectionPool>>>>);

impl Connections {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    async fn connection<S: ToString>(&self, addr: S) -> Connection {
        let mut conns = self.0.lock().await;
        let addr = addr.to_string();
        let pool = conns
            .entry(addr.clone())
            .or_insert_with(ConnectionPool::new);
        Connection {
            conn: pool.connection(addr).await,
            pool: pool.clone(),
        }
    }
}

type NodeAddrFn = dyn Fn(usize) -> String + Send + Sync;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct RpcTransport {
    /// Node address mapping function.
    #[derivative(Debug = "ignore")]
    node_addr: Box<NodeAddrFn>,
    connections: Connections,
}

impl RpcTransport {
    /// Creates a new RPC transport.
    pub fn new(node_addr: Box<NodeAddrFn>) -> Self {
        RpcTransport {
            node_addr,
            connections: Connections::new(),
        }
    }
}

// Helping functions to get proto buffers from paxos or ble structs

fn get_proto_ballot(ballot: ble::Ballot) -> Option<proto::Ballot> {
    Some(proto::Ballot {
        n: ballot.n,
        priority: ballot.priority,
        pid: ballot.pid,
    })
}

fn get_proto_entry(cmd: StoreCommand) -> proto::Entry {
    proto::Entry {
        id: cmd.id as u64,
        sql: cmd.sql,
    }
}

fn get_proto_sync_item(syncitem: util::SyncItem<StoreCommand, ()>) -> Option<proto::SyncItem> {
    match syncitem {
        util::SyncItem::Entries(entries) => Some(proto::SyncItem {
            syncitem: Some(proto::sync_item::Syncitem::Entries(
                proto::sync_item::Entries {
                    entries: entries
                        .into_iter()
                        .map(|entry| get_proto_entry(entry))
                        .collect(),
                },
            )),
        }),
        util::SyncItem::Snapshot(_) => Some(proto::SyncItem {
            // not implemented for snapshot
            syncitem: Some(proto::sync_item::Syncitem::Snapshot(true)),
        }),
        util::SyncItem::None => Some(proto::SyncItem {
            syncitem: Some(proto::sync_item::Syncitem::None(true)),
        }),
    }
}

fn get_proto_stop_sign(stopsign: storage::StopSign) -> Option<proto::StopSign> {
    let config_id = stopsign.config_id;
    let nodes = stopsign.nodes;
    let metadata = match stopsign.metadata {
        Some(meta) => meta.into_iter().map(|m| m as u32).collect(),
        _ => Vec::new(),
    };
    Some(proto::StopSign {
        config_id,
        nodes,
        metadata,
    })
}

fn get_proto_compaction(compaction: messages::Compaction) -> proto::compaction::Compaction {
    match compaction {
        messages::Compaction::Trim(ent) => {
            proto::compaction::Compaction::Trim(proto::Trim { trim: ent })
        }
        messages::Compaction::Snapshot(snp) => proto::compaction::Compaction::Snapshot(snp),
    }
}

fn get_proto_forward_compaction(
    compaction: messages::Compaction,
) -> proto::forward_compaction::Compaction {
    match compaction {
        messages::Compaction::Trim(ent) => {
            proto::forward_compaction::Compaction::Trim(proto::Trim { trim: ent })
        }
        messages::Compaction::Snapshot(snp) => proto::forward_compaction::Compaction::Snapshot(snp),
    }
}

#[async_trait]
impl SequencePaxosStoreTransport for RpcTransport {
    fn send_paxos_message(&self, msg: messages::Message<StoreCommand, ()>) {
        match msg.msg {
            messages::PaxosMsg::PrepareReq => {
                let from = msg.from;
                let to = msg.to;

                let request = proto::PrepareReq { from, to };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.prepare_request(request).await.unwrap();
                });
            }

            messages::PaxosMsg::Prepare(prep) => {
                let from = msg.from;
                let to = msg.to;

                let n = get_proto_ballot(prep.n);
                let ld = prep.ld;
                let n_accepted = get_proto_ballot(prep.n_accepted);
                let la = prep.la;
                let request = proto::Prepare {
                    from,
                    to,
                    n,
                    ld,
                    n_accepted,
                    la,
                };
                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.prepare_message(request).await.unwrap();
                });
            }

            messages::PaxosMsg::Promise(prom) => {
                let from = msg.from;
                let to = msg.to;

                let n = get_proto_ballot(prom.n);
                let n_accepted = get_proto_ballot(prom.n_accepted);
                let sync_item = prom.sync_item;
                let sync_item = match sync_item {
                    Some(sync_item) => get_proto_sync_item(sync_item),
                    _ => None,
                };
                let ld = prom.ld;
                let la = prom.la;

                let stopsign = prom.stopsign;
                let stopsign = match stopsign {
                    Some(stopsign) => get_proto_stop_sign(stopsign),
                    _ => None,
                };

                let request = proto::Promise {
                    from,
                    to,
                    n,
                    n_accepted,
                    sync_item,
                    ld,
                    la,
                    stopsign,
                };
                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.promise_message(request).await.unwrap();
                });
            }

            messages::PaxosMsg::AcceptSync(acc_sync) => {
                let from = msg.from;
                let to = msg.to;

                let n = get_proto_ballot(acc_sync.n);

                let sync_item = acc_sync.sync_item;
                let sync_item = get_proto_sync_item(sync_item);
                let sync_idx = acc_sync.sync_idx;
                let decided_idx = acc_sync.decide_idx;

                let stopsign = acc_sync.stopsign;
                let stopsign = match stopsign {
                    Some(stopsign) => get_proto_stop_sign(stopsign),
                    _ => None,
                };

                let request = proto::AcceptSync {
                    from,
                    to,
                    n,
                    sync_item,
                    sync_idx,
                    decided_idx,
                    stopsign,
                };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.accept_sync_message(request).await.unwrap();
                });
            }

            messages::PaxosMsg::FirstAccept(f) => {
                let from = msg.from;
                let to = msg.to;

                let n = get_proto_ballot(f.n);
                let entries = f
                    .entries
                    .into_iter()
                    .map(|entry| get_proto_entry(entry))
                    .collect();

                let request = proto::FirstAccept {
                    from,
                    to,
                    n,
                    entries,
                };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.first_accept_message(request).await.unwrap();
                });
            }

            messages::PaxosMsg::AcceptDecide(acc) => {
                let from = msg.from;
                let to = msg.to;

                let n = get_proto_ballot(acc.n);
                let ld = acc.ld;
                let entries = acc
                    .entries
                    .into_iter()
                    .map(|entry| get_proto_entry(entry))
                    .collect();

                let request = proto::AcceptDecide {
                    from,
                    to,
                    n,
                    ld,
                    entries,
                };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.accept_decide_message(request).await.unwrap();
                });
            }

            messages::PaxosMsg::Accepted(accepted) => {
                let from = msg.from;
                let to = msg.to;

                let n = get_proto_ballot(accepted.n);
                let la = accepted.la;
                let request = proto::Accepted { from, to, n, la };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.accepted_message(request).await.unwrap();
                });
            }

            messages::PaxosMsg::Decide(dec) => {
                let from = msg.from;
                let to = msg.to;

                let n = get_proto_ballot(dec.n);
                let ld = dec.ld;
                let request = proto::Decide { from, to, n, ld };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.decide_message(request).await.unwrap();
                });
            }

            messages::PaxosMsg::ProposalForward(props) => {
                let from = msg.from;
                let to = msg.to;

                let proposals = props
                    .into_iter()
                    .map(|prop| get_proto_entry(prop))
                    .collect();

                let request = proto::ProposalForward {
                    from,
                    to,
                    proposals,
                };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.proposal_forward_message(request).await.unwrap();
                });
            }

            messages::PaxosMsg::Compaction(comps) => {
                let from = msg.from;
                let to = msg.to;

                let compaction = Some(get_proto_compaction(comps));
                let request = proto::Compaction {
                    from,
                    to,
                    compaction,
                };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.compaction_message(request).await.unwrap();
                });
            }

            messages::PaxosMsg::ForwardCompaction(comps) => {
                let from = msg.from;
                let to = msg.to;
                let compaction = Some(get_proto_forward_compaction(comps));

                let request = proto::ForwardCompaction {
                    from,
                    to,
                    compaction,
                };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client
                        .conn
                        .forward_compaction_message(request)
                        .await
                        .unwrap();
                });
            }

            messages::PaxosMsg::AcceptStopSign(acc_ss) => {
                let from = msg.from;
                let to = msg.to;

                let n = get_proto_ballot(acc_ss.n);
                let stopsign = acc_ss.ss;
                let stopsign = get_proto_stop_sign(stopsign);

                let request = proto::AcceptStopSign {
                    from,
                    to,
                    n,
                    stopsign,
                };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.accept_stop_sign_message(request).await.unwrap();
                });
            }

            messages::PaxosMsg::AcceptedStopSign(acc_ss) => {
                let from = msg.from;
                let to = msg.to;

                let n = get_proto_ballot(acc_ss.n);
                let request = proto::AcceptedStopSign { from, to, n };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client
                        .conn
                        .accepted_stop_sign_message(request)
                        .await
                        .unwrap();
                });
            }

            messages::PaxosMsg::DecideStopSign(d_ss) => {
                let from = msg.from;
                let to = msg.to;

                let n = get_proto_ballot(d_ss.n);
                let request = proto::DecideStopSign { from, to, n };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.decide_stop_sign_message(request).await.unwrap();
                });
            }
        }
    }

    fn send_ble_message(&self, ble_msg: ble::messages::BLEMessage) {
        match ble_msg.msg {
            ble::messages::HeartbeatMsg::Request(req) => {
                let from = ble_msg.from;
                let to = ble_msg.to;

                let round = req.round;
                let request = proto::HeartbeatRequest { from, to, round };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client
                        .conn
                        .heartbeat_request_message(request)
                        .await
                        .unwrap();
                });
            }

            ble::messages::HeartbeatMsg::Reply(reply) => {
                let from = ble_msg.from;
                let to = ble_msg.to;

                let round = reply.round;
                let ballot = get_proto_ballot(reply.ballot);
                let majority_connected = reply.majority_connected;
                let request = proto::HeartbeatReply {
                    from,
                    to,
                    round,
                    ballot,
                    majority_connected,
                };

                let peer = (self.node_addr)(to as usize);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.heartbeat_reply_message(request).await.unwrap();
                });
            }
        }
    }
}

// functions to get ble or paxos structs from proto messages

fn get_ballot_from_proto(proto_ballot: proto::Ballot) -> ble::Ballot {
    ble::Ballot {
        n: proto_ballot.n,
        priority: proto_ballot.priority,
        pid: proto_ballot.pid,
    }
}

fn get_entry_from_proto(proto_entry: proto::Entry) -> StoreCommand {
    StoreCommand {
        id: proto_entry.id as usize,
        sql: proto_entry.sql,
    }
}

fn get_syncitem_from_proto(syncitem: proto::SyncItem) -> Option<util::SyncItem<StoreCommand, ()>> {
    match syncitem.syncitem.unwrap() {
        proto::sync_item::Syncitem::Entries(entries) => Some(util::SyncItem::Entries(
            entries
                .entries
                .into_iter()
                .map(|ent| get_entry_from_proto(ent))
                .collect(),
        )),
        proto::sync_item::Syncitem::Snapshot(_) => Some(util::SyncItem::Snapshot(
            storage::SnapshotType::Complete(()),
        )),
        proto::sync_item::Syncitem::None(_) => Some(util::SyncItem::None),
    }
}

fn get_stopsign_from_proto(stopsign: proto::StopSign) -> storage::StopSign {
    let config_id = stopsign.config_id;
    let nodes = stopsign.nodes;
    let metadata = Some(stopsign.metadata.into_iter().map(|m| m as u8).collect());
    storage::StopSign {
        config_id,
        nodes,
        metadata,
    }
}

fn get_compaction_from_proto(compaction: proto::compaction::Compaction) -> messages::Compaction {
    match compaction {
        proto::compaction::Compaction::Trim(trim) => messages::Compaction::Trim(trim.trim),
        proto::compaction::Compaction::Snapshot(snp) => messages::Compaction::Snapshot(snp),
    }
}

fn get_forward_compaction_from_proto(
    compaction: proto::forward_compaction::Compaction,
) -> messages::Compaction {
    match compaction {
        proto::forward_compaction::Compaction::Trim(trim) => messages::Compaction::Trim(trim.trim),
        proto::forward_compaction::Compaction::Snapshot(snp) => messages::Compaction::Snapshot(snp),
    }
}

#[derive(Debug)]
pub struct RpcService {
    /// The ChiselStore server access via this RPC service.
    pub server: Arc<StoreServer<RpcTransport>>,
}

impl RpcService {
    /// Creates a new RPC service.
    pub fn new(server: Arc<StoreServer<RpcTransport>>) -> Self {
        Self { server }
    }
}

#[tonic::async_trait]
impl Rpc for RpcService {
    async fn execute(
        &self,
        request: Request<proto::Query>,
    ) -> Result<Response<proto::QueryResults>, tonic::Status> {
        let query = request.into_inner();
        let consistency =
            proto::Consistency::from_i32(query.consistency).unwrap_or(proto::Consistency::Strong);
        let consistency = match consistency {
            proto::Consistency::Strong => Consistency::Strong,
            proto::Consistency::RelaxedReads => Consistency::RelaxedReads,
        };

        let server = self.server.clone();
        let results = match server.query(query.sql, consistency).await {
            Ok(results) => results,
            Err(e) => return Err(Status::internal(format!("{}", e))),
        };

        let mut rows = vec![];
        for row in results.rows {
            rows.push(proto::QueryRow {
                values: row.values.clone(),
            })
        }
        Ok(Response::new(proto::QueryResults { rows }))
    }

    async fn prepare_request(
        &self,
        request: Request<proto::PrepareReq>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from as u64;
        let to_id = msg.to as u64;

        let msg = messages::Message::with(from_id, to_id, messages::PaxosMsg::PrepareReq);

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn prepare_message(
        &self,
        request: Request<proto::Prepare>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let n = get_ballot_from_proto(msg.n.unwrap());
        let ld = msg.ld;
        let n_accepted = get_ballot_from_proto(msg.n_accepted.unwrap());
        let la = msg.la;
        let prep = messages::Prepare::with(n, ld, n_accepted, la);
        let msg = messages::Message::with(from_id, to_id, messages::PaxosMsg::Prepare(prep));

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn promise_message(
        &self,
        request: Request<proto::Promise>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let n = get_ballot_from_proto(msg.n.unwrap());
        let n_accepted = get_ballot_from_proto(msg.n_accepted.unwrap());
        let sync_item = msg.sync_item;
        let sync_item = match sync_item {
            Some(sync_item) => get_syncitem_from_proto(sync_item),
            _ => None,
        };
        let ld = msg.ld;
        let la = msg.la;
        let stopsign = msg.stopsign;
        let stopsign = match stopsign {
            Some(stopsign) => Some(get_stopsign_from_proto(stopsign)),
            _ => None,
        };
        let promise = messages::Promise::with(n, n_accepted, sync_item, ld, la, stopsign);
        let msg = messages::Message::with(from_id, to_id, messages::PaxosMsg::Promise(promise));

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn accept_sync_message(
        &self,
        request: Request<proto::AcceptSync>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let n = get_ballot_from_proto(msg.n.unwrap());
        let sync_item = msg.sync_item;
        let sync_item = get_syncitem_from_proto(sync_item.unwrap()).unwrap();
        let sync_idx = msg.sync_idx;
        let decide_idx = msg.decided_idx;
        let stopsign = msg.stopsign;
        let stopsign = match stopsign {
            Some(stopsign) => Some(get_stopsign_from_proto(stopsign)),
            _ => None,
        };
        let acc_sync = messages::AcceptSync::with(n, sync_item, sync_idx, decide_idx, stopsign);
        let msg = messages::Message::with(from_id, to_id, messages::PaxosMsg::AcceptSync(acc_sync));

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn first_accept_message(
        &self,
        request: Request<proto::FirstAccept>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let n = get_ballot_from_proto(msg.n.unwrap());
        let entries = msg
            .entries
            .into_iter()
            .map(|entry| get_entry_from_proto(entry))
            .collect();
        let first_acc = messages::FirstAccept::with(n, entries);
        let msg =
            messages::Message::with(from_id, to_id, messages::PaxosMsg::FirstAccept(first_acc));

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn accept_decide_message(
        &self,
        request: Request<proto::AcceptDecide>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let n = get_ballot_from_proto(msg.n.unwrap());
        let ld = msg.ld;
        let entries = msg
            .entries
            .into_iter()
            .map(|entry| get_entry_from_proto(entry))
            .collect();
        let acc_dec = messages::AcceptDecide::with(n, ld, entries);
        let msg =
            messages::Message::with(from_id, to_id, messages::PaxosMsg::AcceptDecide(acc_dec));

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn accepted_message(
        &self,
        request: Request<proto::Accepted>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let n = get_ballot_from_proto(msg.n.unwrap());
        let la = msg.la;
        let acc = messages::Accepted::with(n, la);
        let msg = messages::Message::with(from_id, to_id, messages::PaxosMsg::Accepted(acc));

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn decide_message(
        &self,
        request: Request<proto::Decide>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let n = get_ballot_from_proto(msg.n.unwrap());
        let ld = msg.ld;
        let dec = messages::Decide::with(n, ld);
        let msg = messages::Message::with(from_id, to_id, messages::PaxosMsg::Decide(dec));

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn proposal_forward_message(
        &self,
        request: Request<proto::ProposalForward>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let proposals = msg
            .proposals
            .into_iter()
            .map(|prop| get_entry_from_proto(prop))
            .collect();
        let prop_for = messages::PaxosMsg::ProposalForward(proposals);
        let msg = messages::Message::with(from_id, to_id, prop_for);

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn compaction_message(
        &self,
        request: Request<proto::Compaction>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let compaction = get_compaction_from_proto(msg.compaction.unwrap());
        let com = messages::PaxosMsg::Compaction(compaction);
        let msg = messages::Message::with(from_id, to_id, com);

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn forward_compaction_message(
        &self,
        request: Request<proto::ForwardCompaction>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let compaction = get_forward_compaction_from_proto(msg.compaction.unwrap());
        let com = messages::PaxosMsg::ForwardCompaction(compaction);
        let msg = messages::Message::with(from_id, to_id, com);

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn accept_stop_sign_message(
        &self,
        request: Request<proto::AcceptStopSign>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let n = get_ballot_from_proto(msg.n.unwrap());
        let stopsign = get_stopsign_from_proto(msg.stopsign.unwrap());
        let acc_ss = messages::AcceptStopSign::with(n, stopsign);
        let msg =
            messages::Message::with(from_id, to_id, messages::PaxosMsg::AcceptStopSign(acc_ss));

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn accepted_stop_sign_message(
        &self,
        request: Request<proto::AcceptedStopSign>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let n = get_ballot_from_proto(msg.n.unwrap());
        let acced_ss = messages::AcceptedStopSign::with(n);
        let msg = messages::Message::with(
            from_id,
            to_id,
            messages::PaxosMsg::AcceptedStopSign(acced_ss),
        );

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn decide_stop_sign_message(
        &self,
        request: Request<proto::DecideStopSign>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let n = get_ballot_from_proto(msg.n.unwrap());
        let dec_ss = messages::DecideStopSign::with(n);
        let msg =
            messages::Message::with(from_id, to_id, messages::PaxosMsg::DecideStopSign(dec_ss));

        let server = self.server.clone();
        server.recv_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn heartbeat_request_message(
        &self,
        request: Request<proto::HeartbeatRequest>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;

        let round = msg.round;
        let req = ble::messages::HeartbeatRequest::with(round);
        let msg = ble::messages::BLEMessage::with(
            from_id,
            to_id,
            ble::messages::HeartbeatMsg::Request(req),
        );

        let server = self.server.clone();
        server.recv_ble_msg(msg);
        Ok(Response::new(proto::Void {}))
    }

    async fn heartbeat_reply_message(
        &self,
        request: Request<proto::HeartbeatReply>,
    ) -> Result<Response<proto::Void>, tonic::Status> {
        let msg = request.into_inner();
        let from_id = msg.from;
        let to_id = msg.to;
        let round = msg.round;

        let ballot = get_ballot_from_proto(msg.ballot.unwrap());
        let majority_connected = msg.majority_connected;
        let rep = ble::messages::HeartbeatReply::with(round, ballot, majority_connected);
        let msg = ble::messages::BLEMessage::with(
            from_id,
            to_id,
            ble::messages::HeartbeatMsg::Reply(rep),
        );

        let server = self.server.clone();
        server.recv_ble_msg(msg);
        Ok(Response::new(proto::Void {}))
    }
}
