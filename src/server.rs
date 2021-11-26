use crate::errors::StoreError;
use async_notify::Notify;
use crossbeam_channel as channel;
use crossbeam_channel::{Receiver, Sender};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use little_raft::{
    cluster::Cluster,
    message::Message,
    replica::{Replica, ReplicaID},
    state_machine::{StateMachine, StateMachineTransition, TransitionState},
};
use sqlite::Connection;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// ChiselStore transport layer.
///
/// Your application should implement this trait to provide network access
/// to the ChiselStore server.
pub trait StoreTransport {
    fn send(&self, to_id: usize, msg: Message<StoreCommand>);
}

/// Consistency mode.
pub enum Consistency {
    /// Strong consistency. Both reads and writes go through the Raft leader,
    /// which makes them linearizable.
    Strong,
    /// Relaxed reads. Reads are performed on the local node, which relaxes
    /// read consistency and allows stale reads.
    RelaxedReads,
}

#[derive(Clone, Debug)]
pub struct StoreCommand {
    pub id: usize,
    pub sql: String,
}

impl StateMachineTransition for StoreCommand {
    type TransitionID = usize;

    fn get_id(&self) -> Self::TransitionID {
        self.id
    }
}

struct Store {
    conn: Connection,
    pending_transitions: Vec<StoreCommand>,
    command_completions: HashMap<u64, Arc<Notify>>,
    results: HashMap<u64, Result<QueryResults, StoreError>>,
}

impl Store {
    pub fn new(conn: Connection) -> Self {
        Store {
            conn,
            pending_transitions: Vec::new(),
            command_completions: HashMap::new(),
            results: HashMap::new(),
        }
    }

    pub fn query(&self, sql: String) -> Result<QueryResults, StoreError> {
        let mut rows = vec![];
        self.conn.iterate(sql, |pairs| {
            let mut row = QueryRow::new();
            for &(_, value) in pairs.iter() {
                row.values.push(value.unwrap().to_string());
            }
            rows.push(row);
            true
        })?;
        Ok(QueryResults { rows })
    }
}

impl StateMachine<StoreCommand> for Store {
    fn register_transition_state(&mut self, transition_id: usize, state: TransitionState) {
        if state == TransitionState::Applied {
            if let Some(completion) = self.command_completions.remove(&(transition_id as u64)) {
                completion.notify();
            }
        }
    }

    fn apply_transition(&mut self, transition: StoreCommand) {
        if transition.id == NOP_TRANSITION_ID {
            return;
        }
        let results = self.query(transition.sql);
        self.results.insert(transition.id as u64, results);
    }

    fn get_pending_transitions(&mut self) -> Vec<StoreCommand> {
        let cur = self.pending_transitions.clone();
        self.pending_transitions = Vec::new();
        cur
    }
}

pub struct StoreCluster<T: StoreTransport> {
    /// ID of the node this Cluster objecti s on.
    this_id: usize,
    /// Is this node the leader?
    is_leader: bool,
    /// Pending messages
    pending_messages: Vec<Message<StoreCommand>>,
    /// Transport layer.
    transport: T,
}

impl<T: StoreTransport> StoreCluster<T> {
    pub fn new(this_id: usize, transport: T) -> Self {
        StoreCluster {
            this_id,
            is_leader: false,
            pending_messages: Vec::new(),
            transport,
        }
    }
}

impl<T: StoreTransport> Cluster<StoreCommand> for StoreCluster<T> {
    fn register_leader(&mut self, leader_id: Option<ReplicaID>) {
        if let Some(id) = leader_id {
            println!("{} is the leader.", id);
            if id == self.this_id {
                self.is_leader = true;
            } else {
                self.is_leader = false;
            }
        } else {
            self.is_leader = false;
        }
    }

    fn send_message(&mut self, to_id: usize, message: Message<StoreCommand>) {
        self.transport.send(to_id, message);
    }

    fn receive_messages(&mut self) -> Vec<Message<StoreCommand>> {
        let cur = self.pending_messages.clone();
        self.pending_messages = Vec::new();
        cur
    }

    fn halt(&self) -> bool {
        false
    }
}

/// ChiselStore server.
pub struct StoreServer<T: StoreTransport> {
    next_cmd_id: AtomicU64,
    cluster: Arc<Mutex<StoreCluster<T>>>,
    state_machine: Arc<Mutex<Store>>,
    replica: Arc<Mutex<Replica<Store, StoreCommand, StoreCluster<T>>>>,
    message_notifier_rx: Receiver<()>,
    message_notifier_tx: Sender<()>,
    transition_notifier_rx: Receiver<()>,
    transition_notifier_tx: Sender<()>,
}

pub struct QueryRow {
    pub values: Vec<String>,
}

impl QueryRow {
    fn new() -> Self {
        QueryRow { values: Vec::new() }
    }
}

pub struct QueryResults {
    pub rows: Vec<QueryRow>,
}

const NOP_TRANSITION_ID: usize = 0;
const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(500);
const MIN_ELECTION_TIMEOUT: Duration = Duration::from_millis(750);
const MAX_ELECTION_TIMEOUT: Duration = Duration::from_millis(950);

impl<T: StoreTransport + Send + 'static> StoreServer<T> {
    /// Start a new server as part of a ChiselStore cluster.
    pub fn start(this_id: usize, peers: Vec<usize>, transport: T) -> Result<Self, StoreError> {
        let conn = sqlite::open(":memory:")?;
        let state_machine = Arc::new(Mutex::new(Store::new(conn)));
        let cluster = Arc::new(Mutex::new(StoreCluster::new(this_id, transport)));
        let noop = StoreCommand {
            id: NOP_TRANSITION_ID,
            sql: "".to_string(),
        };
        let (message_notifier_tx, message_notifier_rx) = channel::unbounded();
        let (transition_notifier_tx, transition_notifier_rx) = channel::unbounded();
        let clusterx = cluster.clone();
        let state_machinex = state_machine.clone();
        let replica = Replica::new(
            this_id,
            peers,
            clusterx,
            state_machinex,
            noop,
            HEARTBEAT_TIMEOUT,
            (MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT),
        );
        let replica = Arc::new(Mutex::new(replica));
        Ok(StoreServer {
            next_cmd_id: AtomicU64::new(1), // zero is reserved for no-op.
            cluster,
            state_machine,
            replica,
            message_notifier_rx,
            message_notifier_tx,
            transition_notifier_rx,
            transition_notifier_tx,
        })
    }

    /// Run the blocking event loop.
    pub fn start_blocking(&self) {
        self.replica.lock().unwrap().start(
            self.message_notifier_rx.clone(),
            self.transition_notifier_rx.clone(),
        );
    }

    /// Execute a SQL statement on the ChiselStore cluster.
    pub async fn query<S: AsRef<str>>(
        &self,
        stmt: S,
        consistency: Consistency,
    ) -> Result<QueryResults, StoreError> {
        let results = match consistency {
            Consistency::Strong => {
                // FIXME: check that we are the leader, if not, delegate.
                let id = self.next_cmd_id.fetch_add(1, Ordering::SeqCst);
                self.state_machine
                    .lock()
                    .unwrap()
                    .pending_transitions
                    .push(StoreCommand {
                        id: id as usize,
                        sql: stmt.as_ref().to_string(),
                    });
                let notify = Arc::new(Notify::new());
                self.state_machine
                    .lock()
                    .unwrap()
                    .command_completions
                    .insert(id, notify.clone());
                self.transition_notifier_tx.send(()).unwrap();
                notify.notified().await;
                let results = self
                    .state_machine
                    .lock()
                    .unwrap()
                    .results
                    .remove(&id)
                    .unwrap();
                results?
            }
            Consistency::RelaxedReads => {
                // FIXME: ensure that `stmt` is a read!
                let state_machine = self.state_machine.lock().unwrap();
                state_machine.query(stmt.as_ref().to_string())?
            }
        };
        Ok(results)
    }

    /// Receive a message from the ChiselStore cluster.
    pub fn recv_msg(&self, msg: little_raft::message::Message<StoreCommand>) {
        let mut cluster = self.cluster.lock().unwrap();
        cluster.pending_messages.push(msg);
        self.message_notifier_tx.send(()).unwrap();
    }
}
