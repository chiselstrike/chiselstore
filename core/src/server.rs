//! ChiselStore server module.

use crate::errors::StoreError;
use async_notify::Notify;
use async_trait::async_trait;
use bytes::Bytes;
use crossbeam_channel as channel;
use crossbeam_channel::{Receiver, Sender};
use derivative::Derivative;
use little_raft::{
    cluster::Cluster,
    message::Message,
    replica::{Replica, ReplicaID},
    state_machine::{Snapshot, StateMachine, StateMachineTransition, TransitionState},
};
use sqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// ChiselStore transport layer.
///
/// Your application should implement this trait to provide network access
/// to the ChiselStore server.
#[async_trait]
pub trait StoreTransport {
    /// Send a store command message `msg` to `to_id` node.
    fn send(&self, to_id: usize, msg: Message<StoreCommand, Bytes>);

    /// Delegate command to another node.
    async fn delegate(
        &self,
        to_id: usize,
        sql: String,
        consistency: Consistency,
    ) -> Result<QueryResults, StoreError>;
}

/// Consistency mode.
#[derive(Debug)]
pub enum Consistency {
    /// Strong consistency. Both reads and writes go through the Raft leader,
    /// which makes them linearizable.
    Strong,
    /// Relaxed reads. Reads are performed on the local node, which relaxes
    /// read consistency and allows stale reads.
    RelaxedReads,
}

/// Store command.
///
/// A store command is a SQL statement that is replicated in the Raft cluster.
#[derive(Clone, Debug)]
pub struct StoreCommand {
    /// Unique ID of this command.
    pub id: usize,
    /// The SQL statement of this command.
    pub sql: String,
}

impl StateMachineTransition for StoreCommand {
    type TransitionID = usize;

    fn get_id(&self) -> Self::TransitionID {
        self.id
    }
}

/// Store configuration.
#[derive(Debug)]
struct StoreConfig {
    /// Connection pool size.
    conn_pool_size: usize,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct Store<T: StoreTransport + Send + Sync> {
    /// ID of the node this Cluster objecti s on.
    this_id: usize,
    /// Is this node the leader?
    leader: Option<usize>,
    leader_exists: AtomicBool,
    waiters: Vec<Arc<Notify>>,
    /// Pending messages
    pending_messages: Vec<Message<StoreCommand, Bytes>>,
    /// Transport layer.
    transport: Arc<T>,
    #[derivative(Debug = "ignore")]
    conn_pool: Vec<Arc<Mutex<Connection>>>,
    conn_idx: usize,
    pending_transitions: Vec<StoreCommand>,
    command_completions: HashMap<u64, Arc<Notify>>,
    results: HashMap<u64, Result<QueryResults, StoreError>>,
}

impl<T: StoreTransport + Send + Sync> Store<T> {
    pub fn new(this_id: usize, transport: T, config: StoreConfig) -> Self {
        let mut conn_pool = vec![];
        let conn_pool_size = config.conn_pool_size;
        for _ in 0..conn_pool_size {
            // FIXME: Let's use the 'memdb' VFS of SQLite, which allows concurrent threads
            // accessing the same in-memory database.
            let flags = OpenFlags::new()
                .set_read_write()
                .set_create()
                .set_no_mutex();
            let mut conn =
                Connection::open_with_flags(format!("node{}.db", this_id), flags).unwrap();
            conn.set_busy_timeout(5000).unwrap();
            conn_pool.push(Arc::new(Mutex::new(conn)));
        }
        let conn_idx = 0;
        Store {
            this_id,
            leader: None,
            leader_exists: AtomicBool::new(false),
            waiters: Vec::new(),
            pending_messages: Vec::new(),
            transport: Arc::new(transport),
            conn_pool,
            conn_idx,
            pending_transitions: Vec::new(),
            command_completions: HashMap::new(),
            results: HashMap::new(),
        }
    }

    pub fn is_leader(&self) -> bool {
        match self.leader {
            Some(id) => id == self.this_id,
            _ => false,
        }
    }

    pub fn get_connection(&mut self) -> Arc<Mutex<Connection>> {
        let idx = self.conn_idx % self.conn_pool.len();
        let conn = &self.conn_pool[idx];
        self.conn_idx += 1;
        conn.clone()
    }
}

fn query(conn: Arc<Mutex<Connection>>, sql: String) -> Result<QueryResults, StoreError> {
    let conn = conn.lock().unwrap();
    let mut rows = vec![];
    conn.iterate(sql, |pairs| {
        let mut row = QueryRow::new();
        for &(_, value) in pairs.iter() {
            row.values.push(value.unwrap().to_string());
        }
        rows.push(row);
        true
    })?;
    Ok(QueryResults { rows })
}

impl<T: StoreTransport + Send + Sync> StateMachine<StoreCommand, Bytes> for Store<T> {
    fn register_transition_state(&mut self, transition_id: usize, state: TransitionState) {
        match state {
            TransitionState::Applied | TransitionState::Abandoned(_) => {
                if let Some(completion) = self.command_completions.remove(&(transition_id as u64)) {
                    completion.notify();
                }
            }
            _ => (),
        }
    }

    fn apply_transition(&mut self, transition: StoreCommand) {
        if transition.id == NOP_TRANSITION_ID {
            return;
        }
        let conn = self.get_connection();
        let results = query(conn, transition.sql);
        if self.is_leader() {
            self.results.insert(transition.id as u64, results);
        }
    }

    fn get_pending_transitions(&mut self) -> Vec<StoreCommand> {
        let cur = self.pending_transitions.clone();
        self.pending_transitions = Vec::new();
        cur
    }

    fn get_snapshot(&mut self) -> Option<Snapshot<Bytes>> {
        None
    }

    fn create_snapshot(&mut self, _index: usize, _term: usize) -> Snapshot<Bytes> {
        todo!("Snapshotting is not implemented.");
    }

    fn set_snapshot(&mut self, _snapshot: Snapshot<Bytes>) {
        todo!("Snapshotting is not implemented.");
    }
}

impl<T: StoreTransport + Send + Sync> Cluster<StoreCommand, Bytes> for Store<T> {
    fn register_leader(&mut self, leader_id: Option<ReplicaID>) {
        if let Some(id) = leader_id {
            self.leader = Some(id);
            self.leader_exists.store(true, Ordering::SeqCst);
        } else {
            self.leader = None;
            self.leader_exists.store(false, Ordering::SeqCst);
        }
        let waiters = self.waiters.clone();
        self.waiters = Vec::new();
        for waiter in waiters {
            waiter.notify();
        }
    }

    fn send_message(&mut self, to_id: usize, message: Message<StoreCommand, Bytes>) {
        self.transport.send(to_id, message);
    }

    fn receive_messages(&mut self) -> Vec<Message<StoreCommand, Bytes>> {
        let cur = self.pending_messages.clone();
        self.pending_messages = Vec::new();
        cur
    }

    fn halt(&self) -> bool {
        false
    }
}

type StoreReplica<T> = Replica<Store<T>, Store<T>, StoreCommand, Bytes>;

/// ChiselStore server.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct StoreServer<T: StoreTransport + Send + Sync> {
    next_cmd_id: AtomicU64,
    store: Arc<Mutex<Store<T>>>,
    #[derivative(Debug = "ignore")]
    replica: Arc<Mutex<StoreReplica<T>>>,
    message_notifier_rx: Receiver<()>,
    message_notifier_tx: Sender<()>,
    transition_notifier_rx: Receiver<()>,
    transition_notifier_tx: Sender<()>,
}

/// Query row.
#[derive(Debug)]
pub struct QueryRow {
    /// Column values of the row.
    pub values: Vec<String>,
}

impl QueryRow {
    fn new() -> Self {
        QueryRow { values: Vec::new() }
    }
}

/// Query results.
#[derive(Debug)]
pub struct QueryResults {
    /// Query result rows.
    pub rows: Vec<QueryRow>,
}

const NOP_TRANSITION_ID: usize = 0;
const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(500);
const MIN_ELECTION_TIMEOUT: Duration = Duration::from_millis(750);
const MAX_ELECTION_TIMEOUT: Duration = Duration::from_millis(950);

impl<T: StoreTransport + Send + Sync> StoreServer<T> {
    /// Start a new server as part of a ChiselStore cluster.
    pub fn start(this_id: usize, peers: Vec<usize>, transport: T) -> Result<Self, StoreError> {
        let config = StoreConfig { conn_pool_size: 20 };
        let store = Arc::new(Mutex::new(Store::new(this_id, transport, config)));
        let noop = StoreCommand {
            id: NOP_TRANSITION_ID,
            sql: "".to_string(),
        };
        let (message_notifier_tx, message_notifier_rx) = channel::unbounded();
        let (transition_notifier_tx, transition_notifier_rx) = channel::unbounded();
        let replica = Replica::new(
            this_id,
            peers,
            store.clone(),
            store.clone(),
            0, // snapshotting is disabled
            noop,
            HEARTBEAT_TIMEOUT,
            (MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT),
        );
        let replica = Arc::new(Mutex::new(replica));
        Ok(StoreServer {
            next_cmd_id: AtomicU64::new(1), // zero is reserved for no-op.
            store,
            replica,
            message_notifier_rx,
            message_notifier_tx,
            transition_notifier_rx,
            transition_notifier_tx,
        })
    }

    /// Run the blocking event loop.
    pub fn run(&self) {
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
        // If the statement is a read statement, let's use whatever
        // consistency the user provided; otherwise fall back to strong
        // consistency.
        let consistency = if is_read_statement(stmt.as_ref()) {
            consistency
        } else {
            Consistency::Strong
        };
        let results = match consistency {
            Consistency::Strong => {
                self.wait_for_leader().await;
                let (delegate, leader, transport) = {
                    let store = self.store.lock().unwrap();
                    (!store.is_leader(), store.leader, store.transport.clone())
                };
                if delegate {
                    if let Some(leader_id) = leader {
                        return transport
                            .delegate(leader_id, stmt.as_ref().to_string(), consistency)
                            .await;
                    }
                    return Err(StoreError::NotLeader);
                }
                let (notify, id) = {
                    let mut store = self.store.lock().unwrap();
                    let id = self.next_cmd_id.fetch_add(1, Ordering::SeqCst);
                    let cmd = StoreCommand {
                        id: id as usize,
                        sql: stmt.as_ref().to_string(),
                    };
                    let notify = Arc::new(Notify::new());
                    store.command_completions.insert(id, notify.clone());
                    store.pending_transitions.push(cmd);
                    (notify, id)
                };
                self.transition_notifier_tx.send(()).unwrap();
                notify.notified().await;
                if let Some(results) = self.store.lock().unwrap().results.remove(&id) {
                    results?
                } else {
                    return Err(StoreError::NotLeader);
                }
            }
            Consistency::RelaxedReads => {
                let conn = {
                    let mut store = self.store.lock().unwrap();
                    store.get_connection()
                };
                query(conn, stmt.as_ref().to_string())?
            }
        };
        Ok(results)
    }

    /// Wait for a leader to be elected.
    pub async fn wait_for_leader(&self) {
        loop {
            let notify = {
                let mut store = self.store.lock().unwrap();
                if store.leader_exists.load(Ordering::SeqCst) {
                    break;
                }
                let notify = Arc::new(Notify::new());
                store.waiters.push(notify.clone());
                notify
            };
            if self
                .store
                .lock()
                .unwrap()
                .leader_exists
                .load(Ordering::SeqCst)
            {
                break;
            }
            // TODO: add a timeout and fail if necessary
            notify.notified().await;
        }
    }

    /// Receive a message from the ChiselStore cluster.
    pub fn recv_msg(&self, msg: little_raft::message::Message<StoreCommand, Bytes>) {
        let mut cluster = self.store.lock().unwrap();
        cluster.pending_messages.push(msg);
        self.message_notifier_tx.send(()).unwrap();
    }
}

fn is_read_statement(stmt: &str) -> bool {
    stmt.to_lowercase().starts_with("select")
}
