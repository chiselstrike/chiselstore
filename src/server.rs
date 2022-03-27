//! ChiselStore server module.

use crate::errors::StoreError;
use async_notify::Notify;
use async_trait::async_trait;
use derivative::Derivative;
use omnipaxos_core::ballot_leader_election::Ballot;
use omnipaxos_core::sequence_paxos::{SequencePaxos, SequencePaxosConfig};
use omnipaxos_core::storage::Storage;
use omnipaxos_core::{
    ballot_leader_election as ble, messages,
    storage::{Snapshot, StopSignEntry},
};
use sqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::{thread::sleep, time::Duration};

#[derive(Debug)]
pub struct QueryRow {
    pub values: Vec<String>,
}

impl QueryRow {
    fn new() -> Self {
        QueryRow { values: Vec::new() }
    }
}

#[derive(Debug)]
pub struct QueryResults {
    pub rows: Vec<QueryRow>,
}

#[derive(Debug)]
struct StoreConfig {
    conn_pool_size: usize,
}

#[derive(Clone, Debug)]
pub struct StoreCommand {
    pub id: usize,
    pub sql: String,
}

#[derive(Debug)]
pub enum Consistency {
    Strong,
    RelaxedReads,
}

#[async_trait]
pub trait SequencePaxosStoreTransport {
    fn send_paxos_message(&self, msg: messages::Message<StoreCommand, ()>);
    fn send_ble_message(&self, ble_message: ble::messages::BLEMessage);
}

#[derive(Debug)]
pub struct ResultNotifier {
    cmnd_completion: HashMap<u64, Arc<Notify>>,
    results: HashMap<u64, Result<QueryResults, StoreError>>,
}

impl ResultNotifier {
    pub fn new() -> Self {
        Self {
            cmnd_completion: HashMap::new(),
            results: HashMap::new(),
        }
    }

    pub fn add_command(&mut self, id: u64, notify: Arc<Notify>) {
        self.cmnd_completion.insert(id, notify);
    }

    pub fn remove_command_and_add_result(
        &mut self,
        id: u64,
        res: Result<QueryResults, StoreError>,
    ) {
        self.results.insert(id, res);
        if let Some(completion) = self.cmnd_completion.remove(&id) {
            completion.notify();
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SQLiteConnection {
    #[derivative(Debug = "ignore")]
    conn_pool: Vec<Arc<Mutex<Connection>>>,
    conn_idx: usize,
}

impl SQLiteConnection {
    pub(in crate::server) fn new(this_id: u64, config: StoreConfig) -> Self {
        let mut conn_pool = vec![];
        let conn_pool_size = config.conn_pool_size;
        for _ in 0..conn_pool_size {
            let flags = OpenFlags::new()
                .set_read_write()
                .set_create()
                .set_no_mutex();
            let mut conn =
                Connection::open_with_flags(format!("node{}.db", this_id), flags).unwrap();
            conn.set_busy_timeout(5000).unwrap();
            conn_pool.push(Arc::new(Mutex::new(conn)));
        }

        Self {
            conn_pool,
            conn_idx: 0,
        }
    }

    pub fn get_connection(&mut self) -> Arc<Mutex<Connection>> {
        let idx = self.conn_idx % self.conn_pool.len();
        let conn = &self.conn_pool[idx];
        self.conn_idx += 1;
        conn.clone()
    }

    fn query(&mut self, sql: String) -> Result<QueryResults, StoreError> {
        let conn = self.get_connection();
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
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Store<S>
where
    S: Snapshot<StoreCommand>,
{
    log: Vec<StoreCommand>,
    n_prom: Ballot,
    acc_round: Ballot,
    ld: u64,
    trimmed_idx: u64,
    snapshot: Option<S>,
    stopsign: Option<StopSignEntry>,
    #[derivative(Debug = "ignore")]
    sqlite_connection: Arc<Mutex<SQLiteConnection>>,
    query_result_notifier: Arc<Mutex<ResultNotifier>>,
}

impl<S: Snapshot<StoreCommand>> Store<S> {
    pub fn new(
        sqlite_connection: Arc<Mutex<SQLiteConnection>>,
        query_result_notifier: Arc<Mutex<ResultNotifier>>,
    ) -> Self {
        Self {
            log: Vec::new(),
            n_prom: ble::Ballot::default(),
            acc_round: ble::Ballot::default(),
            ld: 0,
            trimmed_idx: 0,
            snapshot: None,
            stopsign: None,
            sqlite_connection,
            query_result_notifier,
        }
    }

    pub fn apply_queries(&mut self, transition: StoreCommand) {
        let mut query_result_notifier = self.query_result_notifier.lock().unwrap();
        let mut sqlite_connection = self.sqlite_connection.lock().unwrap();
        let results = sqlite_connection.query(transition.sql);
        query_result_notifier.remove_command_and_add_result(transition.id as u64, results);
    }
}

impl<S> Storage<StoreCommand, S> for Store<S>
where
    S: Snapshot<StoreCommand>,
{
    fn append_entry(&mut self, entry: StoreCommand) -> u64 {
        self.log.push(entry);
        self.get_log_len()
    }

    fn append_entries(&mut self, entries: Vec<StoreCommand>) -> u64 {
        let mut e = entries;
        self.log.append(&mut e);
        self.get_log_len()
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<StoreCommand>) -> u64 {
        self.log.truncate(from_idx as usize);
        self.append_entries(entries)
    }

    fn set_promise(&mut self, n_prom: Ballot) {
        self.n_prom = n_prom;
    }

    fn set_decided_idx(&mut self, ld: u64) {
        let decided_entries = self.get_entries(self.ld, ld);
        let mut tmp_vec = Vec::new();
        decided_entries
            .into_iter()
            .for_each(|entry| tmp_vec.push(entry.clone()));

        tmp_vec
            .into_iter()
            .for_each(|entry| self.apply_queries(entry));

        self.ld = ld;
    }

    fn get_decided_idx(&self) -> u64 {
        self.ld
    }

    fn set_accepted_round(&mut self, na: Ballot) {
        self.acc_round = na;
    }

    fn get_accepted_round(&self) -> Ballot {
        self.acc_round
    }

    fn get_entries(&self, from: u64, to: u64) -> &[StoreCommand] {
        self.log.get(from as usize..to as usize).unwrap_or(&[])
    }

    fn get_log_len(&self) -> u64 {
        self.log.len() as u64
    }

    fn get_suffix(&self, from: u64) -> &[StoreCommand] {
        match self.log.get(from as usize..) {
            Some(s) => s,
            None => &[],
        }
    }

    fn get_promise(&self) -> Ballot {
        self.n_prom
    }

    fn set_stopsign(&mut self, s: StopSignEntry) {
        self.stopsign = Some(s);
    }

    fn get_stopsign(&self) -> Option<StopSignEntry> {
        self.stopsign.clone()
    }

    fn trim(&mut self, idx: u64) {
        self.log.drain(0..idx as usize);
    }

    fn set_compacted_idx(&mut self, idx: u64) {
        self.trimmed_idx = idx;
    }

    fn get_compacted_idx(&self) -> u64 {
        self.trimmed_idx
    }

    fn set_snapshot(&mut self, snapshot: S) {
        self.snapshot = Some(snapshot);
    }

    fn get_snapshot(&self) -> Option<S> {
        self.snapshot.clone()
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct StoreServer<T: SequencePaxosStoreTransport + Send + Sync> {
    id: u64,
    transport: Arc<T>,
    next_cmd_id: AtomicU64,
    #[derivative(Debug = "ignore")]
    seq_paxos: Arc<Mutex<SequencePaxos<StoreCommand, (), Store<()>>>>,
    #[derivative(Debug = "ignore")]
    ble: Arc<Mutex<ble::BallotLeaderElection>>,
    sqlite_connection: Arc<Mutex<SQLiteConnection>>,
    query_result_notifier: Arc<Mutex<ResultNotifier>>,
    halt: Arc<Mutex<bool>>,
}

const HEARTBEAT_DELAY: u64 = 10;
const CONN_POOL_SIZE: usize = 20;

impl<T: SequencePaxosStoreTransport + Send + Sync> StoreServer<T> {
    pub fn start(id: u64, peers: Vec<u64>, transport: T) -> Result<Self, StoreError> {
        let config = StoreConfig {
            conn_pool_size: CONN_POOL_SIZE,
        };
        let config_id = 1;

        let mut sp_config = SequencePaxosConfig::default();
        sp_config.set_configuration_id(config_id);
        sp_config.set_pid(id);
        sp_config.set_peers(peers.clone());

        let mut ble_config = ble::BLEConfig::default();
        ble_config.set_pid(id);
        ble_config.set_peers(peers);
        ble_config.set_hb_delay(HEARTBEAT_DELAY);

        let sqlite_connection = Arc::new(Mutex::new(SQLiteConnection::new(id, config)));
        let query_result_notifier = Arc::new(Mutex::new(ResultNotifier::new()));
        let store = Store::new(sqlite_connection.clone(), query_result_notifier.clone());
        let seq_paxos = Arc::new(Mutex::new(SequencePaxos::with(sp_config, store)));
        let ble = Arc::new(Mutex::new(ble::BallotLeaderElection::with(ble_config)));
        let halt = Arc::new(Mutex::new(false));

        Ok(StoreServer {
            id,
            transport: Arc::new(transport),
            next_cmd_id: AtomicU64::new(1),
            seq_paxos,
            ble,
            sqlite_connection,
            query_result_notifier,
            halt,
        })
    }

    pub fn start_msg_event_loop(&self) {
        loop {
            if *self.halt.lock().unwrap() {
                break;
            }

            let mut seq_paxos = self.seq_paxos.lock().unwrap();
            let mut ble = self.ble.lock().unwrap();

            for out_msg in seq_paxos.get_outgoing_msgs() {
                self.transport.send_paxos_message(out_msg);
            }

            for out_ble_msg in ble.get_outgoing_msgs() {
                self.transport.send_ble_message(out_ble_msg);
            }

            sleep(Duration::from_millis(1));
        }
    }

    pub fn start_ble_event_loop(&self) {
        loop {
            if *self.halt.lock().unwrap() {
                break;
            }

            let mut seq_paxos = self.seq_paxos.lock().unwrap();
            let mut ble = self.ble.lock().unwrap();

            if let Some(leader) = ble.tick() {
                seq_paxos.handle_leader(leader);
            }

            std::mem::drop(seq_paxos);
            std::mem::drop(ble);
            sleep(Duration::from_millis(100));
        }
    }

    pub fn get_cluster_leader(&self) -> u64 {
        let seq_paxos = self.seq_paxos.lock().unwrap();
        seq_paxos.get_current_leader()
    }

    pub fn halt(&self, val: bool) {
        let mut halt = self.halt.lock().unwrap();
        *halt = val
    }

    pub async fn query<S: AsRef<str>>(
        &self,
        stmt: S,
        consistency: Consistency,
    ) -> Result<QueryResults, StoreError> {
        let consistency = if is_read_statement(stmt.as_ref()) {
            consistency
        } else {
            Consistency::Strong
        };

        let results = match consistency {
            Consistency::Strong => {
                let (notify, id) = {
                    let mut seq_paxos = self.seq_paxos.lock().unwrap();
                    let mut query_result_notifier = self.query_result_notifier.lock().unwrap();
                    let id = self.next_cmd_id.fetch_add(1, Ordering::SeqCst);
                    let cmd = StoreCommand {
                        id: id as usize,
                        sql: stmt.as_ref().to_string(),
                    };
                    let notify = Arc::new(Notify::new());
                    query_result_notifier.add_command(id, notify.clone());
                    seq_paxos.append(cmd).unwrap();
                    (notify, id)
                };

                notify.notified().await;
                let results = self
                    .query_result_notifier
                    .lock()
                    .unwrap()
                    .results
                    .remove(&id)
                    .unwrap();
                results?
            }

            Consistency::RelaxedReads => {
                let mut sqlite_connection = self.sqlite_connection.lock().unwrap();
                sqlite_connection.query(stmt.as_ref().to_string())?
            }
        };

        Ok(results)
    }

    pub fn recv_msg(&self, msg: messages::Message<StoreCommand, ()>) {
        let mut seq_paxos = self.seq_paxos.lock().unwrap();
        seq_paxos.handle(msg);
    }

    pub fn recv_ble_msg(&self, ble_msg: ble::messages::BLEMessage) {
        let mut ble = self.ble.lock().unwrap();
        ble.handle(ble_msg)
    }
}

fn is_read_statement(stmt: &str) -> bool {
    stmt.to_lowercase().starts_with("select")
}
