//! ChiselStore server module.

use crate::errors::StoreError;
use async_notify::Notify;
use async_trait::async_trait;
use crossbeam_channel as channel;
use crossbeam_channel::{Receiver, Sender};
use derivative::Derivative;
use omnipaxos_core::ballot_leader_election::Ballot;
use omnipaxos_core::storage::Storage;
use omnipaxos_core::{
    ballot_leader_election as ble, messages,
    storage::{Entry, Snapshot, StopSignEntry},
};
use sqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    conn_pool: Vec<Arc<Mutex<Connection>>>,
    conn_idx: usize,
    cmnd_completion: HashMap<u64, Arc<Notify>>,
    results: HashMap<u64, Result<QueryResults, StoreError>>,
}

impl<S: Snapshot<StoreCommand>> Store<S> {
    pub fn new(this_id: u64, config: StoreConfig) -> Self {
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
            log: Vec::new(),
            n_prom: ble::Ballot::default(),
            acc_round: ble::Ballot::default(),
            ld: 0,
            trimmed_idx: 0,
            snapshot: None,
            stopsign: None,
            conn_pool,
            conn_idx: 0,
            cmnd_completion: HashMap::new(),
            results: HashMap::new(),
        }
    }

    pub fn get_connection(&mut self) -> Arc<Mutex<Connection>> {
        let idx = self.conn_idx % self.conn_pool.len();
        let conn = &self.conn_pool[idx];
        self.conn_idx += 1;
        conn.clone()
    }

    pub fn apply_queries(&mut self, transition: StoreCommand) {
        let conn = self.get_connection();
        let results = query(conn, transition.sql);
        self.results.insert(transition.id as u64, results);

        if let Some(completion) = self.cmnd_completion.remove(&(transition.id as u64)) {
            completion.notify();
        }
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
        decided_entries
            .into_iter()
            .for_each(|entry| self.apply_queries(*entry));
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

pub struct StoreServer {
    next_cmd_id: AtomicU64,
    store: Arc<Mutex<Store<()>>>,
    msg_notifier_rx: Receiver<messages::Message<StoreCommand, ()>>,
    msg_notifier_tx: Sender<messages::Message<StoreCommand, ()>>,
    ble_notifier_rx: Receiver<messages::Message<ble::messages::BLEMessage, ()>>,
    ble_notifier_tx: Sender<messages::Message<ble::messages::BLEMessage, ()>>,
    trans_notifier_rx: Receiver<StoreCommand>,
    trans_notifier_tx: Sender<StoreCommand>,
}
