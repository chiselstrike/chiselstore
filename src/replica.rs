use crate::{SequencePaxosStoreTransport, Store, StoreCommand};
use crossbeam_channel::Receiver;
use derivative::Derivative;
use omnipaxos_core::{
    ballot_leader_election as ble, messages,
    sequence_paxos::{SequencePaxos, SequencePaxosConfig},
};
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SequencePaxosReplica<T: SequencePaxosStoreTransport + Send + Sync> {
    id: u64,
    hb_delay: u64,
    transport: Arc<T>,
    #[derivative(Debug = "ignore")]
    seq_paxos: Arc<Mutex<SequencePaxos<StoreCommand, (), Store<()>>>>,
    #[derivative(Debug = "ignore")]
    ble: Arc<Mutex<ble::BallotLeaderElection>>,
    halt: Arc<Mutex<bool>>,
}

impl<T: SequencePaxosStoreTransport + Send + Sync> SequencePaxosReplica<T> {
    pub fn new(
        id: u64,
        peer_ids: Vec<u64>,
        hb_delay: u64,
        transport: T,
        config_id: u32,
        store: Store<()>,
    ) -> Self {
        let mut sp_config = SequencePaxosConfig::default();
        sp_config.set_configuration_id(config_id);
        sp_config.set_pid(id);
        sp_config.set_peers(peer_ids.clone());

        let mut ble_config = ble::BLEConfig::default();
        ble_config.set_pid(id);
        ble_config.set_peers(peer_ids);
        ble_config.set_hb_delay(hb_delay);

        let seq_paxos = Arc::new(Mutex::new(SequencePaxos::with(sp_config, store)));
        let ble = Arc::new(Mutex::new(ble::BallotLeaderElection::with(ble_config)));
        let halt = Arc::new(Mutex::new(false));

        Self {
            id,
            hb_delay,
            transport: Arc::new(transport),
            seq_paxos,
            ble,
            halt,
        }
    }

    pub async fn start(
        &mut self,
        recv_msg: Receiver<messages::Message<StoreCommand, ()>>,
        recv_transition: Receiver<StoreCommand>,
        recv_ballot: Receiver<ble::messages::BLEMessage>,
    ) {
        loop {
            if *self.halt.lock().unwrap() {
                return;
            }

            let mut seq_paxos = self.seq_paxos.lock().unwrap();
            let mut ble = self.ble.lock().unwrap();

            if let Some(leader) = ble.tick() {
                seq_paxos.handle_leader(leader);
            }

            match recv_msg.try_recv() {
                Ok(msg) => seq_paxos.handle(msg),
                _ => {}
            }

            match recv_transition.try_recv() {
                Ok(trans) => seq_paxos.append(trans).unwrap(),
                _ => {}
            }

            match recv_ballot.try_recv() {
                Ok(ble_msg) => ble.handle(ble_msg),
                _ => {}
            }

            for out_msg in seq_paxos.get_outgoing_msgs() {
                self.transport.send_paxos_message(out_msg);
            }

            for out_ble_msg in ble.get_outgoing_msgs() {
                self.transport.send_ble_message(out_ble_msg);
            }

            sleep(Duration::from_millis(1)).await;
        }
    }

    pub async fn start_ble_event_loop(&mut self, recv_ballot: Receiver<ble::messages::BLEMessage>) {
        loop {
            if *self.halt.lock().unwrap() {
                return;
            }

            let mut seq_paxos = self.seq_paxos.lock().unwrap();
            let mut ble = self.ble.lock().unwrap();

            if let Some(leader) = ble.tick() {
                seq_paxos.handle_leader(leader);
            }

            match recv_ballot.try_recv() {
                Ok(ble_msg) => ble.handle(ble_msg),
                _ => {}
            }

            for out_ble_msg in ble.get_outgoing_msgs() {
                self.transport.send_ble_message(out_ble_msg);
            }

            sleep(Duration::from_millis(1)).await;
        }
    }

    pub async fn start_paxos_event_loop(
        &mut self,
        recv_msg: Receiver<messages::Message<StoreCommand, ()>>,
        recv_transition: Receiver<StoreCommand>,
    ) {
        loop {
            if *self.halt.lock().unwrap() {
                return;
            }

            let mut seq_paxos = self.seq_paxos.lock().unwrap();

            match recv_msg.try_recv() {
                Ok(msg) => seq_paxos.handle(msg),
                _ => {}
            }

            match recv_transition.try_recv() {
                Ok(trans) => seq_paxos.append(trans).unwrap(),
                _ => {}
            }

            for out_msg in seq_paxos.get_outgoing_msgs() {
                self.transport.send_paxos_message(out_msg);
            }

            sleep(Duration::from_millis(1)).await;
        }
    }

    pub fn halt(&self, val: bool) {
        let mut halt = self.halt.lock().unwrap();
        *halt = val;
    }
}