use crate::{rpc::RpcTransport, SequencePaxosStoreTransport, Store, StoreCommand};
use crossbeam_channel::Receiver;
use omnipaxos_core::{
    ballot_leader_election as ble, messages,
    sequence_paxos::{SequencePaxos, SequencePaxosConfig},
    storage::{Entry, Snapshot, Storage},
};
use std::sync::{Arc, Mutex};
use std::{thread::sleep, time::Duration};

pub struct SequencePaxosReplica {
    id: u64,
    peer_ids: Vec<u64>,
    hb_delay: u64,
    // store: Arc<Mutex<Store<T, S>>>,
    seq_paxos: Arc<Mutex<SequencePaxos<StoreCommand, (), Store<()>>>>,
    ble: Arc<Mutex<ble::BallotLeaderElection>>,
    halt: Arc<Mutex<bool>>,
}

impl SequencePaxosReplica {
    pub fn new(
        id: u64,
        peer_ids: Vec<u64>,
        hb_delay: u64,
        config_id: u32,
        store: Store<()>,
    ) -> Self {
        let mut sp_config = SequencePaxosConfig::default();
        sp_config.set_configuration_id(config_id);
        sp_config.set_pid(id);
        sp_config.set_peers(peer_ids);

        let mut ble_config = ble::BLEConfig::default();
        ble_config.set_pid(id);
        ble_config.set_peers(peer_ids);
        ble_config.set_hb_delay(hb_delay);

        let mut seq_paxos = Arc::new(Mutex::new(SequencePaxos::with(sp_config, store)));
        let mut ble = Arc::new(Mutex::new(ble::BallotLeaderElection::with(ble_config)));
        let mut halt = Arc::new(Mutex::new(false));

        Self {
            id,
            peer_ids,
            hb_delay,
            seq_paxos,
            ble,
            halt,
        }
    }

    pub fn start(
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
            let mut transport = self.transport.lock().unwrap();

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

            for out_msg in seq_paxos.get_outgoing_msgs() {}
        }
    }

    fn halt(&self, val: bool) {
        let mut halt = self.halt.lock().unwrap();
        *halt = val;
    }
}
