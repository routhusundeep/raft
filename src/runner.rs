use crossbeam::channel::Receiver;
use log::info;

use crate::{
    cluster::ProcessId,
    message::{Message, WireMessage},
    raft::Raft,
    sender::Sender,
    storage::Storage,
};

struct Runner {
    pid: ProcessId,
    r: Receiver<WireMessage>,
}

impl Runner {
    pub fn run<S: Storage, E: Sender>(self, mut raft: Raft<S, E>) {
        loop {
            match self.r.try_recv() {
                Ok(m) => raft.process(m.from, m.message),
                Err(e) => match e {
                    crossbeam::channel::TryRecvError::Empty => {
                        raft.process(self.pid.clone(), Message::Empty)
                    }
                    crossbeam::channel::TryRecvError::Disconnected => {
                        info!("stopping the runner since the channel is disconnected");
                        break;
                    }
                },
            }
        }
    }
}
