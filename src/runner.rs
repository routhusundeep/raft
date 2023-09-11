use crossbeam::channel::Receiver;
use log::{debug, info};

use crate::{
    cluster::ProcessId,
    committer::Committer,
    message::{Message, ReceivedMessage, WireMessage},
    raft::Raft,
    sender::Sender,
    storage::Storage,
};

#[derive(Eq, PartialEq, Debug)]
enum Options {
    TillEmpty,
    Forever,
}

pub struct Runner<S: Storage, E: Sender, C: Committer> {
    pid: ProcessId,
    r: Receiver<ReceivedMessage>,
    raft: Raft<S, E, C>,
}

impl<S: Storage, E: Sender, C: Committer> Runner<S, E, C> {
    pub fn new(r: Receiver<ReceivedMessage>, raft: Raft<S, E, C>) -> Self {
        Self {
            pid: raft.id().clone(),
            r: r,
            raft: raft,
        }
    }

    pub fn raft(&self) -> &Raft<S, E, C> {
        &self.raft
    }

    fn run(&mut self, opt: Options) {
        loop {
            match self.r.try_recv() {
                Ok(m) => self.raft.process(m.from, m.message),
                Err(e) => match e {
                    crossbeam::channel::TryRecvError::Empty => {
                        if opt == Options::TillEmpty {
                            debug!("breaking the runner since there are no message");
                            break;
                        } else {
                            self.raft.process(self.pid.clone(), Message::Empty);
                        }
                    }
                    crossbeam::channel::TryRecvError::Disconnected => {
                        info!("stopping the runner since the channel is disconnected");
                        break;
                    }
                },
            }
        }
    }

    pub fn start(&mut self) {
        self.run(Options::Forever)
    }

    pub fn run_pending(&mut self) {
        self.run(Options::TillEmpty)
    }

    pub fn tick(&mut self) {
        self.raft.process(self.pid.clone(), Message::Empty)
    }
}
