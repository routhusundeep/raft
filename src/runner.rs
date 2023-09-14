use std::{
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::channel::Receiver;
use log::{debug, info};

use crate::{
    cluster::ProcessId,
    committer::Committer,
    message::{Message, ReceivedMessage},
    raft::Raft,
    sender::Sender,
    storage::Storage,
};

#[derive(Eq, PartialEq, Debug)]
enum BreakCond {
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

    fn run(&mut self, cond: BreakCond, sleep_on_empty: Duration) {
        loop {
            match self.r.try_recv() {
                Ok(m) => self.raft.process(m.from, m.message),
                Err(e) => match e {
                    crossbeam::channel::TryRecvError::Empty => {
                        if cond == BreakCond::TillEmpty {
                            debug!("breaking the runner since there are no message");
                            break;
                        } else {
                            self.raft.process(self.pid.clone(), Message::Empty);
                            if !sleep_on_empty.is_zero() {
                                thread::sleep(sleep_on_empty);
                            }
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
        self.run(BreakCond::Forever, Duration::ZERO)
    }

    pub fn start_with_sleep(&mut self, d: Duration) {
        self.run(BreakCond::Forever, d)
    }

    pub fn run_pending(&mut self) {
        self.run(BreakCond::TillEmpty, Duration::ZERO)
    }

    pub fn tick(&mut self) {
        self.raft.process(self.pid.clone(), Message::Empty)
    }
}
