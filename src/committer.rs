use crossbeam::channel::{unbounded, Receiver, Sender};
use log::info;

use crate::basic::Command;

pub trait Committer {
    fn apply(&self, c: Command);
}

pub struct CommitLogger {}

impl CommitLogger {
    pub(crate) fn new() -> CommitLogger {
        CommitLogger {}
    }
}

impl Committer for CommitLogger {
    fn apply(&self, c: Command) {
        info!("applying command: {}", c)
    }
}

pub struct AsyncCommitter {
    sender: Sender<Command>,
}

impl AsyncCommitter {
    pub fn new() -> (Self, Receiver<Command>) {
        let (sx, rx) = unbounded();
        (Self { sender: sx }, rx)
    }
}

impl Committer for AsyncCommitter {
    fn apply(&self, c: Command) {
        let _ = self.sender.send(c);
    }
}
