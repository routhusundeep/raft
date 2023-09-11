use std::process::Command;

use crossbeam::channel::{unbounded, Receiver, Sender};

pub trait Committer {
    fn apply(&self, c: Command);
}

pub struct CommitLogger {}

impl CommitLogger{}

impl Committer for CommitLogger {}

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
        self.sender.send(c);
    }
}
