use bytes::Bytes;

use crate::{
    basic::{Index, Term},
    cluster::ProcessId,
};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LogEntry {
    pub term: Term,
    pub index: Index,
    pub bytes: Bytes,
}

pub trait Storage {
    fn get_vote(&self) -> Option<ProcessId>;
    fn set_vote(&mut self, id: Option<ProcessId>);

    fn get_term(&self) -> Term;
    fn set_term(&mut self, t: Term);

    fn last_index(&self) -> Index;
    fn read(&self, from: Index, to: Index) -> Vec<LogEntry>;
    fn read_from(&self, from: Index) -> Vec<LogEntry>;
    fn at(&self, idx: Index) -> Option<LogEntry>;
    fn append(&mut self, term: Term, b: Bytes) -> Index;
    fn update(&mut self, from: Index, v: Vec<LogEntry>);
}

pub struct MemStorage {
    term: Term,
    vote: Option<ProcessId>,
    log: Vec<LogEntry>,
}
impl MemStorage {
    pub(crate) fn new() -> MemStorage {
        MemStorage {
            term: 0,
            vote: None,
            log: vec![
                // initializing with an empty entry
                LogEntry {
                    term: 0,
                    index: 0,
                    bytes: Bytes::new(),
                },
            ],
        }
    }
}

impl Storage for MemStorage {
    fn get_vote(&self) -> Option<ProcessId> {
        self.vote.clone()
    }

    fn set_vote(&mut self, id: Option<ProcessId>) {
        self.vote = id
    }

    fn get_term(&self) -> Term {
        self.term
    }

    fn set_term(&mut self, t: Term) {
        self.term = t
    }

    fn last_index(&self) -> Index {
        self.log.len() - 1
    }

    fn read(&self, from: Index, to: Index) -> Vec<LogEntry> {
        self.log[from..to].to_vec()
    }

    fn read_from(&self, from: Index) -> Vec<LogEntry> {
        self.log[from..].to_vec()
    }

    fn at(&self, idx: Index) -> Option<LogEntry> {
        self.log.get(idx).map(|e| e.clone())
    }

    fn append(&mut self, term: Term, b: Bytes) -> Index {
        let idx = self.log.len() - 1;
        let e = LogEntry {
            term,
            index: idx,
            bytes: b,
        };
        self.log.push(e);
        idx
    }

    fn update(&mut self, from: Index, v: Vec<LogEntry>) {
        for i in from..from + v.len() {
            let e = v[i - from].clone();
            if i < self.log.len() {
                if self.log[i].term != e.term {
                    self.log.drain(i..);
                    self.log.push(e);
                }
            } else {
                self.log.push(e);
            }
        }
    }
}
