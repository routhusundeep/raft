use bytes::Bytes;

use crate::{
    basic::{Entry, EntryType, Index, Term},
    cluster::ProcessId,
};

pub trait Storage {
    fn get_vote(&self) -> Option<ProcessId>;
    fn set_vote(&mut self, id: Option<ProcessId>);

    fn get_term(&self) -> Term;
    fn set_term(&mut self, t: Term);

    fn last_index(&self) -> Index;
    fn read(&self, from: Index, to: Index) -> Vec<Entry>;
    fn read_from(&self, from: Index) -> Vec<Entry>;
    fn at(&self, idx: Index) -> Option<Entry>;
    fn append(&mut self, term: Term, b: Bytes) -> Index;
    fn update(&mut self, from: Index, v: Vec<Entry>);
}

pub struct MemStorage {
    term: Term,
    vote: Option<ProcessId>,
    log: Vec<Entry>,
}
impl MemStorage {
    pub fn new() -> MemStorage {
        MemStorage {
            term: 0,
            vote: None,
            log: vec![
                // initializing with an empty entry
                Entry {
                    term: 0,
                    index: 0,
                    t: EntryType::Normal(Bytes::new()),
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

    fn read(&self, from: Index, to: Index) -> Vec<Entry> {
        self.log[from..to].to_vec()
    }

    fn read_from(&self, from: Index) -> Vec<Entry> {
        self.log[from..].to_vec()
    }

    fn at(&self, idx: Index) -> Option<Entry> {
        self.log.get(idx).map(|e| e.clone())
    }

    fn append(&mut self, term: Term, b: Bytes) -> Index {
        let idx = self.log.len() - 1;
        let e = Entry {
            term: term,
            index: idx,
            t: EntryType::Normal(b),
        };
        self.log.push(e);
        idx
    }

    fn update(&mut self, from: Index, v: Vec<Entry>) {
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
