use crate::{
    basic::{Index, Term},
    cluster::ProcessId,
    storage::{LogEntry, Storage},
};

pub struct StoredState<S: Storage> {
    storage: S,
}

impl<S: Storage> StoredState<S> {
    pub fn insert_after(&mut self, index: Index, entries: Vec<LogEntry>) {
        self.storage.update(index, entries);
    }

    pub fn at(&self, index: Index) -> std::option::Option<LogEntry> {
        self.storage.at(index)
    }

    pub fn get_vote(&self) -> Option<ProcessId> {
        self.storage.get_vote()
    }

    pub fn last_index(&self) -> Index {
        self.storage.last_index()
    }

    pub fn last_index_and_term(&self) -> (Index, Term) {
        (self.storage.last_index(), self.storage.get_term())
    }

    pub fn vote_for(&mut self, id: ProcessId) {
        self.storage.set_vote(Some(id))
    }

    pub fn current_term(&self) -> Term {
        self.storage.get_term()
    }

    pub fn increase_term(&mut self) {
        self.storage.set_term(self.storage.get_term() + 1)
    }

    pub fn append(&mut self, b: bytes::Bytes) -> (Index, Term) {
        let term = self.storage.get_term();
        let index = self.storage.append(term, b);
        (index, term)
    }

    pub fn set_term(&mut self, term: usize) {
        self.storage.set_term(term)
    }

    pub fn not_voted(&mut self) {
        self.storage.set_vote(None)
    }

    pub fn entries_from(&self, index: Index) -> Vec<LogEntry> {
        self.storage.read_from(index)
    }
}
