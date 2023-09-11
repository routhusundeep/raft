use crate::{
    basic::{Entry, Index, Term},
    cluster::ProcessId,
    storage::Storage,
};

pub struct StoredState<S: Storage> {
    storage: S,
}

impl<S: Storage> StoredState<S> {
    pub fn new(storage: S) -> StoredState<S> {
        StoredState { storage: storage }
    }

    pub fn insert_after(&mut self, index: Index, entries: Vec<Entry>) {
        self.storage.update(index, entries);
    }

    pub fn at(&self, index: Index) -> std::option::Option<Entry> {
        self.storage.at(index)
    }

    pub fn get_vote(&self) -> Option<ProcessId> {
        self.storage.get_vote()
    }

    pub fn last_index(&self) -> Index {
        self.storage.last_index()
    }

    pub fn last_index_and_term(&self) -> (Index, Term) {
        let last_index = self.storage.last_index();
        (
            last_index,
            self.storage
                .at(last_index)
                .map(|e| e.term)
                .expect("will be present"),
        )
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

    pub fn entries_from(&self, index: Index) -> Vec<Entry> {
        self.storage.read_from(index)
    }
}
