use bytes::Bytes;

pub type Term = usize;
pub type Index = usize;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Entry {
    Normal(Index, Term, Bytes),
}
