use std::fmt::Display;

use bytes::Bytes;

pub type Term = usize;
pub type Index = usize;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Entry {
    pub index: Index,
    pub term: Term,
    pub t: EntryType,
}
impl Entry {
    pub(crate) fn normal(index: usize, term: usize, bytes: Bytes) -> Entry {
        Entry {
            index: index,
            term: term,
            t: EntryType::Normal(bytes),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum EntryType {
    Normal(Bytes),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Command {
    Normal(Bytes),
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Normal(b) => write!(
                f,
                "Normal:{}",
                String::from_utf8(b.to_vec()).unwrap_or("not valid UTF-8".into())
            ),
        }
    }
}

impl From<String> for Command {
    fn from(value: String) -> Self {
        Self::Normal(value.into())
    }
}
