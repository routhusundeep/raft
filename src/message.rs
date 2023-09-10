use std::{fmt::Display, vec};

use bytes::Bytes;
use protobuf::MessageField;

use crate::{
    basic::{Entry, Index, Term},
    cluster::ProcessId,
    proto::messages as proto,
};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReceivedMessage {
    pub from: ProcessId,
    pub message: Message,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Message {
    Empty,
    Terminate,
    Command(ProcessId, Bytes),
    AppendEntries(
        Term,       // leaders term
        Index,      // index of the previous log immediately preceeding the new one
        Term,       // term of the previous log immediately preceeding the new one
        Vec<Entry>, // log entries to store(empty for heartbeat)
        Index,      // leader commit index
    ),
    AppendEntriesResponse(Index, Term, bool),
    RequestVote(
        Term,  // requested for term
        Index, // last log index in the candidate
        Term,  // last log term in the candidate
    ),
    RequestVoteResponse(Term, bool),
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Empty => write!(f, "None"),
            Message::Terminate => write!(f, "Terminate"),
            Message::AppendEntries(term, prev_index, prev_term, entries, commit_index) => {
                write!(
                    f,
                    "AppendEntries ({},  {}, {}, {})",
                    term, prev_index, prev_term, commit_index
                )
            }
            Message::RequestVote(term, prev_index, prev_term) => {
                write!(f, "RequestVote ({},  {}, {})", term, prev_index, prev_term)
            }
            Message::AppendEntriesResponse(index, term, success) => {
                write!(
                    f,
                    "AppendEntriesResponse ({}, {}, {})",
                    index, term, success
                )
            }
            Message::RequestVoteResponse(term, granted) => {
                write!(f, "RequestVoteResponse ({}, {})", term, granted)
            }
            Message::Command(client, b) => write!(
                f,
                "Command ({}, {})",
                client,
                String::from_utf8(b.to_vec()).unwrap_or("not valid UTF-8".into())
            ),
        }
    }
}

impl Into<proto::Message> for Message {
    fn into(self) -> proto::Message {
        match self {
            Message::Empty => {
                let mut m = proto::Message::default();
                m.type_ = proto::MessageType::Empty.into();
                m
            }
            Message::Terminate => {
                let mut m = proto::Message::default();
                m.type_ = proto::MessageType::Terminate.into();
                m
            }
            Message::AppendEntries(term, prev_index, prev_term, entries, commit_index) => {
                let mut m = proto::Message::default();
                m.type_ = proto::MessageType::AppendEntries.into();
                m.term = term.try_into().unwrap();
                m.prevIndex = prev_index.try_into().unwrap();
                m.prevTerm = prev_term.try_into().unwrap();
                m.entries = convert_to_vec(entries);
                m.commitIndex = commit_index.try_into().unwrap();
                m
            }
            Message::AppendEntriesResponse(index, term, success) => {
                let mut m = proto::Message::default();
                m.type_ = proto::MessageType::AppendEntriesResponse.into();
                m.prevIndex = index.try_into().unwrap();
                m.term = term.try_into().unwrap();
                m.success = success;
                m
            }
            Message::RequestVote(term, prev_index, prev_term) => {
                let mut m = proto::Message::default();
                m.type_ = proto::MessageType::RequestVote.into();
                m.term = term.try_into().unwrap();
                m.prevIndex = prev_index.try_into().unwrap();
                m.prevTerm = prev_term.try_into().unwrap();
                m
            }
            Message::RequestVoteResponse(term, granted) => {
                let mut m = proto::Message::default();
                m.type_ = proto::MessageType::RequestVoteResponse.into();
                m.term = term.try_into().unwrap();
                m.success = granted;
                m
            }
            Message::Command(client, c) => {
                let mut m = proto::Message::default();
                m.command = c.into();
                m.client = MessageField::some(client.into());
                m
            }
        }
    }
}

fn convert_to_vec(entries: Vec<Entry>) -> Vec<proto::Entry> {
    let mut v = vec![];
    for e in entries {
        v.push(e.into());
    }
    v
}

fn convert_from_vec(entries: Vec<proto::Entry>) -> Vec<Entry> {
    let mut v = vec![];
    for e in entries {
        v.push(e.into());
    }
    v
}

impl From<proto::Message> for Message {
    fn from(value: proto::Message) -> Self {
        match value.type_.enum_value() {
            Ok(v) => match v {
                proto::MessageType::Empty => Message::Empty,
                proto::MessageType::Terminate => Message::Terminate,
                proto::MessageType::AppendEntries => Message::AppendEntries(
                    value.term.try_into().unwrap(),
                    value.prevIndex.try_into().unwrap(),
                    value.prevTerm.try_into().unwrap(),
                    convert_from_vec(value.entries),
                    value.commitIndex.try_into().unwrap(),
                ),
                proto::MessageType::AppendEntriesResponse => Message::AppendEntriesResponse(
                    value.prevIndex.try_into().unwrap(),
                    value.term.try_into().unwrap(),
                    value.success,
                ),
                proto::MessageType::RequestVote => Message::RequestVote(
                    value.term.try_into().unwrap(),
                    value.prevIndex.try_into().unwrap(),
                    value.prevTerm.try_into().unwrap(),
                ),
                proto::MessageType::RequestVoteResponse => {
                    Message::RequestVoteResponse(value.term.try_into().unwrap(), value.success)
                }
                proto::MessageType::Command => {
                    Message::Command(value.client.unwrap().into(), value.command.into())
                }
            },
            Err(_) => unreachable!("should always be present"),
        }
    }
}

impl Into<proto::Entry> for Entry {
    fn into(self) -> proto::Entry {
        let mut def = proto::Entry::default();
        match self {
            Entry::Normal(index, term, bytes) => {
                let mut def = proto::Entry::default();
                def.term = term.try_into().unwrap();
                def.index = index.try_into().unwrap();
                def.bytes = bytes.into();
                def
            }
        }
    }
}

impl From<proto::Entry> for Entry {
    fn from(value: proto::Entry) -> Self {
        match value.type_.enum_value() {
            Ok(v) => match v {
                proto::EntryType::Normal => Entry::Normal(
                    value.index.try_into().unwrap(),
                    value.term.try_into().unwrap(),
                    value.bytes.into(),
                ),
            },
            Err(_) => panic!("not possible"),
        }
    }
}

impl Into<proto::ProcessId> for ProcessId {
    fn into(self) -> proto::ProcessId {
        let mut def = proto::ProcessId::default();
        def.ip = match self.ip {
            std::net::IpAddr::V4(v4) => Option::Some(proto::process_id::Ip::V4(v4.into())),
            std::net::IpAddr::V6(_) => todo!("ip v6 is not supported"),
        };
        def.port = self.port;
        def.id = self.id;
        def
    }
}

impl From<proto::ProcessId> for ProcessId {
    fn from(value: proto::ProcessId) -> Self {
        Self {
            ip: match value.ip {
                Some(proto::process_id::Ip::V4(v)) => std::net::IpAddr::V4(v.into()),
                _ => unreachable!("ip v6 is not supported"),
            },
            port: value.port,
            id: value.id,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WireMessage {
    pub from: ProcessId,
    pub to: ProcessId,
    pub message: Message,
}

impl Display for WireMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "from:{}, to:{}, message:{}",
            self.from, self.to, self.message
        )
    }
}

impl Into<proto::WireMessage> for WireMessage {
    fn into(self) -> proto::WireMessage {
        let mut def = proto::WireMessage::default();
        def.to = MessageField::some(self.to.into());
        def.from = MessageField::some(self.from.into());
        def.message = MessageField::some(self.message.into());
        def
    }
}

impl From<proto::WireMessage> for WireMessage {
    fn from(value: proto::WireMessage) -> Self {
        Self {
            to: ProcessId::from(value.to.unwrap()),
            from: ProcessId::from(value.from.unwrap()),
            message: Message::from(value.message.unwrap()),
        }
    }
}
