use std::fmt::Display;

use protobuf::MessageField;

use crate::{cluster::ProcessId, proto::messages as proto};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReceivedMessage {
    pub from: ProcessId,
    pub message: Message,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Message {
    Empty,
    Terminate,
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Empty => write!(f, "None"),
            Message::Terminate => write!(f, "Terminate"),
        }
    }
}

impl Into<proto::Message> for Message {
    fn into(self) -> proto::Message {
        match self {
            Message::Empty => {
                let mut m = proto::Message::default();
                m.type_ = proto::MessageType::EMPTY.into();
                m
            }
            Message::Terminate => {
                let mut m = proto::Message::default();
                m.type_ = proto::MessageType::Terminate.into();
                m
            }
        }
    }
}

impl From<proto::Message> for Message {
    fn from(value: proto::Message) -> Self {
        match value.type_.enum_value() {
            Ok(v) => match v {
                proto::MessageType::EMPTY => Message::Empty,
                proto::MessageType::Terminate => Message::Terminate,
            },
            Err(_) => unreachable!("should always be present"),
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

#[derive(Clone, PartialEq, Eq)]
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
