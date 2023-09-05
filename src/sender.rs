use std::fmt::Display;

use protobuf::Message as pbm;
use zmq::Context;

use crate::{
    cluster::ProcessId,
    message::{Message, WireMessage, ReceivedMessage},
};

pub enum Error {
    Closed,
    Unable(String),
    NotFound(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Closed => write!(f, "Closed"),
            Error::Unable(e) => write!(f, "Unable: {}", e),
            Error::NotFound(e) => write!(f, "Not Found: {}", e),
        }
    }
}

pub trait Sender {
    fn send(&self, from: ProcessId, to: ProcessId, m: Message) -> Result<(), Error>;
}

struct ZMQSender {
    context: Context,
}

impl Sender for ZMQSender {
    fn send(&self, from: ProcessId, to: ProcessId, m: Message) -> Result<(), Error> {
        let s = self.context.socket(zmq::PUSH).unwrap();
        assert!(s.connect(&to.addr()).is_ok());
        let p: crate::proto::messages::WireMessage = WireMessage {
            to: to,
            from: from,
            message: m,
        }
        .into();
        match s.send(p.write_to_bytes().unwrap(), 0) {
            Ok(()) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

impl Sender for crossbeam::channel::Sender<ReceivedMessage> {
    fn send(&self, from: ProcessId, to: ProcessId, m: Message) -> Result<(), Error> {
        match (&*self).send(ReceivedMessage{from: from, message: m}) {
            Ok(()) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

impl Into<Error> for zmq::Error {
    fn into(self) -> Error {
        Error::Unable(self.to_string())
    }
}

impl Into<Error> for crossbeam::channel::SendError<ReceivedMessage> {
    fn into(self) -> Error {
        Error::Closed
    }
}
