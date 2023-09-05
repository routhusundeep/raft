use std::sync::Arc;

use crate::{cluster::ProcessId, message::WireMessage, sender::Sender};
use chashmap::CHashMap;
use crossbeam::channel::{Receiver, TryRecvError};
use log::{debug, error, info};
use protobuf::Message;
use zmq::Socket;

pub struct ZMQPoller<S>
where
    S: Sender,
{
    context: zmq::Context,
    map: Arc<CHashMap<ProcessId, S>>,
    terminator: Receiver<String>,
}

impl<S> ZMQPoller<S>
where
    S: Sender,
{
    pub fn new(c: zmq::Context, terminator: Receiver<String>) -> ZMQPoller<S> {
        Self {
            context: c,
            map: Arc::new(CHashMap::new()),
            terminator: terminator,
        }
    }

    pub fn add(&self, id: ProcessId, s: S) {
        self.map.insert(id, s);
    }

    pub fn start(&self, addr: &str) {
        let server = self.context.socket(zmq::PULL).unwrap();
        let b = server.bind(addr);
        assert!(b.is_ok());

        loop {
            match self.terminator.try_recv() {
                Ok(_) | Err(TryRecvError::Disconnected) => {
                    info!("Terminating the poller");
                    break;
                }
                Err(TryRecvError::Empty) => {}
            }
            match Socket::recv_bytes(&server, 1) {
                Ok(b) => match crate::proto::messages::WireMessage::parse_from_bytes(&b) {
                    Ok(m) => self.handle(m.into()),
                    Err(e) => panic!("unexpected error while parsing {}", e),
                },
                Err(e) => match e {
                    zmq::Error::EAGAIN => {}
                    e => panic!("polling encountered error {}", e),
                },
            }
        }
    }

    fn handle(&self, m: WireMessage) -> () {
        let mclone = m.clone();
        debug!("polled message {}", &m);

        self.map
            .get(&m.to)
            .map(|s| s.send(m.from, m.to, m.message))
            .unwrap_or_else(|| {
                error!("handler not found for id: {}", &mclone.to);
                Ok(())
            })
            .unwrap_or_else(|e| {
                error!(
                    "error while handling the message: {}, by: {}, {}",
                    mclone.message, mclone.to, e
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, thread};

    use crate::message::ReceivedMessage;

    use super::*;

    #[test]
    fn terminate() {
        let context = zmq::Context::new();
        let (ts, tr) = crossbeam::channel::unbounded();
        let poller = ZMQPoller::new(context.clone(), tr);
        let pid = ProcessId::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6000, 1);
        let (s, r) = crossbeam::channel::unbounded();
        let addr = pid.addr();
        poller.add(pid.clone(), s);

        let jh = thread::spawn(move || {
            poller.start(&addr);
        });
        assert_eq!(false, jh.is_finished());
        assert_eq!(Ok(()), ts.send("terminate".to_string()));
        jh.join().unwrap()
    }

    #[test]
    fn poller() {
        let context = zmq::Context::new();
        let (ts, tr) = crossbeam::channel::unbounded();
        let poller = ZMQPoller::new(context.clone(), tr);
        let pid = ProcessId::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6666, 1);
        let (s, r) = crossbeam::channel::unbounded();

        let addr = pid.addr();
        poller.add(pid.clone(), s);

        let addr_clone = addr.clone();
        let jh = thread::spawn(move || {
            poller.start(&addr_clone);
        });

        let client = context.socket(zmq::PUSH).unwrap();
        assert!(client.connect(&addr).is_ok());

        let wired_message = WireMessage {
            from: pid.clone(),
            to: pid.clone(),
            message: crate::message::Message::Empty,
        };
        let m: crate::proto::messages::WireMessage = wired_message.clone().into();

        for _ in 1..1000 {
            assert_eq!(Ok(()), client.send(m.clone().write_to_bytes().unwrap(), 0));
        }

        for _ in 1..1000 {
            let exp = ReceivedMessage {
                from: wired_message.clone().from,
                message: wired_message.clone().message,
            };
            assert_eq!(exp, r.recv().unwrap())
        }

        assert_eq!(Ok(()), ts.send("terminate".to_string()));
        jh.join().unwrap()
    }
}
