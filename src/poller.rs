use std::sync::Arc;

use crate::{basic::Term, cluster::ProcessId, message::WireMessage, sender::Sender};
use chashmap::CHashMap;
use crossbeam::channel::{Receiver, TryRecvError};
use log::{debug, error, info, trace};
use protobuf::Message;
use zmq::Socket;

#[derive(PartialEq, Eq)]
enum TerminateCond {
    Never,
    Empty,
    Count(usize),
}

pub struct ZMQPoller<S>
where
    S: Sender,
{
    server: Socket,
    addr: String,
    map: Arc<CHashMap<ProcessId, S>>,
    terminator: Receiver<String>,
}

impl<S> ZMQPoller<S>
where
    S: Sender,
{
    pub fn new(context: zmq::Context, addr: &str, terminator: Receiver<String>) -> ZMQPoller<S> {
        let server = context.socket(zmq::PULL).unwrap();
        let b = server.bind(addr);
        assert!(b.is_ok());

        Self {
            server: server,
            addr: addr.into(),
            map: Arc::new(CHashMap::new()),
            terminator: terminator,
        }
    }

    pub fn addr(&self) -> String {
        self.addr.clone()
    }

    pub fn add(&self, id: ProcessId, s: S) {
        self.map.insert(id, s);
    }

    pub fn start(&self) {
        self.exec(TerminateCond::Never)
    }

    pub fn empty_queue(&self) {
        self.exec(TerminateCond::Empty)
    }

    pub fn run(&self, count: usize) {
        self.exec(TerminateCond::Count(count))
    }

    fn exec(&self, cond: TerminateCond) {
        let mut count = 0;
        loop {
            match self.terminator.try_recv() {
                Ok(m) => {
                    info!("Terminating the poller: {}", m);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    info!("Terminating the poller as it is disconnected");
                    break;
                }
                Err(TryRecvError::Empty) => {
                    if cond == TerminateCond::Empty {
                        debug!("terminated since the channel is empty");
                        break;
                    }
                }
            }
            match Socket::recv_bytes(&self.server, 1) {
                Ok(b) => match crate::proto::messages::WireMessage::parse_from_bytes(&b) {
                    Ok(m) => {
                        match cond {
                            TerminateCond::Count(_) => count += 1,
                            _ => {}
                        }
                        self.handle(m.into())
                    }
                    Err(e) => panic!("unexpected error while parsing {}", e),
                },
                Err(e) => match e {
                    zmq::Error::EAGAIN => {}
                    e => panic!("polling encountered error {}", e),
                },
            }
            if TerminateCond::Count(count) == cond {
                break;
            }
        }
    }

    fn handle(&self, m: WireMessage) -> () {
        let mclone = m.clone();
        trace!("polled message {}", &m);

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

    use crate::{message::ReceivedMessage, sender::ZMQSender};

    use super::*;

    #[test]
    fn terminate() {
        let pid = ProcessId::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6000, 1);
        let context = zmq::Context::new();
        let (ts, tr) = crossbeam::channel::unbounded();
        let poller = ZMQPoller::new(context.clone(), &pid.addr(), tr);
        let (s, r) = crossbeam::channel::unbounded();

        poller.add(pid.clone(), s);

        let jh = thread::spawn(move || {
            poller.start();
        });
        assert_eq!(false, jh.is_finished());
        assert_eq!(Ok(()), ts.send("terminate".to_string()));
        jh.join().unwrap()
    }

    #[test]
    fn poller() {
        let pid = ProcessId::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6666, 1);
        let context = zmq::Context::new();
        let (ts, tr) = crossbeam::channel::unbounded();
        let poller = ZMQPoller::new(context.clone(), &pid.addr(), tr);
        let (s, r) = crossbeam::channel::unbounded();

        let addr = pid.addr();
        poller.add(pid.clone(), s);

        let jh = thread::spawn(move || {
            poller.start();
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

    #[test]
    fn count() {
        let pid = ProcessId::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6001, 1);
        let context = zmq::Context::new();
        let (ts, tr) = crossbeam::channel::unbounded();
        let poller = ZMQPoller::new(context.clone(), &pid.addr(), tr);
        let (s, r) = crossbeam::channel::unbounded();

        let addr = pid.addr();
        poller.add(pid.clone(), s);

        let client = context.socket(zmq::PUSH).unwrap();
        assert!(client.connect(&addr).is_ok());

        let wired_message = WireMessage {
            from: pid.clone(),
            to: pid.clone(),
            message: crate::message::Message::Empty,
        };
        let m: crate::proto::messages::WireMessage = wired_message.clone().into();

        for _ in 1..1001 {
            assert_eq!(Ok(()), client.send(m.clone().write_to_bytes().unwrap(), 0));
        }

        poller.run(1000);

        for _ in 1..1001 {
            let exp = ReceivedMessage {
                from: wired_message.clone().from,
                message: wired_message.clone().message,
            };
            assert_eq!(exp, r.recv().unwrap())
        }

        assert_eq!(Ok(()), ts.send("terminate".to_string()));
    }

    #[test]
    fn poller_with_sender() {
        let pid = ProcessId::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6002, 1);
        let context = zmq::Context::new();
        let (ts, tr) = crossbeam::channel::unbounded();
        let poller = ZMQPoller::new(context.clone(), &pid.addr(), tr);
        let (s, r) = crossbeam::channel::unbounded();

        poller.add(pid.clone(), s);

        let sender = ZMQSender::new(context);
        let from = ProcessId::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6666, 2);

        poller.run(1);

        assert_eq!(
            Ok(()),
            sender.send(from.clone(), pid.clone(), crate::message::Message::Empty)
        );

        let exp = ReceivedMessage {
            from: from.clone(),
            message: crate::message::Message::Empty,
        };
        assert_eq!(exp, r.recv().unwrap());
    }
}
