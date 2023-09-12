use super::committer::AsyncCommitter;
use super::message::ReceivedMessage;
use super::raft::Raft;
use super::runner::Runner;
use super::{basic::Command, poller::ZMQPoller};
use super::{
    cluster::{Cluster, ProcessId},
    sender::ZMQSender,
    storage::MemStorage,
};
use crossbeam::channel::{Receiver, Sender};
use std::net::Ipv4Addr;
use zmq::Context;

fn new_process(i: u32) -> ProcessId {
    ProcessId::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5555, i)
}

fn test_cluster() -> (ProcessId, ProcessId, ProcessId, Cluster) {
    let p1 = new_process(1);
    let p2 = new_process(2);
    let p3 = new_process(3);

    let cluster = Cluster::new();
    cluster.add_all(vec![p1.clone(), p2.clone(), p3.clone()]);
    (p1, p2, p3, cluster)
}

fn new_raft(
    id: ProcessId,
    cluster: Cluster,
    context: Context,
) -> (
    Raft<MemStorage, ZMQSender, AsyncCommitter>,
    Receiver<Command>,
) {
    let s = MemStorage::new();
    let (c, receiver) = AsyncCommitter::new();
    let e = ZMQSender::new(context);
    let r = Raft::new_with_defaults(id.clone(), cluster, s, e, c);
    (r, receiver)
}

fn new_runner(
    pid: ProcessId,
    cluster: Cluster,
    context: Context,
    poller: &mut ZMQPoller<Sender<ReceivedMessage>>,
) -> (
    Runner<MemStorage, ZMQSender, AsyncCommitter>,
    Receiver<Command>,
) {
    let (raft, receiver) = new_raft(pid.clone(), cluster, context);
    let (ts, tr) = crossbeam::channel::unbounded();
    poller.add(pid.clone(), ts);
    (Runner::new(tr, raft), receiver)
}

pub struct Env {
    pub poller: ZMQPoller<Sender<ReceivedMessage>>,
    pub poller_sender: Sender<String>,

    pub runners: Vec<Runner<MemStorage, ZMQSender, AsyncCommitter>>,
    pub receivers: Vec<Receiver<Command>>,
}

impl Env {
    pub fn new(i: usize) -> Self {
        let (p1, p2, p3, cluster) = test_cluster();

        let context = Context::new();
        let (ts, tr) = crossbeam::channel::unbounded();
        let mut p = ZMQPoller::new(context.clone(), &new_process(0).addr(), tr);

        let (r1, r1_c) = new_runner(p1.clone(), cluster.clone(), context.clone(), &mut p);
        let (r2, r2_c) = new_runner(p2.clone(), cluster.clone(), context.clone(), &mut p);
        let (r3, r3_c) = new_runner(p3.clone(), cluster.clone(), context.clone(), &mut p);
        Env {
            poller: p,
            poller_sender: ts,
            runners: vec![r1, r2, r3],
            receivers: vec![r1_c, r2_c, r3_c],
        }
    }
}

pub mod logger {
    use std::time::Instant;

    use chrono::Local;
    use log::{Level, Metadata, Record};
    pub struct SimpleLogger {
        pub level: Level,
    }

    impl log::Log for SimpleLogger {
        fn enabled(&self, metadata: &Metadata) -> bool {
            metadata.level() <= self.level
        }

        fn log(&self, record: &Record) {
            if self.enabled(record.metadata()) {
                println!(
                    "{} {} - {}",
                    Local::now().format("%m-%d %H:%M:%S:%.3f:"),
                    record.level(),
                    record.args()
                );
            }
        }

        fn flush(&self) {}
    }
}
