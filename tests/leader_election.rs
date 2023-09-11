use std::net::Ipv4Addr;
use std::thread;
use std::time::Duration;

use crossbeam::channel::Sender;
use raft::cluster::{Cluster, ProcessId};
use raft::consts::ELECTION_INTERVAL_RANGE;
use raft::message::ReceivedMessage;
use raft::poller::ZMQPoller;
use raft::raft::{Raft, RaftType};
use raft::runner::Runner;
use raft::sender::ZMQSender;
use raft::storage::MemStorage;
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

fn new_raft(id: ProcessId, cluster: Cluster, context: Context) -> Raft<MemStorage, ZMQSender> {
    let s = MemStorage::new();
    let c = Committer::new();
    let e = ZMQSender::new(context);
    let r = Raft::new_with_defaults(id.clone(), cluster, s, e, c);
    r
}

fn new_runner(
    pid: ProcessId,
    cluster: Cluster,
    context: Context,
    poller: &mut ZMQPoller<Sender<ReceivedMessage>>,
) -> Runner<MemStorage, ZMQSender> {
    let raft = new_raft(pid.clone(), cluster, context);
    let (ts, tr) = crossbeam::channel::unbounded();
    poller.add(pid.clone(), ts);
    Runner::new(tr, raft)
}

fn init() -> (
    ZMQPoller<Sender<ReceivedMessage>>,
    Sender<String>,
    Runner<MemStorage, ZMQSender>,
    Runner<MemStorage, ZMQSender>,
    Runner<MemStorage, ZMQSender>,
) {
    let (p1, p2, p3, cluster) = test_cluster();

    let context = Context::new();
    let (ts, tr) = crossbeam::channel::unbounded();
    let mut p = ZMQPoller::new(context.clone(), &new_process(0).addr(), tr);

    let r1 = new_runner(p1.clone(), cluster.clone(), context.clone(), &mut p);
    let r2 = new_runner(p2.clone(), cluster.clone(), context.clone(), &mut p);
    let r3 = new_runner(p3.clone(), cluster.clone(), context.clone(), &mut p);
    (p, ts, r1, r2, r3)
}

#[test]
fn elect_leader() {
    let (p, ts, mut r1, mut r2, mut r3) = init();
    let (pid1, pid2, pid3) = (
        r1.raft().id().clone(),
        r2.raft().id().clone(),
        r3.raft().id().clone(),
    );

    // wait for election timeout
    wait_for_election_timeout();

    // request for votes
    r1.tick();
    assert_eq!(1, r1.raft().current_term());

    // respond for vote request
    p.run(2);
    r2.empty_queue();
    r3.empty_queue();
    assert_eq!(1, r2.raft().current_term());
    assert_eq!(1, r3.raft().current_term());

    // become leader
    p.run(2);
    r1.empty_queue();
    assert_eq!(&RaftType::Leader, r1.raft().raft_type());
    assert_eq!(Some(pid1.clone()), r1.raft().leader_id());

    // leader sends append entries
    r1.tick();

    // followers respond to append entries
    p.run(2);
    r2.empty_queue();
    r3.empty_queue();
    assert_eq!(Some(pid1.clone()), r2.raft().leader_id());
    assert_eq!(Some(pid1.clone()), r3.raft().leader_id());

    // update follower states in leader and commit
    p.run(2);
    r1.empty_queue();
}

fn wait_for_election_timeout() {
    thread::sleep(Duration::from_millis(ELECTION_INTERVAL_RANGE.end));
}
