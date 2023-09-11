use std::net::Ipv4Addr;
use std::thread;
use std::time::Duration;

use crossbeam::channel::{Receiver, Sender};
use raft::basic::Command;
use raft::cluster::{Cluster, ProcessId};
use raft::committer::AsyncCommitter;
use raft::consts::{ELECTION_INTERVAL_RANGE, HEARTBEAT_INTERVAL};
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

fn init() -> TestEnv {
    let (p1, p2, p3, cluster) = test_cluster();

    let context = Context::new();
    let (ts, tr) = crossbeam::channel::unbounded();
    let mut p = ZMQPoller::new(context.clone(), &new_process(0).addr(), tr);

    let (r1, r1_c) = new_runner(p1.clone(), cluster.clone(), context.clone(), &mut p);
    let (r2, r2_c) = new_runner(p2.clone(), cluster.clone(), context.clone(), &mut p);
    let (r3, r3_c) = new_runner(p3.clone(), cluster.clone(), context.clone(), &mut p);
    TestEnv {
        poller: p,
        poller_sender: ts,
        r1: r1,
        r1_c: r1_c,
        r2: r2,
        r2_c: r2_c,
        r3: r3,
        r3_c: r3_c,
    }
}

struct TestEnv {
    poller: ZMQPoller<Sender<ReceivedMessage>>,
    poller_sender: Sender<String>,

    r1: Runner<MemStorage, ZMQSender, AsyncCommitter>,
    r1_c: Receiver<Command>,

    r2: Runner<MemStorage, ZMQSender, AsyncCommitter>,
    r2_c: Receiver<Command>,

    r3: Runner<MemStorage, ZMQSender, AsyncCommitter>,
    r3_c: Receiver<Command>,
}

#[test]
fn elect_leader() {
    let mut env = init();
    let (pid1, pid2, pid3) = (
        env.r1.raft().id().clone(),
        env.r2.raft().id().clone(),
        env.r3.raft().id().clone(),
    );

    // wait for election timeout
    wait_for_election_timeout();

    // request for votes
    env.r1.tick();
    assert_eq!(1, env.r1.raft().current_term());

    // respond for vote request
    env.poller.run(2);
    env.r2.run_pending();
    env.r3.run_pending();
    assert_eq!(1, env.r2.raft().current_term());
    assert_eq!(1, env.r3.raft().current_term());

    // become leader
    env.poller.run(2);
    env.r1.run_pending();
    assert_eq!(&RaftType::Leader, env.r1.raft().raft_type());
    assert_eq!(Some(pid1.clone()), env.r1.raft().leader_id());

    // leader sends append entries
    env.r1.tick();

    // followers respond to append entries
    env.poller.run(2);
    env.r2.run_pending();
    env.r3.run_pending();
    assert_eq!(Some(pid1.clone()), env.r2.raft().leader_id());
    assert_eq!(Some(pid1.clone()), env.r3.raft().leader_id());

    // update follower states in leader and commit
    env.poller.run(2);
    env.r1.run_pending();
    assert_command(env.r1_c, "");

    // send commit index and uncommitted append logs to follower
    wait_for_heartbeat();
    env.r1.tick();

    // update follower states in leader and commit
    env.poller.run(2);
    env.r2.run_pending();
    env.r3.run_pending();
    assert_command(env.r2_c, "");
    assert_command(env.r3_c, "");

    env.poller.run(2);
}

fn assert_command(r: Receiver<Command>, arg: &str) {
    assert_eq!(r.recv().unwrap(), Command::Normal(arg.to_string().into()));
}

fn wait_for_election_timeout() {
    thread::sleep(Duration::from_millis(ELECTION_INTERVAL_RANGE.end));
}

fn wait_for_heartbeat() {
    thread::sleep(Duration::from_millis(HEARTBEAT_INTERVAL));
}
