use std::net::Ipv4Addr;
use std::time::Duration;
use std::{thread, vec};

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
use raft::util::Env;
use zmq::Context;

#[test]
fn leader_elect() {
    let mut env = Env::new(3);
    let (mut r1, mut r2, mut r3) = (
        env.runners.pop().unwrap(),
        env.runners.pop().unwrap(),
        env.runners.pop().unwrap(),
    );
    let (r1_c, r2_c, r3_c) = (
        env.receivers.pop().unwrap(),
        env.receivers.pop().unwrap(),
        env.receivers.pop().unwrap(),
    );
    let (pid1, pid2, pid3) = (
        r1.raft().id().clone(),
        r2.raft().id().clone(),
        r3.raft().id().clone(),
    );
    let poller = env.poller;

    // wait for election timeout
    wait_for_election_timeout();

    // request for votes
    r1.tick();
    assert_eq!(1, r1.raft().current_term());

    // respond for vote request
    poller.run(2);
    r2.run_pending();
    r3.run_pending();
    assert_eq!(1, r2.raft().current_term());
    assert_eq!(1, r3.raft().current_term());

    // become leader
    poller.run(2);
    r1.run_pending();
    assert_eq!(&RaftType::Leader, r1.raft().raft_type());
    assert_eq!(Some(pid1.clone()), r1.raft().leader_id());

    // leader sends append entries
    r1.tick();

    // followers respond to append entries
    poller.run(2);
    r2.run_pending();
    r3.run_pending();
    assert_eq!(Some(pid1.clone()), r2.raft().leader_id());
    assert_eq!(Some(pid1.clone()), r3.raft().leader_id());

    // update follower states in leader and commit
    poller.run(2);
    r1.run_pending();
    assert_command(r1_c, "");

    // send commit index and uncommitted append logs to follower
    wait_for_heartbeat();
    r1.tick();

    // update follower states in leader and commit
    poller.run(2);
    r2.run_pending();
    r3.run_pending();
    assert_command(r2_c, "");
    assert_command(r3_c, "");

    poller.run(2);
}

#[test]
fn leader_elect_with_multiple_candidates() {
    let mut env = Env::new(3);
    let (mut r1, mut r2, mut r3) = (
        env.runners.pop().unwrap(),
        env.runners.pop().unwrap(),
        env.runners.pop().unwrap(),
    );
    let (pid1, pid2, pid3) = (
        r1.raft().id().clone(),
        r2.raft().id().clone(),
        r3.raft().id().clone(),
    );
    let poller = env.poller;

    // wait for election timeout
    wait_for_election_timeout();

    // r1 request for votes
    r1.tick();
    assert_eq!(1, r1.raft().current_term());
    assert_eq!(&RaftType::Candidate, r1.raft().raft_type());

    // r2 request for votes
    r2.tick();
    assert_eq!(1, r2.raft().current_term());
    assert_eq!(&RaftType::Candidate, r1.raft().raft_type());

    // r3 responds for vote request
    poller.run(4);
    r3.run_pending();
    assert_eq!(1, r3.raft().current_term());

    // r1 and r2 recieves the vote response and one of them will be the leader
    poller.run(2);
    r1.run_pending();
    r2.run_pending();
    let (t1, t2) = (r1.raft().raft_type(), r2.raft().raft_type());
    assert!(
        (t1, t2) == (&RaftType::Candidate, &RaftType::Leader)
            || (t2, t1) == (&RaftType::Candidate, &RaftType::Leader)
    );
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
