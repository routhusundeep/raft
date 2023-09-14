use std::{
    net::{IpAddr, Ipv4Addr},
    thread,
    time::Duration,
};

use log::{Level, LevelFilter, SetLoggerError};
use raft::{
    cluster::ProcessId,
    message::Message,
    sender::{Sender, ZMQSender},
    util::{logger::SimpleLogger, Env},
};
use rand::Rng;
use zmq::Context;

static LOGGER: SimpleLogger = SimpleLogger { level: Level::Info };

pub fn init() -> Result<(), SetLoggerError> {
    log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Trace))
}

#[test]
fn without_commands() {
    let _ = init();

    let mut env = Env::new(3);
    let (mut r1, mut r2, mut r3) = (
        env.runners.pop().unwrap(),
        env.runners.pop().unwrap(),
        env.runners.pop().unwrap(),
    );
    let poller = env.poller;

    let jp = thread::spawn(move || {
        poller.start();
    });

    let j1 = thread::spawn(move || {
        r1.start();
    });
    let j2 = thread::spawn(move || {
        r2.start();
    });
    let j3 = thread::spawn(move || {
        r3.start();
    });

    thread::sleep(Duration::from_secs(3));
}

#[test]
fn constant_qps_commands() {
    let duration = 10;
    let qps = 50;

    let _ = init();

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
    let addr = poller.addr();

    let jp = thread::spawn(move || {
        poller.start();
    });

    let j1 = thread::spawn(move || {
        r1.start_with_sleep(Duration::from_millis(100));
    });
    let j2 = thread::spawn(move || {
        r2.start_with_sleep(Duration::from_millis(100));
    });
    let j3 = thread::spawn(move || {
        r3.start_with_sleep(Duration::from_millis(100));
    });

    let fire_handle = thread::spawn(move || {
        // just wait for the first leader election
        thread::sleep(Duration::from_millis(300));
        fire_commands(pid1, pid2, pid3, duration, qps);
    });

    let (r1, r2, r3) = (
        env.receivers.pop().unwrap(),
        env.receivers.pop().unwrap(),
        env.receivers.pop().unwrap(),
    );

    thread::spawn(move || {
        let c1 = r1.recv().unwrap();
        let c2 = r2.recv().unwrap();
        let c3 = r3.recv().unwrap();
        assert_eq!(c1, c2);
        assert_eq!(c1, c3);
    });

    let _ = fire_handle.join();
}

fn fire_commands(pid1: ProcessId, pid2: ProcessId, pid3: ProcessId, duration: i32, qps: i32) {
    let client_id = ProcessId::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5555, 0);
    let client = ZMQSender::new(Context::new());

    let v = vec![pid1.clone(), pid2.clone(), pid3.clone()];
    let mut command_num = 1;
    for _ in 1..=duration {
        for _ in 1..=qps {
            let to = rand::thread_rng().gen_range(0..=2);
            command_num += 1;
            let _ = client.send(
                client_id.clone(),
                v[to].clone(),
                Message::Command(client_id.clone(), format!("command:{}", command_num).into()),
            );
        }
        thread::sleep(Duration::from_secs(1));
    }
}
