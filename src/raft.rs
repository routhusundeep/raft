use std::{
    cmp,
    collections::{HashMap, HashSet},
    hash::Hash,
    ops::Range,
    process::Command,
    time::{Duration, Instant},
};

use bytes::Bytes;
use log::{debug, info};
use rand::Rng;

use crate::{
    basic::{Entry, Index, Term},
    cluster::{Cluster, ProcessId},
    committer::Committer,
    consts::{ELECTION_INTERVAL_RANGE, HEARTBEAT_INTERVAL},
    message::Message,
    sender::Sender,
    storage::{LogEntry, Storage},
    stored::StoredState,
};

#[derive(Eq, PartialEq, Debug)]
pub enum RaftType {
    Leader,
    Follower,
    Candidate,
}

pub struct Raft<S: Storage, E: Sender, C:Committer> {
    t: RaftType,
    id: ProcessId,
    cluster: Cluster,

    sender: E,
    store: StoredState<S>,

    commiter: C,

    commit_index: Index,
    last_applied: Index,

    leader_id: Option<ProcessId>,

    // candidate
    votes: HashSet<ProcessId>,

    // leader
    next_indexes: HashMap<ProcessId, Index>,
    match_indexes: HashMap<ProcessId, Index>,

    // timeouts
    election_time_out_range: Range<u64>, // election time out range
    election_time_out: Instant,          // next time at which election will be triggered

    heart_beat_interval: u64,     // interval at which empty appends are sent
    heart_beat_time_out: Instant, // next time after which empty appends will be sent
}

impl<S: Storage, E: Sender, C:Committer> Raft<S, E, C> {
    pub fn new_with_defaults(id: ProcessId, cluster: Cluster, storage: S, sender: E, committer: C) -> Raft<S, E, C> {
        Self::new(
            id,
            cluster,
            storage,
            sender,
            committer,
            ELECTION_INTERVAL_RANGE.clone(),
            HEARTBEAT_INTERVAL,
        )
    }

    pub fn new(
        id: ProcessId,
        cluster: Cluster,
        storage: S,
        sender: E,
        committer: C,
        election_time_out_range: Range<u64>,
        heart_beat_interval: u64,
    ) -> Raft<S, E, C> {
        Raft {
            t: RaftType::Follower,
            id: id,
            cluster: cluster,
            sender: sender,
            commiter: committer,
            store: StoredState::new(storage),
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            votes: HashSet::new(),
            next_indexes: HashMap::new(),
            match_indexes: HashMap::new(),
            election_time_out_range: election_time_out_range.clone(),
            election_time_out: Instant::now()
                .checked_add(Duration::from_millis(
                    rand::thread_rng()
                        .gen_range(election_time_out_range.clone())
                        .into(),
                ))
                .expect("time exceeded"),
            heart_beat_interval: heart_beat_interval,
            heart_beat_time_out: Instant::now(),
        }
    }

    pub fn id(&self) -> &ProcessId {
        &self.id
    }

    pub fn raft_type(&self) -> &RaftType {
        &self.t
    }

    pub fn leader_id(&self) -> Option<ProcessId> {
        self.leader_id.clone()
    }

    pub fn current_term(&self) -> Term {
        self.store.current_term()
    }

    pub fn commit_index(&self) -> Index {
        self.commit_index
    }

    pub fn process(&mut self, from: ProcessId, m: Message) {
        match self.t {
            RaftType::Leader => {
                self.leader(from, m);
                self.apply_remaining();
            }
            RaftType::Follower => self.follower(from, m),
            RaftType::Candidate => {
                self.candidate(from, m);
                self.apply_remaining();
            }
        }
    }

    fn move_commit_index(&mut self) {
        let last_index = self.store.last_index();
        for idx in last_index..self.commit_index {
            let mut quorum = self.cluster.len() / 2 + 1;
            for server in self.cluster.all() {
                if quorum == 0 {
                    break;
                }
                if self.id == server
                    || self.match_indexes.get(&server).expect("will be present") >= &idx
                {
                    quorum -= 1;
                }
            }
            if quorum == 0 {
                self.commit_index = idx;
                debug!("process:{}, commit index is set to {}", self.id, idx);
                break;
            }
        }
    }

    fn apply_remaining(&mut self) {
        while self.commit_index > self.last_applied {
            self.last_applied += 1;
            self.apply(self.last_applied);
        }
    }

    fn apply(&self, index: Index) -> () {
        info!("process:{} is applying index {}", self.id, index)
    }

    fn leader(&mut self, from: ProcessId, m: Message) {
        match m {
            Message::Empty => {
                if Instant::now() > self.heart_beat_time_out {
                    self.heart_beat_time_out = Instant::now()
                        .checked_add(Duration::from_millis(self.heart_beat_interval.into()))
                        .expect("time exceeded");

                    self.send_append_entries();
                }
            }
            Message::Terminate => ignore(m, self.id.clone()),
            Message::AppendEntries(term, last_index, last_term, entries, commit_index) => {
                // if the message's term is greater than our term, change to follower and apply
                if self.check_term_and_change_to_follower(term) {
                    self.handle_append_entries(
                        term,
                        last_index,
                        last_term,
                        &from,
                        entries,
                        commit_index,
                    );
                }
            }
            Message::AppendEntriesResponse(index, term, success) => {
                if self.check_term_and_change_to_follower(term) {
                    self.update_term(term);
                    return;
                }

                if term != self.store.current_term() {
                    debug!("process:{} dropping stale term", self.id);
                    return;
                }

                if success {
                    let _ = self
                        .next_indexes
                        .insert(from.clone(), index + 1)
                        .expect("will be present");
                    self.match_indexes.insert(from, index);
                    self.move_commit_index();
                } else {
                    self.next_indexes.insert(from, cmp::max(index - 1, 1));
                }
            }
            Message::RequestVote(term, last_term, last_index) => {
                if self.check_term_and_change_to_follower(term) {
                    self.handle_request_vote(term, from, last_term, last_index);
                }
            }
            Message::RequestVoteResponse(_, _) => ignore(m, self.id.clone()),
            Message::Command(client, b) => {
                self.store.append(b.clone());
                self.send_append_entries();
            }
        }
    }

    fn candidate(&mut self, from: ProcessId, m: Message) {
        match m {
            Message::Empty => {
                if Instant::now() > self.election_time_out {
                    self.start_election();
                }
            }
            Message::AppendEntries(term, last_index, last_term, entries, commit_index) => {
                let current_term = self.store.current_term();
                if current_term <= term {
                    self.change_to_follower();
                    self.handle_append_entries(
                        term,
                        last_index,
                        last_term,
                        &from,
                        entries,
                        commit_index,
                    )
                }
            }
            Message::RequestVote(term, last_term, last_index) => {
                if self.check_term_and_change_to_follower(term) {
                    self.handle_request_vote(term, from, last_term, last_index)
                }
            }
            Message::RequestVoteResponse(term, voted) => {
                if voted && self.store.current_term() == term {
                    self.votes.insert(from);
                    let quorum = self.cluster.all().len() / 2 + 1;

                    if self.votes.len() >= quorum {
                        self.change_to_leader();
                    }
                }
            }
            Message::AppendEntriesResponse(_, _, _) => ignore(m, self.id.clone()),
            Message::Terminate => ignore(m, self.id.clone()),
            Message::Command(_, _) => todo!(),
        }
    }

    fn follower(&mut self, from: ProcessId, m: Message) {
        match m {
            Message::Empty => {
                if Instant::now() > self.election_time_out {
                    self.change_to_candidate();
                }
            }
            Message::AppendEntries(term, last_index, last_term, entries, commit_index) => self
                .handle_append_entries(term, last_index, last_term, &from, entries, commit_index),
            Message::RequestVote(term, last_index, last_term) => {
                self.handle_request_vote(term, from, last_term, last_index);
            }
            _ => ignore(m, self.id.clone()),
        }
    }

    fn change_to_candidate(&mut self) {
        self.t = RaftType::Candidate;
        self.votes.clear();
        self.store.increase_term();
        self.start_election();
    }

    fn change_to_leader(&mut self) {
        self.t = RaftType::Leader;
        self.set_leader(&self.id.clone());

        let last_index = self.store.last_index();
        for server in self.cluster.all() {
            self.next_indexes.insert(server.clone(), last_index + 1);
            self.match_indexes.insert(server.clone(), 0);
        }
        self.store.append(Bytes::new());
        self.heart_beat_time_out = Instant::now();
    }

    fn change_to_follower(&mut self) {
        self.t = RaftType::Follower;
    }

    fn check_term_and_change_to_follower(&mut self, term: Term) -> bool {
        if term > self.store.current_term() {
            self.change_to_follower();
            return true;
        }
        false
    }

    fn send_append_entries(&mut self) {
        for server in self.cluster.all() {
            if server == self.id {
                continue;
            }

            let next = self.next_indexes.get(&server).expect("will be present");
            let last_index = next - 1;
            let last_term = self.store.at(last_index).expect("will be present").term;
            let entries = self.store.entries_from(*next);

            let _ = self.sender.send(
                self.id.clone(),
                server.clone(),
                Message::AppendEntries(
                    self.store.current_term(),
                    last_index,
                    last_term,
                    entries
                        .into_iter()
                        .map(|e| Entry::Normal(e.index, e.term, e.bytes))
                        .collect(),
                    self.commit_index,
                ),
            );
        }
    }

    fn start_election(&mut self) {
        self.store.vote_for(self.id.clone());
        self.votes.clear();
        self.votes.insert(self.id.clone());
        self.set_election_timeout();

        let (last_index, last_term) = self.store.last_index_and_term();
        for server in self.cluster.all() {
            if server == self.id {
                continue;
            }
            let _ = self.sender.send(
                self.id.clone(),
                server.clone(),
                Message::RequestVote(self.store.current_term(), last_index, last_term),
            );
        }
    }

    fn update_term(&mut self, term: Term) -> bool {
        if self.store.current_term() < term {
            self.store.set_term(term);
            self.change_to_follower();
            self.store.not_voted();
            self.set_election_timeout();
            return true;
        }
        return false;
    }

    fn handle_append_entries(
        &mut self,
        term: Term,
        last_index: Index,
        last_term: Term,
        from: &ProcessId,
        entries: Vec<Entry>,
        commit_index: Index,
    ) {
        self.update_term(term);

        // only followers can append
        assert_eq!(RaftType::Follower, self.t);

        self.set_election_timeout();
        self.set_leader(from);

        if term < self.store.current_term() {
            debug!(
                "process:{} is dropping request from term:{} since it is a message from old leader",
                self.id, term
            );
            self.respond_append(from.clone(), false);
            return;
        }

        // last_index and last_term should be same as leader
        let valid_previous_log = last_index == 0 // first step
            || self
                .store
                .at(last_index)
                .map(|e| e.term != last_term)
                .unwrap_or(true);
        if !valid_previous_log {
            self.respond_append(from.clone(), false);
            return;
        }

        // persist all entries in the log
        self.store.insert_after(
            last_index + 1,
            entries
                .into_iter()
                .map(|e| match e {
                    Entry::Normal(index, term, bytes) => LogEntry {
                        index: index,
                        term: term,
                        bytes: bytes,
                    },
                })
                .collect(),
        );

        // if leaders commit index is greater than our commit index, then set our commit index to the minimum of last index or leaders
        if commit_index > self.commit_index {
            self.commit_index = cmp::min(commit_index, self.store.last_index());
        }

        self.respond_append(from.clone(), true);
    }

    fn handle_request_vote(
        &mut self,
        term: Term,
        from: ProcessId,
        last_term: Term,
        last_index: Index,
    ) {
        self.update_term(term);

        if term < self.store.current_term() {
            debug!(
                "process:{} not granting vote for lower term:{}",
                self.id, term
            );
            self.respond_vote(from, false);
            return;
        }

        let voted_for = self.store.get_vote();
        let (our_index, our_term) = self.store.last_index_and_term();
        let log_ok = last_term > our_term || (last_term == our_term && last_index >= our_index);
        let grant = term == self.store.current_term()
            && log_ok
            && (voted_for.is_none() || voted_for.is_some_and(|p| p == from));

        if !grant {
            self.respond_vote(from.clone(), false);
            return;
        }

        self.respond_vote(from.clone(), true);
        self.store.vote_for(from);
        self.set_election_timeout();
    }

    fn respond_append(&mut self, process_id: ProcessId, b: bool) {
        let _ = self.sender.send(
            self.id.clone(),
            process_id,
            Message::AppendEntriesResponse(self.store.last_index(), self.store.current_term(), b),
        );
    }

    fn respond_vote(&mut self, process_id: ProcessId, b: bool) {
        let _ = self.sender.send(
            self.id.clone(),
            process_id,
            Message::RequestVoteResponse(self.store.current_term(), b),
        );
    }

    fn set_election_timeout(&mut self) {
        self.election_time_out = Instant::now()
            .checked_add(Duration::from_millis(
                rand::thread_rng()
                    .gen_range(self.election_time_out_range.clone())
                    .into(),
            ))
            .expect("time exceeded");
    }

    fn set_leader(&mut self, id: &ProcessId) {
        self.leader_id = Some(id.clone());
    }
}

fn ignore(m: Message, id: ProcessId) -> () {
    debug!("process:{} is ignoring the message:{}", id, m)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, net::Ipv4Addr, thread, time::Duration, vec};

    use bytes::Bytes;

    use crate::{
        basic::Entry,
        cluster::{Cluster, ProcessId},
        message::{Message, WireMessage},
        raft::RaftType,
        sender::Sender,
        storage::{LogEntry, MemStorage},
    };

    use super::Raft;

    struct MockSender {
        v: RefCell<Vec<WireMessage>>,
    }
    impl MockSender {
        fn new() -> MockSender {
            MockSender {
                v: RefCell::new(vec![]),
            }
        }

        fn clear(&mut self) {
            self.v.borrow_mut().clear();
        }
    }

    impl Sender for MockSender {
        fn send(
            &self,
            from: ProcessId,
            to: ProcessId,
            m: Message,
        ) -> Result<(), crate::sender::Error> {
            self.v.borrow_mut().push(WireMessage {
                from: from,
                to: to,
                message: m,
            });
            Ok(())
        }
    }

    fn new_process(i: u32) -> ProcessId {
        ProcessId::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5555, i)
    }

    fn new_raft(id: ProcessId, cluster: Cluster) -> Raft<MemStorage, MockSender> {
        let s = MemStorage::new();
        let e = MockSender::new();
        let c = Committer::new();
        let r = Raft::new_with_defaults(id.clone(), cluster, s, e, c);
        r
    }

    fn test_cluster() -> (ProcessId, ProcessId, ProcessId, Cluster) {
        let p1 = new_process(1);
        let p2 = new_process(2);
        let p3 = new_process(3);

        let cluster = Cluster::new();
        cluster.add_all(vec![p1.clone(), p2.clone(), p3.clone()]);
        (p1, p2, p3, cluster)
    }

    #[test]
    fn follower_change_to_candidate() {
        let (p1, p2, p3, cluster) = test_cluster();
        let mut r = new_raft(p1.clone(), cluster.clone());

        let client = new_process(100);
        thread::sleep(Duration::from_millis(r.election_time_out_range.end));
        r.process(client, Message::Empty);

        assert_eq!(RaftType::Candidate, r.t);
        assert_eq!(1, r.store.current_term());
        assert_sent_messages(
            r.sender.v.borrow().clone(),
            vec![
                WireMessage {
                    from: p1.clone(),
                    to: p2.clone(),
                    message: Message::RequestVote(1, 0, 0),
                },
                WireMessage {
                    from: p1.clone(),
                    to: p3.clone(),
                    message: Message::RequestVote(1, 0, 0),
                },
            ],
        );
    }

    #[test]
    fn follower_append() {
        let (p1, p2, p3, cluster) = test_cluster();
        let mut r = new_raft(p1.clone(), cluster.clone());

        // process the first request
        r.process(p2.clone(), Message::AppendEntries(1, 0, 0, vec![], 0));
        assert_eq!(RaftType::Follower, r.t);
        assert_eq!(Some(p2.clone()), r.leader_id);
        assert_sent_messages(
            r.sender.v.borrow().clone(),
            vec![WireMessage {
                from: p1.clone(),
                to: p2.clone(),
                message: Message::AppendEntriesResponse(0, 1, true),
            }],
        );
        assert_eq!(0, r.store.last_index());

        // process the second request with some commands
        r.sender.clear();
        r.process(
            p2.clone(),
            Message::AppendEntries(
                1,
                0,
                0,
                vec![
                    Entry::Normal(1, 1, Bytes::new()),
                    Entry::Normal(2, 1, "command1".into()),
                ],
                0,
            ),
        );

        assert_eq!(RaftType::Follower, r.t);
        assert_sent_messages(
            r.sender.v.borrow().clone(),
            vec![WireMessage {
                from: p1.clone(),
                to: p2.clone(),
                message: Message::AppendEntriesResponse(2, 1, true),
            }],
        );
        assert_eq!(2, r.store.last_index());
        assert_eq!(
            Some(LogEntry {
                term: 1,
                index: 1,
                bytes: Bytes::new()
            }),
            r.store.at(1)
        );
        assert_eq!(
            Some(LogEntry {
                term: 1,
                index: 2,
                bytes: "command1".into()
            }),
            r.store.at(2)
        );

        // process the third request and do not apply it since it is stale
        r.sender.clear();
        r.process(
            p2.clone(),
            Message::AppendEntries(0, 0, 0, vec![Entry::Normal(1, 1, Bytes::new())], 0),
        );
        assert_sent_messages(
            r.sender.v.borrow().clone(),
            vec![WireMessage {
                from: p1.clone(),
                to: p2.clone(),
                message: Message::AppendEntriesResponse(2, 1, false),
            }],
        );
        assert_eq!(2, r.store.last_index());
    }

    #[test]
    fn follower_vote() {
        let (p1, p2, p3, cluster) = test_cluster();
        let mut r = new_raft(p1.clone(), cluster.clone());

        r.process(p2.clone(), Message::RequestVote(1, 0, 0));
        assert_eq!(RaftType::Follower, r.t);
        assert_sent_messages(
            r.sender.v.borrow().clone(),
            vec![WireMessage {
                from: p1.clone(),
                to: p2.clone(),
                message: Message::RequestVoteResponse(1, true),
            }],
        );
        assert_eq!(Some(p2.clone()), r.store.get_vote());
        assert_eq!(1, r.store.current_term());
    }

    #[test]
    fn candidate_vote() {
        let (p1, p2, p3, cluster) = test_cluster();
        let mut r = new_raft(p1.clone(), cluster.clone());

        r.change_to_candidate();
        assert_eq!(RaftType::Candidate, r.t);
        r.process(p2.clone(), Message::RequestVote(2, 2, 0));
        assert_eq!(RaftType::Follower, r.t);
        assert_eq!(2, r.store.current_term());
    }

    #[test]
    fn candidate_append() {
        let (p1, p2, p3, cluster) = test_cluster();
        let mut r = new_raft(p1.clone(), cluster.clone());

        r.change_to_candidate();
        r.process(
            p2.clone(),
            Message::AppendEntries(1, 0, 0, vec![Entry::Normal(2, 1, "command1".into())], 0),
        );
        assert_eq!(RaftType::Follower, r.t);
        assert_eq!(1, r.store.current_term());
    }

    #[test]
    fn candidate_change_to_leader() {
        let (p1, p2, p3, cluster) = test_cluster();
        let mut r = new_raft(p1.clone(), cluster.clone());

        r.change_to_candidate();
        r.start_election();
        r.process(p2.clone(), Message::Empty);
        assert_eq!(RaftType::Candidate, r.t);
        r.process(p2.clone(), Message::RequestVoteResponse(1, true));
        assert_eq!(RaftType::Leader, r.t);
        assert_eq!(1, r.store.current_term());
    }

    #[test]
    fn candidate_restart_election() {
        let (p1, p2, p3, cluster) = test_cluster();
        let mut r = new_raft(p1.clone(), cluster.clone());

        r.change_to_candidate();
        r.start_election();
        r.sender.clear();
        thread::sleep(Duration::from_millis(r.election_time_out_range.end));

        r.process(p2.clone(), Message::Empty);
        assert_eq!(RaftType::Candidate, r.t);
        assert_sent_messages(
            r.sender.v.borrow().clone(),
            vec![
                WireMessage {
                    from: p1.clone(),
                    to: p2.clone(),
                    message: Message::RequestVote(1, 0, 0),
                },
                WireMessage {
                    from: p1.clone(),
                    to: p3.clone(),
                    message: Message::RequestVote(1, 0, 0),
                },
            ],
        );
    }

    #[test]
    fn leader_on_elected() {
        let (p1, p2, p3, cluster) = test_cluster();
        let mut r = new_raft(p1.clone(), cluster.clone());

        r.change_to_candidate();
        r.sender.clear();
        r.change_to_leader();
        r.process(p1.clone(), Message::Empty);
        assert_sent_messages(
            r.sender.v.borrow().clone(),
            vec![
                WireMessage {
                    from: p1.clone(),
                    to: p2.clone(),
                    message: Message::AppendEntries(
                        1,
                        0,
                        0,
                        vec![Entry::Normal(0, 1, Bytes::new())],
                        0,
                    ),
                },
                WireMessage {
                    from: p1.clone(),
                    to: p3.clone(),
                    message: Message::AppendEntries(
                        1,
                        0,
                        0,
                        vec![Entry::Normal(0, 1, Bytes::new())],
                        0,
                    ),
                },
            ],
        );
    }

    #[test]
    fn leader_process_command() {
        let (p1, p2, p3, cluster) = test_cluster();
        let mut r = new_raft(p1.clone(), cluster.clone());

        r.change_to_candidate();
        r.change_to_leader();
        r.sender.clear();
        r.process(p1.clone(), Message::Command(p2.clone(), "command".into()));
        assert_sent_messages(
            r.sender.v.borrow().clone(),
            vec![
                WireMessage {
                    from: p1.clone(),
                    to: p2.clone(),
                    message: Message::AppendEntries(
                        1,
                        0,
                        0,
                        vec![
                            Entry::Normal(0, 1, "".into()),
                            Entry::Normal(1, 1, "command".into()),
                        ],
                        0,
                    ),
                },
                WireMessage {
                    from: p1.clone(),
                    to: p3.clone(),
                    message: Message::AppendEntries(
                        1,
                        0,
                        0,
                        vec![
                            Entry::Normal(0, 1, "".into()),
                            Entry::Normal(1, 1, "command".into()),
                        ],
                        0,
                    ),
                },
            ],
        );
    }

    #[test]
    fn leader_heartbeat() {
        let (p1, p2, p3, cluster) = test_cluster();
        let mut r = new_raft(p1.clone(), cluster.clone());

        r.change_to_candidate();
        r.change_to_leader();
        r.sender.clear();
        thread::sleep(Duration::from_millis(r.heart_beat_interval));
        r.process(p1.clone(), Message::Empty);
        assert_sent_messages(
            r.sender.v.borrow().clone(),
            vec![
                WireMessage {
                    from: p1.clone(),
                    to: p2.clone(),
                    message: Message::AppendEntries(
                        1,
                        0,
                        0,
                        vec![Entry::Normal(0, 1, Bytes::new())],
                        0,
                    ),
                },
                WireMessage {
                    from: p1.clone(),
                    to: p3.clone(),
                    message: Message::AppendEntries(
                        1,
                        0,
                        0,
                        vec![Entry::Normal(0, 1, Bytes::new())],
                        0,
                    ),
                },
            ],
        );
    }

    fn assert_sent_messages(mut v1: Vec<WireMessage>, mut v2: Vec<WireMessage>) {
        assert_eq!(v1.len(), v2.len());
        v1.sort_by(|e1, e2| e1.to.id.cmp(&e2.to.id));
        v2.sort_by(|e1, e2| e1.to.id.cmp(&e2.to.id));
        for i in 0..v1.len() {
            assert_eq!(v1[i], v2[i]);
        }
    }
}
