use std::{
    fmt::Display,
    net::IpAddr,
    sync::{Arc, Mutex},
};

#[derive(Eq, Ord, PartialEq, PartialOrd, Hash, Clone, Debug)]
pub struct ProcessId {
    pub ip: IpAddr,
    pub port: u32,
    pub id: u32,
}

impl ProcessId {
    pub fn new(ip: IpAddr, port: u32, id: u32) -> ProcessId {
        ProcessId {
            ip: ip,
            port: port,
            id: id,
        }
    }
}

impl Display for ProcessId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ProcessID(ip:{}, port:{}, id:{})",
            self.ip, self.port, self.id
        )
    }
}

impl ProcessId {
    pub fn addr(&self) -> String {
        format!("tcp://{}:{}", self.ip, self.port)
    }
}

#[derive(Clone, Debug)]
pub struct Cluster {
    all: Arc<Mutex<Vec<ProcessId>>>,
}

impl Cluster {
    pub(crate) fn all(&self) -> Vec<ProcessId> {
        let g = self.all.lock().unwrap();
        let mut v = vec![];
        for i in g.iter() {
            v.push(i.clone());
        }
        v
    }

    pub(crate) fn len(&self) -> usize {
        self.all.lock().unwrap().len()
    }

    pub fn new() -> Cluster {
        Cluster {
            all: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn add(&mut self, id: ProcessId) {
        self.all.lock().unwrap().push(id)
    }

    pub fn add_all(&self, p: Vec<ProcessId>) {
        let mut g = self.all.lock().unwrap();
        for i in p {
            g.push(i);
        }
    }
}
