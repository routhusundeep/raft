use std::{fmt::Display, net::IpAddr};

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

pub struct Cluster {
    all: Vec<ProcessId>,
}
impl Cluster {
    pub(crate) fn all(&self) -> std::slice::Iter<'_, ProcessId> {
        self.all.iter()
    }

    pub(crate) fn len(&self) -> usize {
        self.all.len()
    }
}
