use std::ops::Range;

pub static PORT: u32 = 5555;
pub static ELECTION_INTERVAL_RANGE: Range<u64> = 150..300;
pub static HEARTBEAT_INTERVAL: u64 = 20;
