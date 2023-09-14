This is a library implementing Raft Consensus algorithm in Rust. Most of the implementation inspired from the original paper. Unlike my Paxos implementation, I intend to make it feature rich and fully functional. 

## Features
### Basic Algorithm
A fully funtional algorithm is implemented and some basic tests are added to prove its functionality. The system is in need of rigorous testing since distributed systems are notorious for their faulty implementations. I will try to plug in a test suite to overcome this.

### Randomized timeout for leader election
Randomized election timeouts are critical to ensure that split votes are rare and that they are resolved quickly. To prevent split votes in the first place, election timeouts are chosen randomly from a fixed interval, same as the original Raft paper, the interval in the implementation is between 150-300 ms.

### Persistence
A partial implementation is already in place.

## Planned Feature
### Log Compaction
The commit log grows unconditionally and it is necessary to trim to stop this. As discussed in the paper, I am planning on implementing log snapshotting with only the leader doing the snapshot and propagating it to the followers.

### Membership Change
Joint Consensus as discussed in the paper will be used.

## TLA+
Just as a personal project, I would like to add the TLA Specification for Raft.

## References:
* [Original Paper](https://github.com/papers-we-love/papers-we-love/blob/main/distributed_systems/in-search-of-an-understandable-consensus-algorithm.pdf)
* [Code Reference in GO](https://github.com/eatonphil/goraft/tree/main)
