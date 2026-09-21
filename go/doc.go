// Package coracle is a small, readable implementation of the Raft consensus
// algorithm - leader election and log replication - with a deterministic
// simulator so an election, a partition, or a replication round replays
// identically every run.
//
// A node never touches the clock or the network: it is a pure state machine with
// two entry points, Tick and Handle, each returning the messages it wants sent.
// All time and I/O live in the Cluster simulator. This mirrors the Python
// reference implementation module for module.
package coracle
