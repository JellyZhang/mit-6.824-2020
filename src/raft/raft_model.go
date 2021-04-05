package raft

import (
	"fmt"
	"sync"

	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm     int
	Role            Role
	VotedFor        int
	GetVotedTickets int32
	LastHeartbeat   int64
	Logs            []*Entry
	CommitIndex     int
	LastApplied     int
	NextIndex       []int
	MatchIndex      []int
	applyCh         chan ApplyMsg

	// logmu is used when accessing rf.Logs
	logmu sync.Mutex
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// String Function for print debug info
func (e *Entry) String() string {
	return fmt.Sprintf("{index=%v term=%v command=%v} ", e.Index, e.Term, e.Command)
}
