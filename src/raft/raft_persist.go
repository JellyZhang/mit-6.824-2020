package raft

import (
	"bytes"

	"6.824/labgob"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.Role)
	e.Encode(rf.VotedFor)
	e.Encode(rf.GetVotedTickets)
	e.Encode(rf.Logs)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.LastApplied)
	e.Encode(rf.NextIndex)
	e.Encode(rf.MatchIndex)
	data := w.Bytes()

	DPrintf("[persist] %v, rf.currentTerm=%+v, len(rf.logs)=%v, rf.NextIndex=%+v", rf.me, rf.CurrentTerm, len(rf.Logs), rf.NextIndex)
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var role Role
	var votedFor int
	var getVotedTickets int32
	var logs []*Entry
	var commitIndex int
	var lastApplied int
	var nextIndex []int
	var matchIndex []int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&role) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&getVotedTickets) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&nextIndex) != nil ||
		d.Decode(&matchIndex) != nil {
		DPrintf("[readPersist] %v error", rf.me)
	} else {
		rf.CurrentTerm = currentTerm
		rf.Role = role
		rf.VotedFor = votedFor
		rf.GetVotedTickets = getVotedTickets
		rf.Logs = logs
		rf.CommitIndex = commitIndex
		rf.LastApplied = lastApplied
		rf.NextIndex = nextIndex
		rf.MatchIndex = matchIndex
	}
	DPrintf("[readPersist] %v, rf.CurrentTerm=%v, len(rf.Logs=%v), rf.NextIndex=%+v", rf.me, rf.CurrentTerm, len(rf.Logs), rf.NextIndex)
}
