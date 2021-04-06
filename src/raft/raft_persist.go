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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.role)
	e.Encode(rf.votedFor)
	e.Encode(rf.getVotedTickets)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.nextIndex)
	e.Encode(rf.matchIndex)
	state := w.Bytes()

	w = new(bytes.Buffer)
	e = labgob.NewEncoder(w)
	e.Encode(rf.logs)
	snapshot := w.Bytes()

	DPrintf("[persist] %v, rf.currentTerm=%+v, len(rf.logs)=%v, rf.NextIndex=%+v", rf.me, rf.currentTerm, len(rf.logs), rf.nextIndex)
	rf.persister.SaveStateAndSnapshot(state, snapshot)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(state []byte, snapshot []byte) {
	if state == nil || len(state) < 1 { // bootstrap without any state?
		return
	}
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var role Role
	var votedFor int
	var getVotedTickets int32
	var commitIndex int
	var lastApplied int
	var nextIndex []int
	var matchIndex []int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&role) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&getVotedTickets) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&nextIndex) != nil ||
		d.Decode(&matchIndex) != nil {
		DPrintf("[readPersist] %v decode error", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.role = role
		rf.votedFor = votedFor
		rf.getVotedTickets = getVotedTickets
		rf.commitIndex = commitIndex
		rf.lastApplied = lastApplied
		rf.nextIndex = nextIndex
		rf.matchIndex = matchIndex
	}

	r = bytes.NewBuffer(snapshot)
	d = labgob.NewDecoder(r)
	var logs []*Entry
	if d.Decode(&logs) != nil {
		DPrintf("[readPersist] %v decode error", rf.me)
	} else {
		rf.logs = logs
	}
	DPrintf("[readPersist] %v, rf.CurrentTerm=%v, len(rf.Logs=%v), rf.NextIndex=%+v", rf.me, rf.currentTerm, len(rf.logs), rf.nextIndex)
}
