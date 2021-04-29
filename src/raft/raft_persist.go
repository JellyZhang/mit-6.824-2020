package raft

import (
	"bytes"
	"log"

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
	e.Encode(rf.logs)
	state := w.Bytes()

	DPrintf("[persist] %v, len(Statesize)=%v", rf.me, len(state))
	rf.persister.SaveStateAndSnapshot(state, rf.snapshotData)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(state []byte, snapshotBytes []byte) {
	DPrintf("[readPersist] %v readPersist", rf.me)
	if state == nil || len(state) < 1 { // bootstrap without any state?
		return
	}
	if snapshotBytes == nil {
		return
	}

	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var logs []*Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&logs) != nil {
		log.Fatalf("[readPersist] %v decode error", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.logs = logs
	}

	rf.snapshotData = snapshotBytes
	rf.commitIndex = rf.getSnapshotLastIndex()

	DPrintf("[readPersist] %v, rf.snapshotData=%v, rf.logs=%+v", rf.me, rf.snapshotData, rf.logs)
}
