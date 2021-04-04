package raft

import (
	"bytes"

	"6.824/labgob"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              interface{}
}

type InstallSnapshotReply struct {
	Success bool
	Term    int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logmu.Lock()
	defer rf.logmu.Unlock()
	DPrintf("[InstallSnapshot] %v get InstallSnapshot, args=%+v", rf.me, args)

	if args.Term < rf.CurrentTerm {
		DPrintf("[InstallSnapshot] %v get low term from %v, myterm=%v, histerm=%v", rf.me, args.LeaderId, rf.CurrentTerm, args.Term)
		reply.Term = rf.CurrentTerm
		return
	}

	rf.CurrentTerm = args.Term
	rf.Role = Follower
	reply.Term = rf.CurrentTerm

	if args.LastIncludedIndex < rf.getSnapshotLastIndex() {
		DPrintf("[panic] args=%+v, rf.snapShotLastIndex=%v", args, rf.getSnapshotLastIndex())
		panic("")
	}

	if args.LastIncludedIndex == rf.getSnapshotLastIndex() {
		reply.Success = true
		return
	}

	if args.LastIncludedIndex >= rf.getLastLogIndex() {
		newLog := make([]*Entry, 0)
		newLog = append(newLog, &Entry{
			Index:   args.LastIncludedIndex,
			Term:    args.LastIncludedTerm,
			Command: args.Data,
		})
		rf.CommitIndex = args.LastIncludedIndex
		rf.Logs = newLog
		reply.Success = true
	} else {
		newLog := make([]*Entry, 0)
		newLog = append(newLog, &Entry{
			Index:   args.LastIncludedIndex,
			Term:    args.LastIncludedTerm,
			Command: args.Data,
		})
		for i := args.LastIncludedIndex + 1; i <= rf.getLastLogIndex(); i++ {
			newLog = append(newLog, rf.getLog(i))
		}
		rf.CommitIndex = max(rf.CommitIndex, args.LastIncludedIndex)
		rf.Logs = newLog
		reply.Success = true
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.getSnapshotLastData())
	b := w.Bytes()
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      b,
		SnapshotTerm:  rf.getSnapshotLastTerm(),
		SnapshotIndex: rf.getSnapshotLastIndex(),
	}
	rf.applyCh <- msg
	rf.persist()
	DPrintf("[InstallSnapshot] %v get snapShot, args=%+v, now log=%+v", rf.me, args, rf.Logs)

}
