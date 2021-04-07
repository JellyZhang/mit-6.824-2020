package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logmu.Lock()
	defer rf.logmu.Unlock()
	DPrintf("[InstallSnapshot] %v get InstallSnapshot, args=%+v", rf.me, args)

	if args.Term < rf.currentTerm {
		DPrintf("[InstallSnapshot] %v get low term from %v, myterm=%v, histerm=%v", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		return
	}

	rf.currentTerm = args.Term
	rf.role = Follower
	reply.Term = rf.currentTerm

	// already have this snapShot
	if args.LastIncludedIndex <= rf.getSnapshotLastIndex() {
		return
	}

	newLog := make([]*Entry, 0)
	newLog = append(newLog, &Entry{
		Index:   args.LastIncludedIndex,
		Term:    args.LastIncludedTerm,
		Command: args.Data,
	})

	// retain log entries that following Snapshot
	if args.LastIncludedIndex < rf.getLastLogIndex() {
		for i := args.LastIncludedIndex + 1; i <= rf.getLastLogIndex(); i++ {
			newLog = append(newLog, rf.getLog(i))
		}
	}
	rf.logs = newLog
	rf.commitIndex = args.LastIncludedIndex
	rf.snapshotData = args.Data

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.getSnapshotLastData(),
		SnapshotTerm:  rf.getSnapshotLastTerm(),
		SnapshotIndex: rf.getSnapshotLastIndex(),
	}
	rf.applyCh <- msg
	rf.persist()
	DPrintf("[InstallSnapshot] %v get snapShot, args=%+v, now log=%+v", rf.me, args, rf.logs)

}
