package raft

func (rf *Raft) doInstallSnapshot(server int, startTerm int, LastIncludedIndex int, LastIncludedTerm int, LastIncludedData []byte) {
	args := &InstallSnapshotArgs{
		Term:              startTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: LastIncludedIndex,
		LastIncludedTerm:  LastIncludedTerm,
		Data:              LastIncludedData,
	}
	reply := &InstallSnapshotReply{}
	DPrintf("[doInstallSnapshot] leader %v send snapshot to %v, args=%+v", rf.me, server, args)
	if ok := rf.sendInstallSnapshot(server, args, reply); !ok {
		DPrintf("[doInstallSnapshot] leader %v send snapshot to %v rpc error", rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// term changed after we rehold the lock.
	if startTerm != rf.currentTerm {
		return
	}

	if reply.Term > startTerm {
		rf.currentTerm = max(reply.Term, rf.currentTerm)
		rf.role = Follower
		return
	}

	rf.nextIndex[server] = LastIncludedIndex + 1

	DPrintf("[doInstallSnapshot] %v send snapshot to %v success, args=%+v", rf.me, server, args)

}
