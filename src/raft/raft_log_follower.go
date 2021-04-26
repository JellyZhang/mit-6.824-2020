package raft

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []*Entry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// tell leader next time which index to try if this one fails.
	// this is used for leader to optimize the decrement of nextIndex[]
	// see $5.3 (page 7-8), the quoted section
	NextTryIndex int
}

// follower response to Leader's AppendEntries call.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		//rf.logmu.Lock()
		//rf.persist()
		//rf.logmu.Unlock()
	}()
	DPrintf("[AppendEntries] %v get AppendEntries, args=%+v", rf.me, args)

	// AppendEntries request from old term, ignore.
	if args.Term < rf.currentTerm {
		DPrintf("[AppendEntries] %v get low term from %v, myterm=%v, histerm=%v", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Success = true
	rf.currentTerm = args.Term
	rf.role = Follower
	rf.refreshElectionTimeout()

	DPrintf("[AppendEntries] %v set term to %v ", rf.me, rf.currentTerm)

	rf.logmu.Lock()

	// too old request
	if args.PrevLogIndex < rf.getSnapshotLastIndex() {
		rf.logmu.Unlock()
		return
	}
	// check if prevLogIndex and prevLogTerm match.
	if args.PrevLogIndex > rf.getLastLogIndex() || rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm || (args.PrevLogIndex == rf.getSnapshotLastIndex() && args.PrevLogTerm != rf.getSnapshotLastTerm()) {
		var confictTermFirstIndex int
		if args.PrevLogIndex <= rf.getLastLogIndex() {
			confictTerm := rf.getLogTerm(args.PrevLogIndex)
			confictTermFirstIndex = args.PrevLogIndex
			for confictTermFirstIndex-1 >= rf.getSnapshotLastIndex() && rf.getLogTerm(confictTermFirstIndex-1) == confictTerm {
				confictTermFirstIndex--
			}
		} else {
			confictTermFirstIndex = rf.getLastLogIndex() + 1
		}

		DPrintf("[AppendEntries] %v dont exist prev entry, rf.entry=%+v, args=%+v, giveback nexttry=%v", rf.me, rf.logs, args, confictTermFirstIndex)
		reply.NextTryIndex = confictTermFirstIndex
		reply.Success = false
		rf.logmu.Unlock()
		return
	}

	// if there are some new logs, append it to our log.
	if len(args.Entries) > 0 {
		newLog := make([]*Entry, 0)
		for i := rf.logs[0].Index; i <= args.PrevLogIndex; i++ {
			newLog = append(newLog, rf.getLog(i))
		}
		newLog = append(newLog, args.Entries...)
		rf.logs = newLog
		rf.persist()
	}

	// update commitIndex.
	oldCommitIndex := rf.commitIndex
	newCommitIndex := min(args.LeaderCommitIndex, rf.getLastLogIndex())
	DPrintf("[AppendEntries] %v try check commitIndex, oldCommit=%v, new=%v", rf.me, oldCommitIndex, newCommitIndex)
	rf.logmu.Unlock()

	// if our commitIndex is updated, then we apply the logs between them.
	if oldCommitIndex < newCommitIndex {
		for i := oldCommitIndex + 1; i <= newCommitIndex; i++ {
			rf.logmu.Lock()
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.getLog(i).Command,
				CommandIndex: i,
			}
			rf.logmu.Unlock()
			DPrintf("[applyLog] %v apply msg=%+v", rf.me, msg)
			rf.applyCh <- msg
		}
		rf.commitIndex = newCommitIndex

		rf.logmu.Lock()
		rf.persist()
		rf.logmu.Unlock()
	}
	return
}
