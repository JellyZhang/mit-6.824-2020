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

	// check if prevLogIndex and prevLogTerm match.
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[AppendEntries] dont exist prev entry, rf.entry=%+v, args.Entries=%+v", rf.logs, args.Entries)
		confictTermFirstIndex := -1
		if args.PrevLogIndex < len(rf.logs) {
			confictTerm := rf.logs[args.PrevLogIndex].Term
			confictTermFirstIndex = args.PrevLogIndex
			for rf.logs[confictTermFirstIndex-1].Term == confictTerm {
				confictTermFirstIndex--
			}
		} else {
			confictTermFirstIndex = rf.getLastLogIndex()
		}

		reply.NextTryIndex = confictTermFirstIndex
		reply.Success = false
		return
	}

	// if there are some new logs, append it to our log.
	if len(args.Entries) > 0 {
		rf.logs = rf.logs[0 : args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
	}

	// update commitIndex.
	oldCommit := rf.commitIndex
	rf.commitIndex = min(args.LeaderCommitIndex, rf.getLastLogIndex())
	DPrintf("[AppendEntries] %v try check commitIndex, oldCommit=%v, new=%v", rf.me, oldCommit, rf.commitIndex)

	// if our commitIndex is updated, then we apply the logs between them.
	if oldCommit < rf.commitIndex {
		for i := oldCommit + 1; i <= rf.commitIndex; i++ {
			valid := false
			if i >= rf.lastApplied {
				valid = true
				rf.lastApplied = i
			}
			msg := ApplyMsg{
				CommandValid: valid,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			}
			DPrintf("[AppendEntries] %v apply, msg=%+v", rf.me, msg)
			rf.applyLog(msg)
		}
	}
	return
}
