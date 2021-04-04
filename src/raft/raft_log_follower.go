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
	if args.Term < rf.CurrentTerm {
		DPrintf("[AppendEntries] %v get low term from %v, myterm=%v, histerm=%v", rf.me, args.LeaderId, rf.CurrentTerm, args.Term)
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}

	reply.Success = true
	rf.CurrentTerm = args.Term
	rf.Role = Follower
	rf.refreshElectionTimeout()
	rf.logmu.Lock()
	rf.persist()
	rf.logmu.Unlock()
	DPrintf("[AppendEntries] %v set term to %v ", rf.me, rf.CurrentTerm)

	rf.logmu.Lock()
	// check if prevLogIndex and prevLogTerm match.
	if args.PrevLogIndex > rf.getLastLogIndex() || rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm || (args.PrevLogIndex == rf.getSnapshotLastIndex() && args.PrevLogTerm != rf.getSnapshotLastTerm()) {
		var confictTermFirstIndex int
		if args.PrevLogIndex <= rf.getLastLogIndex() {
			confictTerm := rf.getLogTerm(args.PrevLogIndex)
			confictTermFirstIndex = args.PrevLogIndex
			for confictTermFirstIndex-1 >= rf.Logs[0].Index && rf.getLogTerm(confictTermFirstIndex-1) == confictTerm {
				confictTermFirstIndex--
			}
		} else {
			confictTermFirstIndex = rf.getLastLogIndex() + 1
		}

		DPrintf("[AppendEntries] %v dont exist prev entry, rf.entry=%+v, args=%+v, giveback nexttry=%v", rf.me, rf.Logs, args, confictTermFirstIndex)
		reply.NextTryIndex = confictTermFirstIndex
		reply.Success = false
		rf.logmu.Unlock()
		return
	}

	// if there are some new logs, append it to our log.
	if len(args.Entries) > 0 {
		newLog := make([]*Entry, 0)
		for i := rf.Logs[0].Index; i <= args.PrevLogIndex; i++ {
			newLog = append(newLog, rf.getLog(i))
		}
		newLog = append(newLog, args.Entries...)
		rf.Logs = newLog
		//rf.Logs = rf.Logs[0 : args.PrevLogIndex-rf.snapShotLastIndex]
		//rf.Logs = append(rf.Logs, args.Entries...)
		rf.persist()
	}

	// update commitIndex.
	oldCommit := rf.CommitIndex
	newCommit := min(args.LeaderCommitIndex, rf.getLastLogIndex())
	DPrintf("[AppendEntries] %v try check commitIndex, oldCommit=%v, new=%v", rf.me, oldCommit, newCommit)
	rf.logmu.Unlock()

	// if our commitIndex is updated, then we apply the logs between them.
	if oldCommit < newCommit {
		for i := oldCommit + 1; i <= newCommit; i++ {
			//msg := ApplyMsg{
			//CommandValid: true,
			//Command:      rf.getLog(i).Command,
			//CommandIndex: i,
			//}
			//DPrintf("[AppendEntries] %v apply, msg=%+v", rf.me, msg)
			//rf.applyLog(msg)
			//rf.CommitIndex = i
			//rf.LastApplied = i
			rf.applyLog(i)
			rf.CommitIndex = i
			rf.LastApplied = i
		}
		rf.logmu.Lock()
		rf.persist()
		rf.logmu.Unlock()
	}
	return
}
