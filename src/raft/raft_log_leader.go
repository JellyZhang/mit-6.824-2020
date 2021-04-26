package raft

// leader send AppendEntries to one follower, and try to update commitIndex
func (rf *Raft) sendHeartbeat(server int, startTerm int, prevLogIndex int, prevLogTerm int, entries []*Entry, leaderCommitIndex int) {
	args := &AppendEntriesArgs{
		Term:              startTerm,
		LeaderId:          rf.me,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		Entries:           entries,
		LeaderCommitIndex: leaderCommitIndex,
	}
	reply := &AppendEntriesReply{}
	DPrintf("[sendHeartbeat] %v sendHeartbeat to %v, args=%+v", rf.me, server, args)
	if ok := rf.sendAppendEntries(server, args, reply); !ok {
		DPrintf("[sendHeartbeat] leader %v send to %v rpc error", rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		rf.logmu.Lock()
		rf.persist()
		rf.logmu.Unlock()
	}()

	// term changed after we rehold the lock.
	if startTerm != rf.currentTerm {
		return
	}

	if reply.Success == false {
		if reply.Term > startTerm {
			rf.currentTerm = max(reply.Term, rf.currentTerm)
			rf.role = Follower
			DPrintf("[sendHeartbeat] %v sendHeartbeat to %v but get a newer term, term=%v", rf.me, server, rf.currentTerm)
		} else if reply.NextTryIndex > 0 {
			// decrease this server's nextIndex and retry later.
			rf.nextIndex[server] = reply.NextTryIndex
			DPrintf("[sendHeartbeat] %v sendHeartbeat to %v but get refused, now nextIndex[i]=%v", rf.me, server, rf.nextIndex[server])
		}
		return
	}

	DPrintf("[sendHeartbeat] %v sendHeartbeat to %v succeed, args=%+v", rf.me, server, args)

	// too old request
	if prevLogIndex+len(entries) < rf.matchIndex[server] {
		return
	}

	// update matchIndex and nextIndex
	rf.matchIndex[server] = prevLogIndex + len(entries)
	rf.nextIndex[server] = rf.matchIndex[server] + 1

	// check if we can update commitIndex to newCommitIndex
	oldCommitIndex := rf.commitIndex
	newCommitIndex := rf.matchIndex[server]
	if oldCommitIndex > newCommitIndex {
		return
	}
	rf.logmu.Lock()
	newCommitTerm := rf.getLogTerm(newCommitIndex)
	rf.logmu.Unlock()
	if newCommitIndex <= oldCommitIndex || newCommitTerm != rf.currentTerm {
		//if newCommitIndex <= oldCommitIndex {
		// already commited before or trying to commit only old term entries.
		return
	}

	// count how many nodes have received logs between logs[0] and logs[newCommitIndex]
	var cnt int32 = 0
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.matchIndex[i] >= newCommitIndex {
			cnt++
		}
	}

	DPrintf("[sendHeartbeat] %v try set commitIndex to %v, old=%v, cnt=%v", rf.me, newCommitIndex, oldCommitIndex, cnt)
	// check if majority of cluster (including leader himself) has received logs of at least logs[newCommitIndex]
	if cnt+1 >= rf.getMajority() {
		for i := oldCommitIndex + 1; i <= newCommitIndex; i++ {
			rf.logmu.Lock()
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.getLog(i).Command,
				CommandIndex: i,
			}
			rf.logmu.Unlock()
			DPrintf("[sendHeartbeat] %v apply msg=%+v", rf.me, msg)
			rf.applyCh <- msg
		}
		rf.commitIndex = newCommitIndex
		DPrintf("[sendHeartbeat] %v leader now commitIndex=%v", rf.me, rf.commitIndex)
	}
	DPrintf("[sendHeartbeat] %v sendHeartbeat to %v succeed, now nextIndex[i]=%v, matchIndex[i]=%v", rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
}
