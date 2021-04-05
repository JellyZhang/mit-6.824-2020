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

	// term changed after we rehold the lock.
	if startTerm != rf.CurrentTerm {
		return
	}

	if reply.Success == false {
		if reply.Term > startTerm {
			rf.CurrentTerm = max(reply.Term, rf.CurrentTerm)
			rf.Role = Follower
			DPrintf("[sendHeartbeat] %v sendHeartbeat to %v but get a newer term, term=%v", rf.me, server, rf.CurrentTerm)
		} else if reply.NextTryIndex > 0 {
			// decrease this server's nextIndex and retry later.
			rf.NextIndex[server] = reply.NextTryIndex
			DPrintf("[sendHeartbeat] %v sendHeartbeat to %v but get refused, now nextIndex[i]=%v", rf.me, server, rf.NextIndex[server])
		}
		rf.logmu.Lock()
		rf.persist()
		rf.logmu.Unlock()
		return
	}

	DPrintf("[sendHeartbeat] %v sendHeartbeat to %v succeed, args=%+v", rf.me, server, args)
	rf.MatchIndex[server] = prevLogIndex + len(entries)
	rf.NextIndex[server] = rf.MatchIndex[server] + 1
	rf.logmu.Lock()
	rf.persist()
	rf.logmu.Unlock()

	// check if we can update commitIndex to newCommitIndex
	oldCommitIndex := rf.CommitIndex
	newCommitIndex := rf.MatchIndex[server]
	rf.logmu.Lock()
	newCommitTerm := rf.getLogTerm(newCommitIndex)
	rf.logmu.Unlock()
	if newCommitIndex <= oldCommitIndex || newCommitTerm != rf.CurrentTerm {
		// already commited before or trying to commit only old term entries.
		return
	}

	// count how many nodes have received logs between logs[0] and logs[newCommitIndex]
	var cnt int32 = 0
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.MatchIndex[i] >= newCommitIndex {
			cnt++
		}
	}

	DPrintf("[sendHeartbeat] %v try set commitIndex to %v, cnt=%v", rf.me, newCommitIndex, cnt)
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
		rf.CommitIndex = newCommitIndex
		rf.LastApplied = newCommitIndex
		DPrintf("[sendHeartbeat] %v leader now commitIndex=%v", rf.me, rf.CommitIndex)
		rf.logmu.Lock()
		rf.persist()
		rf.logmu.Unlock()
	}
	DPrintf("[sendHeartbeat] %v sendHeartbeat to %v succeed, now nextIndex[i]=%v, matchIndex[i]=%v", rf.me, server, rf.NextIndex[server], rf.MatchIndex[server])
}
