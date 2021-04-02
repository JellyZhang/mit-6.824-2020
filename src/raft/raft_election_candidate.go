package raft

// Candidate starts Election
func (rf *Raft) startElection(startTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// state changed after we rehold lock.
	if rf.CurrentTerm != startTerm || rf.Role != Candidate {
		return
	}
	rf.VotedFor = rf.me
	rf.GetVotedTickets = 1
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	DPrintf("[startElection] %v start election, term=%v, lastLogIndex=%v, lastLogTerm=%v", rf.me, startTerm, lastLogIndex, lastLogTerm)
	rf.persist()

	// start goroutines to send RequestVote per server
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.askForVote(server, startTerm, lastLogIndex, lastLogTerm)
	}
}

// Candidate send request to Follower for tickets
func (rf *Raft) askForVote(server int, startTerm int, lastLogIndex int, lastLogTerm int) {
	args := &RequestVoteArgs{
		Term:         startTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := &RequestVoteReply{}
	DPrintf("[askForVote] %v send requestVote %+v to %v", rf.me, args, server)

	// do rpc call
	if ok := rf.sendRequestVote(server, args, reply); !ok {
		DPrintf("[askForVote] %v send requestVote to %v, rpc error", rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// state changed after we rehold the lock.
	if startTerm < rf.CurrentTerm || rf.Role != Candidate {
		DPrintf("[askForVote] too old information, startTerm=%v, currentTerm=%v, role=%v", startTerm, rf.CurrentTerm, rf.Role)
		return
	}

	// discovered higher term, we should change to follower.
	if reply.Term > startTerm {
		rf.CurrentTerm = max(reply.Term, rf.CurrentTerm)
		rf.Role = Follower
		rf.persist()
		DPrintf("[askForVote] %v term update during requestVote from %v, now term=%v", rf.me, server, rf.CurrentTerm)
		return
	}

	// received 1 tickect, check if we have enough tickets.
	if reply.VoteGranted && rf.CurrentTerm == startTerm {
		rf.GetVotedTickets++
		if rf.GetVotedTickets >= rf.getMajority() {
			rf.Role = Leader
			rf.leaderInitialization()
		}
		rf.persist()
		DPrintf("[askForVote] %v get vote from %v, now have %v", rf.me, server, rf.GetVotedTickets)
		return
	}
}
