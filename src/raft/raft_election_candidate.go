package raft

// Candidate starts Election
func (rf *Raft) startElection(startTerm int) {
	rf.mu.Lock()
	rf.logmu.Lock()
	defer rf.logmu.Unlock()
	defer rf.mu.Unlock()

	// state changed after we rehold lock.
	if rf.currentTerm != startTerm || rf.role != Candidate {
		return
	}

	// vote for myself
	rf.votedFor = rf.me
	rf.getVotedTickets = 1
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
	rf.logmu.Lock()
	defer rf.logmu.Unlock()

	// state changed after we rehold the lock.
	if startTerm < rf.currentTerm || rf.role != Candidate {
		DPrintf("[askForVote] too old information, startTerm=%v, currentTerm=%v, role=%v", startTerm, rf.currentTerm, rf.role)
		return
	}

	// discovered higher term, we should change to follower.
	if reply.Term > startTerm {
		rf.currentTerm = max(reply.Term, rf.currentTerm)
		rf.role = Follower
		rf.persist()
		DPrintf("[askForVote] %v term update during requestVote from %v, now term=%v", rf.me, server, rf.currentTerm)
		return
	}

	// received 1 tickect, check if we have enough tickets.
	if reply.VoteGranted && rf.currentTerm == startTerm {
		rf.getVotedTickets++
		DPrintf("[askForVote] %v get vote from %v, now have %v", rf.me, server, rf.getVotedTickets)
		if rf.getVotedTickets >= rf.getMajority() {
			rf.role = Leader
			rf.leaderInitialization()
			DPrintf("[askForVote] %v is leader now", rf.me)
		}
		rf.persist()
		return
	}
}
