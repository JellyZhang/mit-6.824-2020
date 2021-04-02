package raft

import (
	"sync"
)

// Candidate starts Election
func (rf *Raft) startElection(startTerm int) {
	rf.mu.Lock()
	if rf.CurrentTerm != startTerm {
		return
	}
	rf.VotedFor = rf.me
	rf.GetVotedTickets = 1
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	DPrintf("[startElection] %v start election, term=%v, lastLogIndex=%v, lastLogTerm=%v", rf.me, startTerm, lastLogIndex, lastLogTerm)
	rf.persist()

	// conditional lock, used to alert candidate if we get a enough votes or our term changed
	m := sync.Mutex{}
	cond := sync.NewCond(&m)
	cond.L.Lock()

	// start goroutines to send RequestVote per server
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.askForVote(server, startTerm, cond, lastLogIndex, lastLogTerm)
	}
	rf.mu.Unlock()

	// be wake up when term changed or votes are enough. (wake by cond.Signal())
	cond.Wait()
	rf.mu.Lock()

	// check if we have enough tickets to be leader.
	if rf.GetVotedTickets >= rf.getMajority() && rf.CurrentTerm == startTerm {
		DPrintf("[startElection] me=%v is leader now, term=%v", rf.me, rf.CurrentTerm)
		rf.Role = Leader
		rf.leaderInitialization()
		rf.persist()
	}

	cond.L.Unlock()
	rf.mu.Unlock()
}

// Candidate send request to Follower for tickets
func (rf *Raft) askForVote(server int, startTerm int, cond *sync.Cond, lastLogIndex int, lastLogTerm int) {
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
		cond.Signal()
		return
	}

	// received 1 tickect, check if we have enough tickets.
	if reply.VoteGranted && rf.CurrentTerm == startTerm {
		rf.GetVotedTickets++
		rf.persist()
		if rf.GetVotedTickets >= rf.getMajority() {
			cond.Signal()
		}
		DPrintf("[askForVote] %v get vote from %v, now have %v", rf.me, server, rf.GetVotedTickets)
	}
}

//func (rf *Raft) startElection() {
//rf.mu.Lock()

//rf.Role = Candidate
//rf.CurrentTerm++
//startTerm := rf.CurrentTerm
//rf.VotedFor = rf.me
//rf.GetVotedTickets = 1
//lastLogIndex := rf.getLastLogIndex()
//lastLogTerm := rf.getLastLogTerm()
//DPrintf("[startElection] %v start election, term=%v, lastLogIndex=%v, lastLogTerm=%v", rf.me, startTerm, lastLogIndex, lastLogTerm)
//rf.persist()

//// conditional lock, used to alert candidate if we get a enough votes or our term changed
//m := sync.Mutex{}
//cond := sync.NewCond(&m)
//cond.L.Lock()

//// start goroutines to send RequestVote per server
//for server := range rf.peers {
//if server == rf.me {
//continue
//}
//go rf.askForVote(server, startTerm, cond, lastLogIndex, lastLogTerm)
//}
//rf.mu.Unlock()

//// be wake up when term changed or votes are enough. (wake by cond.Signal())
//cond.Wait()
//rf.mu.Lock()

//// check if we have enough tickets to be leader.
//if rf.GetVotedTickets >= rf.getMajority() && rf.CurrentTerm == startTerm {
//DPrintf("[startElection] me=%v is leader now, term=%v", rf.me, rf.CurrentTerm)
//rf.Role = Leader
//rf.leaderInitialization()
//rf.persist()
//}

//cond.L.Unlock()
//rf.mu.Unlock()
//}
