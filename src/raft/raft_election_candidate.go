package raft

import (
	"sync"
	"time"
)

// Candidate starts Election
func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("[startElection] %v start election, ts=%v", rf.me, time.Now().UnixNano()/1e6)

	rf.role = Candidate
	rf.currentTerm++
	startTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.getVotedTickets = 1
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

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
	if rf.getVotedTickets >= rf.getMajority() {
		DPrintf("[startElection] me=%v is leader now, term=%v", rf.me, rf.currentTerm)
		rf.role = Leader
		rf.leaderInitialization()
	}

	cond.L.Unlock()
	rf.mu.Unlock()
}

// Candidate send request to Follower for tickets
func (rf *Raft) askForVote(server int, term int, cond *sync.Cond, lastLogIndex int, lastLogTerm int) {
	args := &RequestVoteArgs{
		Term:         term,
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

	// discovered higher term, we should change to follower.
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		DPrintf("[askForVote] %v term update during requestVote from %v, now term=%v", rf.me, server, rf.currentTerm)
		cond.Signal()
		return
	}

	// received 1 tickect, check if we have enough tickets.
	if reply.VoteGranted {
		rf.getVotedTickets++
		if rf.getVotedTickets >= rf.getMajority() {
			cond.Signal()
		}
		DPrintf("[askForVote] %v get vote from %v, now have %v", rf.me, server, rf.getVotedTickets)
	}
}
