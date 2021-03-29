package raft

import (
	"sync"
	"time"
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// node response to RequestVote call.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[RequestVote] me=%v, be asked to voted to %v, his term=%v", rf.me, args.CandidateId, args.Term)

	reply.Term = rf.currentTerm

	// requestVote from old term, ignore.
	if args.Term < rf.currentTerm {
		DPrintf("[RequestVote] me=%v, too old term dont give vote, currentTerm=%v", rf.me, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// requestVote from higher term
	if args.Term > rf.currentTerm {
		DPrintf("[RequestVote] %v get bigger term=%v from %v", rf.me, args.Term, args.CandidateId)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}

	if moreUpToDate(rf.getLastLogIndex(), rf.getLastLogTerm(), args.LastLogIndex, args.LastLogTerm) {
		DPrintf("[RequestVote] %v cant give vote to %v because he is too old, myEntries=%+v, his LastLogIndex=%v, LastLogTerm=%v", rf.me, args.CandidateId, rf.logs, args.LastLogIndex, args.LastLogTerm)
		reply.VoteGranted = false
	} else if rf.votedFor == args.CandidateId || rf.votedFor == -1 {
		DPrintf("[RequestVote] %v give vote1 to %v, myEntries=%v, his LastLogIndex=%v, LastLogTerm=%v", rf.me, args.CandidateId, rf.logs, args.LastLogIndex, args.LastLogTerm)
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now().UnixNano() / 1e6
	} else {
		DPrintf("[RequestVote] %v this term has voted to %v", rf.me, rf.votedFor)
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	DPrintf("[RequestVote] %v end, dont give out vote. rf.votedFor=%v", rf.me, rf.votedFor)
	return

}

func (rf *Raft) requestVote(server int, term int, c *sync.Cond, lastLogIndex int, lastLogTerm int) {
	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := &RequestVoteReply{}
	DPrintf("[requestVote] %v send requestVote %+v to %v", rf.me, args, server)

	if ok := rf.sendRequestVote(server, args, reply); !ok {
		DPrintf("[requestVote] %v send requestVote to %v, rpc error", rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		DPrintf("[requestVote] %v term update during requestVote from %v, now term=%v", rf.me, server, rf.currentTerm)
		c.Signal()
	} else if reply.VoteGranted {
		rf.getVotedTickets++
		if rf.getVotedTickets >= rf.getMajority() {
			c.Signal()
		}
		DPrintf("[requestVote] %v get vote from %v, now have %v", rf.me, server, rf.getVotedTickets)
	} else {
		DPrintf("[requestVote] %v get vote from %v failed, votedGranted=%v", rf.me, server, reply.VoteGranted)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("[startElection] %v start election, ts=%v", rf.me, time.Now().UnixNano()/1e6)

	rf.role = Candidate
	rf.currentTerm++
	startTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.getVotedTickets = 1
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	m := sync.Mutex{}
	c := sync.NewCond(&m)
	c.L.Lock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.requestVote(i, startTerm, c, lastLogIndex, lastLogTerm)
	}
	rf.mu.Unlock()

	// be wake up when term changed or votes are enough
	c.Wait()
	rf.mu.Lock()
	if rf.currentTerm > startTerm {
		DPrintf("[startElection] %v term update during requestVote2 now term=%v", rf.me, rf.currentTerm)
		rf.role = Follower
	} else if rf.getVotedTickets >= rf.getMajority() {
		DPrintf("[startElection] me=%v is leader now, term=%v", rf.me, rf.currentTerm)
		rf.role = Leader
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = 0
		}
	} else {
		rf.role = Follower
		DPrintf("[startElection] me=%v, votes=%v, cant be leader", rf.me, rf.getVotedTickets)
	}
	c.L.Unlock()
	rf.mu.Unlock()
}
