package raft

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

// follower node response to RequestVote call.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[RequestVote] me=%v, be asked to voted to %v, his term=%v", rf.me, args.CandidateId, args.Term)

	reply.Term = rf.CurrentTerm

	// requestVote from old term Candidate, ignore.
	if args.Term < rf.CurrentTerm {
		DPrintf("[RequestVote] me=%v, too old term dont give vote, currentTerm=%v", rf.me, rf.CurrentTerm)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	// requestVote from higher term, update our term
	if args.Term > rf.CurrentTerm {
		DPrintf("[RequestVote] %v get bigger term=%v from %v", rf.me, args.Term, args.CandidateId)
		rf.CurrentTerm = args.Term
		rf.Role = Follower
		rf.VotedFor = -1
		rf.persist()
	}

	// Candidate's log should at least up-to-date as Follower's log.
	// refer to 5.4.1
	if moreUpToDate(rf.getLastLogIndex(), rf.getLastLogTerm(), args.LastLogIndex, args.LastLogTerm) {
		DPrintf("[RequestVote] %v cant give vote to %v because he is too old, myEntries=%+v, his LastLogIndex=%v, LastLogTerm=%v", rf.me, args.CandidateId, rf.Logs, args.LastLogIndex, args.LastLogTerm)
		reply.VoteGranted = false
	} else if rf.VotedFor == args.CandidateId || rf.VotedFor == -1 {
		// Follower have voted to him or havenot vote yet, then give out tickect.
		DPrintf("[RequestVote] %v give vote to %v, myEntries=%v, his LastLogIndex=%v, LastLogTerm=%v", rf.me, args.CandidateId, rf.Logs, args.LastLogIndex, args.LastLogTerm)
		rf.VotedFor = args.CandidateId
		rf.CurrentTerm = args.Term
		reply.VoteGranted = true
		rf.refreshElectionTimeout()
		rf.persist()
	} else {
		// Follower has voted to other Candidate.
		DPrintf("[RequestVote] %v this term has voted to %v", rf.me, rf.VotedFor)
		reply.VoteGranted = false
	}

	reply.Term = rf.CurrentTerm
	return
}
