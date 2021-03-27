package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int32

const (
	Follower = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	leaderIndex     int
	role            Role
	term            int
	votedFor        int
	getVotedTickets int32
	lastHeartbeat   int64
	logs            []*Entry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	applyCh         chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.term
	isleader = (rf.role == Leader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//

func getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(300) + 150)
}

func moreuptodate(lastLogIndex1 int, lastLogTerm1 int, lastLogIndex2 int, lastLogTerm2 int) bool {
	ans := false
	if lastLogTerm1 != lastLogTerm2 {
		ans = lastLogTerm1 > lastLogTerm2
	} else {
		ans = lastLogIndex1 > lastLogIndex2
	}
	DPrintf("[moreuptodate] %v %v , %v %v, ans=%v", lastLogIndex1, lastLogTerm1, lastLogIndex2, lastLogTerm2, ans)
	return ans
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[RequestVote] me=%v, be asked to voted to %v, his term=%v", rf.me, args.CandidateId, args.Term)

	reply.Term = rf.term
	if args.Term < rf.term {
		DPrintf("[RequestVote] me=%v, too old term dont give vote, currentTerm=%v", rf.me, rf.term)
		reply.VoteGranted = false
		return
	}
	if args.Term == rf.term {
		if moreuptodate(len(rf.logs)-1, rf.logs[len(rf.logs)-1].Term, args.LastLogIndex, args.LastLogTerm) {
			DPrintf("[RequestVote] %v cant give vote to %v because he is too old, myEntries=%+v, his LastLogIndex=%v, LastLogTerm=%v", rf.me, args.CandidateId, rf.logs, args.LastLogIndex, args.LastLogTerm)
			reply.VoteGranted = false
			reply.Term = rf.term
			return
		} else if rf.votedFor == args.CandidateId || rf.votedFor == -1 {
			DPrintf("[RequestVote] %v give vote1 to %v, myEntries=%v, his LastLogIndex=%v, LastLogTerm=%v", rf.me, args.CandidateId, rf.logs, args.LastLogIndex, args.LastLogTerm)
			rf.votedFor = args.CandidateId
			rf.term = args.Term
			reply.VoteGranted = true
			rf.lastHeartbeat = time.Now().UnixNano() / 1e6
			return
		} else {
			DPrintf("[RequestVote] %v this term has voted to %v", rf.me, rf.votedFor)
			reply.VoteGranted = false
			return
		}
	}
	if args.Term > rf.term {
		DPrintf("[RequestVote] %v get bigger term=%v from %v", rf.me, args.Term, args.CandidateId)
		if moreuptodate(len(rf.logs)-1, rf.logs[len(rf.logs)-1].Term, args.LastLogIndex, args.LastLogTerm) {
			DPrintf("[RequestVote] %v can not give vote to %v because too old log", rf.me, args.CandidateId)
			rf.term = args.Term
			rf.role = Follower
			rf.votedFor = -1
			reply.VoteGranted = false
			reply.Term = rf.term
		} else {
			rf.term = args.Term
			rf.votedFor = args.CandidateId
			rf.role = Follower
			DPrintf("[RequestVote] %v change term to %v, role to %v", rf.me, rf.term, rf.role)
			reply.VoteGranted = true
			reply.Term = rf.term
			rf.lastHeartbeat = time.Now().UnixNano() / 1e6
		}
		return
	}
	DPrintf("[RequestVote] %v end, dont give out vote. rf.votedFor=%v", rf.me, rf.votedFor)

}

type Entry struct {
	Term    int
	Command interface{}
}

func (e *Entry) String() string {
	return fmt.Sprintf("{term=%v command=%v} ", e.Term, e.Command)
}

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
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[AppendEntries] %v get AppendEntries, args=%+v", rf.me, args)
	if args.Term < rf.term {
		DPrintf("[AppendEntries] %v get low term from %v, myterm=%v, histerm=%v", rf.me, args.LeaderId, rf.term, args.Term)
		reply.Success = false
		reply.Term = rf.term
		return
	} else if args.Term >= rf.term {
		reply.Success = true
		rf.role = Follower
		rf.lastHeartbeat = time.Now().UnixNano() / 1e6
		rf.leaderIndex = args.LeaderId
		DPrintf("[AppendEntries] %v set leaderIndex=%v, set term to%v ", rf.me, args.LeaderId, rf.term)
		if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			DPrintf("[AppendEntries] dont exist prev entry, rf.entry=%+v, args.Entries=%+v", rf.logs, args.Entries)
			reply.Success = false
			return
		} else if len(args.Entries) == 0 {
			oldCommit := rf.commitIndex
			rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs)-1)
			DPrintf("[AppendEntries] %v try check commitIndex, oldCommit=%v, new=%v", rf.me, oldCommit, rf.commitIndex)
			if oldCommit < rf.commitIndex {
				for i := oldCommit + 1; i <= rf.commitIndex; i++ {
					valid := false
					if i >= rf.lastApplied {
						valid = true
						rf.lastApplied = i
					} else {
						DPrintf("[AppendEntries] alert,  %v lastApplied=%v, oldCommit=%v, new=%v", rf.me, rf.lastApplied, oldCommit, rf.commitIndex)
					}
					msg := ApplyMsg{
						CommandValid: valid,
						Command:      rf.logs[i].Command,
						CommandIndex: i,
					}
					DPrintf("[AppendEntries] %v apply, msg=%+v", rf.me, msg)
					rf.applyCh <- msg
				}
			}
			return
		} else {
			rf.logs = rf.logs[0 : args.PrevLogIndex+1]
			rf.logs = append(rf.logs, args.Entries...)
			DPrintf("[AppendEntries] %v now have new log, logs=%+v, args=%+v", rf.me, rf.logs, args)
			oldCommit := rf.commitIndex
			rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs)-1)
			DPrintf("[AppendEntries] %v now have commitIndex=%v", rf.me, rf.commitIndex)
			DPrintf("[AppendEntries] %v try check commitIndex, oldCommit=%v, new=%v", rf.me, oldCommit, rf.commitIndex)
			if oldCommit < rf.commitIndex {
				for i := oldCommit + 1; i <= rf.commitIndex; i++ {
					valid := false
					if i >= rf.lastApplied {
						valid = true
						rf.lastApplied = i
					} else {
						DPrintf("[AppendEntries] alert,  %v lastApplied=%v, oldCommit=%v, new=%v", rf.me, rf.lastApplied, oldCommit, rf.commitIndex)
					}
					msg := ApplyMsg{
						CommandValid: valid,
						Command:      rf.logs[i].Command,
						CommandIndex: i,
					}
					DPrintf("[AppendEntries] %v apply, msg=%+v", rf.me, msg)
					rf.applyCh <- msg
				}
			}
			return
		}

	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Start] %v get command=%v", rf.me, command)
	term := rf.term
	isLeader := (rf.role == Leader)
	if !isLeader {
		return -1, -1, isLeader
	}
	rf.logs = append(rf.logs, &Entry{
		Term:    term,
		Command: command,
	})
	index := len(rf.logs) - 1

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getMajority() int32 {
	return int32((len(rf.peers) / 2) + 1)
}

type RequestVoteResult struct {
	reply  *RequestVoteReply
	ok     bool
	server int
}

func (rf *Raft) sendHeartbeat(server int, term int, prevLogIndex int, prevLogTerm int, entries []*Entry, leaderCommitIndex int) {
	DPrintf("[sendHeartbeat] %v send heartsbeats to %v", rf.me, server)
	args := &AppendEntriesArgs{
		Term:              term,
		LeaderId:          rf.me,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		Entries:           entries,
		LeaderCommitIndex: leaderCommitIndex,
	}
	reply := &AppendEntriesReply{}
	DPrintf("[sendHeartbeat] %v sendHeartbeat to %v, args=%+v", rf.me, server, args)
	ok := rf.sendAppendEntries(server, args, reply)
	rf.mu.Lock()
	if !ok {
		DPrintf("[sendHeartbeat] leader %v send to %v rpc error", rf.me, server)
	} else if reply.Success == false {
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.role = Follower
			DPrintf("[sendHeartbeat] %v sendHeartbeat to %v but get a newer term, term=%v", rf.me, server, rf.term)
		} else {
			rf.nextIndex[server]--
			DPrintf("[sendHeartbeat] %v sendHeartbeat to %v but get refused, now nextIndex[i]=%v", rf.me, server, rf.nextIndex[server])
		}
	} else {
		DPrintf("[sendHeartbeat] %v sendHeartbeat to %v succeed, args=%+v", rf.me, server, args)
		if prevLogIndex+len(entries) < rf.matchIndex[server] {
			log.Fatalf("[sendHeartbeat] %v sendHeartbeat to %v panic, rf.matchIndex[server]=%v, args=%+v", rf.me, server, rf.matchIndex[server], args)
		}
		rf.matchIndex[server] = prevLogIndex + len(entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		target := rf.matchIndex[server]
		oldCommit := rf.commitIndex
		if target > rf.commitIndex {
			var cnt int32 = 0
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= target {
					cnt++
				}
			}
			DPrintf("[sendHeartbeat] %v try set commitIndex to %v, cnt=%v", rf.me, target, cnt)
			if cnt+1 >= rf.getMajority() {
				rf.commitIndex = target
				DPrintf("[sendHeartbeat] %v leader now commitIndex=%v", rf.me, rf.commitIndex)
				for i := oldCommit + 1; i <= target; i++ {
					valid := false
					if i >= rf.lastApplied {
						valid = true
						rf.lastApplied = i
					} else {
						DPrintf("[AppendEntries] alert,  %v lastApplied=%v, oldCommit=%v, new=%v", rf.me, rf.lastApplied, oldCommit, rf.commitIndex)
					}
					msg := ApplyMsg{
						CommandValid: valid,
						Command:      rf.logs[i].Command,
						CommandIndex: i,
					}
					DPrintf("[sendHeartbeat] %v apply msg=%+v", rf.me, msg)
					rf.applyCh <- msg
				}
			}
		}
		DPrintf("[sendHeartbeat] %v sendHeartbeat to %v succeed, now nextIndex[i]=%v, matchIndex[i]=%v", rf.me, server, rf.nextIndex[server], rf.matchIndex[server])

	}
	rf.mu.Unlock()

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) leaderTicker() {
	for rf.killed() == false {
		time.Sleep(time.Duration(150) * time.Millisecond)
		rf.mu.Lock()
		r := rf.role
		t := rf.term
		if r == Leader {
			DPrintf("[leaderTicker] %v send heartsbeats", rf.me)
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := rf.logs[prevLogIndex].Term
				entries := make([]*Entry, 0)
				for j := rf.nextIndex[i]; j < len(rf.logs); j++ {
					entries = append(entries, rf.logs[j])
				}
				leaderCommitIndex := rf.commitIndex
				go rf.sendHeartbeat(i, t, prevLogIndex, prevLogTerm, entries, leaderCommitIndex)
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		before := rf.lastHeartbeat
		rf.mu.Unlock()
		time.Sleep(getElectionTimeout() * time.Millisecond)
		rf.mu.Lock()
		after := rf.lastHeartbeat
		role := rf.role
		rf.mu.Unlock()
		DPrintf("[electionTicker] check election timeout, me=%v", rf.me)

		// if this node dont get leader's heartsbeats during sleep, then start election
		if before == after && role != Leader {
			DPrintf("[electionTicker] start election, me=%v, role=%v", rf.me, role)
			go rf.startElection()
		} else {
			DPrintf("[electionTicker] dont start election, me=%v, before=%v, after=%v, r=%v", rf.me, before, after, role)
		}
	}
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
	ok := rf.sendRequestVote(server, args, reply)
	rf.mu.Lock()
	if !ok {
		DPrintf("[requestVote] %v send requestVote to %v, rpc error", rf.me, server)
	} else if reply.Term > rf.term {
		rf.term = reply.Term
		DPrintf("[requestVote] %v term update during requestVote from %v, now term=%v", rf.me, server, rf.term)
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
	rf.mu.Unlock()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("[startElection] %v start election, ts=%v", rf.me, time.Now().UnixNano()/1e6)
	//if rf.role == Follower {
	//rf.role = Candidate
	//rf.term++
	//}
	rf.role = Candidate
	rf.term++
	rf.leaderIndex = -1
	startTerm := rf.term
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
	if rf.term > startTerm {
		DPrintf("[startElection] %v term update during requestVote2 now term=%v", rf.me, rf.term)
		rf.role = Follower
	} else if rf.getVotedTickets >= rf.getMajority() {
		DPrintf("[startElection] me=%v is leader now, term=%v", rf.me, rf.term)
		rf.leaderIndex = rf.me
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

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.lastHeartbeat = time.Now().UnixNano() / 1e6
	rf.votedFor = -1
	rf.leaderIndex = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logs = make([]*Entry, 0)
	rf.logs = append(rf.logs, &Entry{
		Term:    0,
		Command: "start",
	})
	rf.role = Follower
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.leaderTicker()

	return rf
}
