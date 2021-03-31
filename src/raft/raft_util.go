package raft

import (
	"math/rand"
	"time"
)

type Role int32

const (
	Follower = iota
	Candidate
	Leader
)

// return a random electionTimeout between 150ms~450ms
// according to guidance:
// Because the tester limits you to 10 heartbeats per second, you will have to use an election timeout larger than the paper's 150 to 300 milliseconds, but not too large, because then you may fail to elect a leader within five seconds.
func getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(300) + 150)
}

// return true if log1 is strictly more up-to-date than log2
func moreUpToDate(lastLogIndex1 int, lastLogTerm1 int, lastLogIndex2 int, lastLogTerm2 int) bool {
	ans := false
	if lastLogTerm1 != lastLogTerm2 {
		ans = lastLogTerm1 > lastLogTerm2
	} else {
		ans = lastLogIndex1 > lastLogIndex2
	}
	DPrintf("[moreuptodate] %v %v , %v %v, ans=%v", lastLogIndex1, lastLogTerm1, lastLogIndex2, lastLogTerm2, ans)
	return ans
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[rf.getLastLogIndex()].Term
}

// initialization some variables when rf become a leader
func (rf *Raft) leaderInitialization() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for server := range rf.nextIndex {
		rf.nextIndex[server] = rf.getLastLogIndex() + 1
	}
}

func (rf *Raft) appendLog(entry *Entry) {
	rf.logs = append(rf.logs, entry)
}

func (rf *Raft) applyLog(msg ApplyMsg) {
	rf.applyCh <- msg
}

func (rf *Raft) getMajority() int32 {
	return int32((len(rf.peers) / 2) + 1)
}

func (rf *Raft) refreshElectionTimeout() {
	rf.lastHeartbeat = time.Now().UnixNano() / 1e6
}

func (rf *Raft) isLeader() bool {
	return (rf.role == Leader)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
