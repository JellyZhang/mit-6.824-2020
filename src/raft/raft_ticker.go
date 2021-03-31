package raft

import (
	"time"
)

const leaderHeartBeatDuration = time.Duration(150) * time.Millisecond

// The ticker for leader to send AppendEntries requests periodly
func (rf *Raft) leaderTicker() {
	for rf.killed() == false {
		time.Sleep(leaderHeartBeatDuration)
		rf.mu.Lock()
		r := rf.role
		t := rf.currentTerm
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		before := rf.lastHeartbeat
		rf.mu.Unlock()

		// sleep for a while and check if we received leader's heartsbeats during our sleep
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
