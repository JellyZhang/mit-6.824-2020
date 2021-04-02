package raft

import (
	"time"
)

const leaderHeartBeatDuration = time.Duration(100) * time.Millisecond

// The ticker for leader to send AppendEntries requests periodly
func (rf *Raft) leaderTicker() {
	for rf.killed() == false {
		time.Sleep(leaderHeartBeatDuration)
		rf.mu.Lock()
		r := rf.Role
		t := rf.CurrentTerm
		if r == Leader {
			DPrintf("[leaderTicker] %v send heartsbeats", rf.me)
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				prevLogIndex := rf.NextIndex[i] - 1
				if prevLogIndex == -1 {
					DPrintf("[panic] me=%v, i=%v, nextIndex=%v", rf.me, i, rf.NextIndex)
				}
				prevLogTerm := rf.Logs[prevLogIndex].Term
				entries := make([]*Entry, 0)
				for j := rf.NextIndex[i]; j < len(rf.Logs); j++ {
					entries = append(entries, rf.Logs[j])
				}
				leaderCommitIndex := rf.CommitIndex
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
		before := rf.LastHeartbeat
		r := rf.Role
		rf.mu.Unlock()
		if r == Leader {
			time.Sleep(getElectionTimeout() * time.Millisecond)
			continue
		}
		// sleep for a while and check if we received leader's heartsbeats during our sleep
		time.Sleep(getElectionTimeout() * time.Millisecond)

		rf.mu.Lock()
		after := rf.LastHeartbeat
		role := rf.Role
		DPrintf("[electionTicker] check election timeout, me=%v", rf.me)

		// if this node dont get leader's heartsbeats during sleep, then start election
		if before == after && role != Leader {
			DPrintf("[electionTicker] start election, me=%v, role=%v", rf.me, role)
			rf.CurrentTerm++
			rf.Role = Candidate
			startTerm := rf.CurrentTerm
			go rf.startElection(startTerm)
		} else {
			DPrintf("[electionTicker] dont start election, me=%v, before=%v, after=%v, r=%v", rf.me, before, after, role)
		}
		rf.mu.Unlock()
	}
}
