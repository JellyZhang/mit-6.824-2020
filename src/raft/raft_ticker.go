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
			rf.logmu.Lock()
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				prevLogIndex := rf.NextIndex[i] - 1
				if prevLogIndex < rf.getSnapshotLastIndex() {
					DPrintf("[leaderTicker] leader %v choose to install snapShot to %v, prevLogIndex=%v", rf.me, i, prevLogIndex)
					go rf.doInstallSnapshot(i, t, rf.getSnapshotLastIndex(), rf.getSnapshotLastTerm(), rf.getSnapshotLastData())
				} else {
					prevLogTerm := rf.getLogTerm(prevLogIndex)
					DPrintf("[leaderTicker] %v prevLogIndex=%v, prevLogTerm=%v", i, prevLogIndex, prevLogTerm)
					entries := make([]*Entry, 0)
					for j := prevLogIndex + 1; j <= rf.getLastLogIndex(); j++ {
						entries = append(entries, rf.getLog(j))
					}
					leaderCommitIndex := rf.CommitIndex
					go rf.sendHeartbeat(i, t, prevLogIndex, prevLogTerm, entries, leaderCommitIndex)
				}
			}
			rf.logmu.Unlock()
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
		rf.mu.Unlock()
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
		}
		rf.mu.Unlock()
	}
}
