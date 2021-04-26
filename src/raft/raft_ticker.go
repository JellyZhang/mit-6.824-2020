package raft

import (
	"time"
)

const leaderHeartBeatDuration = time.Duration(100) * time.Millisecond

// The ticker for leader to send AppendEntries requests periodly
func (rf *Raft) leaderTicker() {
	for rf.killed() == false {
		time.Sleep(leaderHeartBeatDuration)
		rf.leaderHandler()
	}
}

func (rf *Raft) leaderHandler() {
	rf.mu.Lock()
	r := rf.role
	t := rf.currentTerm
	if r == Leader {
		DPrintf("[leaderHandler] %v start to send heartsbeats", rf.me)
		rf.logmu.Lock()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			prevLogIndex := rf.nextIndex[i] - 1
			if prevLogIndex < rf.getSnapshotLastIndex() {
				DPrintf("[leaderHandler] leader %v choose to install snapShot to %v, prevLogIndex=%v", rf.me, i, prevLogIndex)
				go rf.doInstallSnapshot(i, t, rf.getSnapshotLastIndex(), rf.getSnapshotLastTerm(), rf.getSnapshotLastData())
			} else {
				prevLogTerm := rf.getLogTerm(prevLogIndex)
				DPrintf("[leaderHandler] %v prevLogIndex=%v, prevLogTerm=%v", i, prevLogIndex, prevLogTerm)
				entries := make([]*Entry, 0)
				for j := prevLogIndex + 1; j <= rf.getLastLogIndex(); j++ {
					entries = append(entries, rf.getLog(j))
				}
				leaderCommitIndex := rf.commitIndex
				go rf.sendHeartbeat(i, t, prevLogIndex, prevLogTerm, entries, leaderCommitIndex)
			}
		}
		rf.logmu.Unlock()
	}
	rf.mu.Unlock()
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
		DPrintf("[electionTicker] check election timeout, me=%v, role=%v", rf.me, role)

		// if this node dont get leader's heartsbeats during sleep, then start election
		if before == after && role != Leader {
			DPrintf("[electionTicker] start election, me=%v, role=%v", rf.me, role)
			rf.currentTerm++
			rf.role = Candidate
			startTerm := rf.currentTerm
			go rf.startElection(startTerm)
		}
		rf.mu.Unlock()
	}
}
