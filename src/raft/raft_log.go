package raft

import (
	"log"
)

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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[AppendEntries] %v get AppendEntries, args=%+v", rf.me, args)

	// AppendEntries request from old term, ignore.
	if args.Term < rf.currentTerm {
		DPrintf("[AppendEntries] %v get low term from %v, myterm=%v, histerm=%v", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Success = true
	rf.role = Follower
	rf.refreshElectionTimeout()
	DPrintf("[AppendEntries] %v set leaderIndex=%v, set term to%v ", rf.me, args.LeaderId, rf.currentTerm)
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[AppendEntries] dont exist prev entry, rf.entry=%+v, args.Entries=%+v", rf.logs, args.Entries)
		reply.Success = false
		return
	}

	//if len(args.Entries) == 0 {
	//oldCommit := rf.commitIndex
	//rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs)-1)
	//DPrintf("[AppendEntries] %v try check commitIndex, oldCommit=%v, new=%v", rf.me, oldCommit, rf.commitIndex)
	//if oldCommit < rf.commitIndex {
	//for i := oldCommit + 1; i <= rf.commitIndex; i++ {
	//valid := false
	//if i >= rf.lastApplied {
	//valid = true
	//rf.lastApplied = i
	//}
	//msg := ApplyMsg{
	//CommandValid: valid,
	//Command:      rf.logs[i].Command,
	//CommandIndex: i,
	//}
	//DPrintf("[AppendEntries] %v apply, msg=%+v", rf.me, msg)
	//rf.applyCh <- msg
	//}
	//}
	//return
	//} else {
	//rf.logs = rf.logs[0 : args.PrevLogIndex+1]
	//rf.logs = append(rf.logs, args.Entries...)
	//DPrintf("[AppendEntries] %v now have new log, logs=%+v, args=%+v", rf.me, rf.logs, args)
	//oldCommit := rf.commitIndex
	//rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs)-1)
	//DPrintf("[AppendEntries] %v try check commitIndex, oldCommit=%v, new=%v", rf.me, oldCommit, rf.commitIndex)
	//if oldCommit < rf.commitIndex {
	//for i := oldCommit + 1; i <= rf.commitIndex; i++ {
	//valid := false
	//if i >= rf.lastApplied {
	//valid = true
	//rf.lastApplied = i
	//}
	//msg := ApplyMsg{
	//CommandValid: valid,
	//Command:      rf.logs[i].Command,
	//CommandIndex: i,
	//}
	//DPrintf("[AppendEntries] %v apply, msg=%+v", rf.me, msg)
	//rf.applyCh <- msg
	//}
	//}
	//return
	//}

	if len(args.Entries) > 0 {
		rf.logs = rf.logs[0 : args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
	}

	oldCommit := rf.commitIndex
	rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs)-1)
	DPrintf("[AppendEntries] %v try check commitIndex, oldCommit=%v, new=%v", rf.me, oldCommit, rf.commitIndex)
	if oldCommit < rf.commitIndex {
		for i := oldCommit + 1; i <= rf.commitIndex; i++ {
			valid := false
			if i >= rf.lastApplied {
				valid = true
				rf.lastApplied = i
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
	if ok := rf.sendAppendEntries(server, args, reply); !ok {
		DPrintf("[sendHeartbeat] leader %v send to %v rpc error", rf.me, server)
		return
	}

	rf.mu.Lock()

	if reply.Success == false {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = Follower
			DPrintf("[sendHeartbeat] %v sendHeartbeat to %v but get a newer term, term=%v", rf.me, server, rf.currentTerm)
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
