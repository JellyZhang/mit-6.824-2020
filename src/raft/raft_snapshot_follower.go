package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {}
