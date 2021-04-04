package raft

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
	rf.logmu.Lock()
	defer rf.logmu.Unlock()
	DPrintf("[Snapshot] %v snapshot, index=%v", rf.me, index)

	if index < rf.getSnapshotLastIndex() {
		return
	}

	newLog := make([]*Entry, 0)
	newLog = append(newLog, &Entry{
		Index:   index,
		Term:    rf.getLog(index).Term,
		Command: rf.getLog(index).Command,
	})
	for i := index + 1; i <= rf.getLastLogIndex(); i++ {
		newLog = append(newLog, rf.getLog(i))
	}
	rf.Logs = newLog
	DPrintf("[Snapshot] %v snapshot success, newLog=%v", rf.me, rf.Logs)

}
