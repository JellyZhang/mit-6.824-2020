package shardctrler

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const serverTimeoutInterval = 500 * time.Millisecond

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	maxraftstate int // snapshot if log grows this big
	isLeader     atomic.Value
	notifyCh     chan Op

	// mapmu controls haveDone and configs
	mapmu    sync.Mutex
	haveDone map[int64]int64
	configs  []Config // indexed by config num
}

type Command int

const (
	JOIN = iota + 1
	LEAVE
	MOVE
	QUERY
)

type Op struct {
	// Your data here.
	OpIndex   int64
	ClientNum int64
	Command   Command
	Servers   map[int][]string // For Join, new GID -> servers mappings
	GIDs      []int            // For Leave
	Shard     int              // For Move
	GID       int              // For Move
	Num       int              // For Query, desired config number

}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.isLeader.Store(true)
	sc.notifyCh = make(chan Op, 1000)
	snapshotBytes := sc.rf.GetSnapshot()
	if len(snapshotBytes) > 0 {
		r := bytes.NewBuffer(snapshotBytes)
		d := labgob.NewDecoder(r)
		var configs []Config
		var haveDone map[int64]int64
		if d.Decode(&configs) != nil ||
			d.Decode(&haveDone) != nil {
			log.Fatalf("[listener] %v decode error", sc.me)
		} else {
			sc.configs = configs
			sc.haveDone = haveDone
		}
	} else {
		sc.configs = make([]Config, 0)
		sc.configs = append(sc.configs, Config{
			Num: 0,
		})
		sc.haveDone = make(map[int64]int64)
	}

	go sc.listener()
	return sc
}

func (sc *ShardCtrler) doJoin(op Op) {
	prev := sc.configs[len(sc.configs)-1]
	//copy
	cur := getCopyConfig(prev)

	//new
	for gid, servers := range op.Servers {
		cur.Groups[gid] = servers
	}

	reBalance(&cur)
	sc.configs = append(sc.configs, cur)
}

func (sc *ShardCtrler) doLeave(op Op) {
	prev := sc.configs[len(sc.configs)-1]
	//copy
	cur := getCopyConfig(prev)

	//new
	for _, gid := range op.GIDs {
		delete(cur.Groups, gid)
	}

	reBalance(&cur)
	sc.configs = append(sc.configs, cur)
}

func (sc *ShardCtrler) doMove(op Op) {
	prev := sc.configs[len(sc.configs)-1]
	//copy
	cur := getCopyConfig(prev)

	//new
	cur.Shards[op.Shard] = op.GID

	sc.configs = append(sc.configs, cur)
}

func (sc *ShardCtrler) listener() {
	for msg := range sc.applyCh {
		sc.mapmu.Lock()

		isLeader := sc.isLeader.Load().(bool)
		DPrintf("[listener] %v get msg=%+v, isLeader=%v", sc.me, msg, isLeader)
		if msg.CommandValid == true {
			m, ok := msg.Command.(Op)
			if !ok {
				panic("assert error")
			}
			if _, ok := sc.haveDone[m.ClientNum]; !ok || m.OpIndex != sc.haveDone[m.ClientNum] {
				if m.Command == JOIN {
					sc.doJoin(m)
				} else if m.Command == LEAVE {
					sc.doLeave(m)
				} else if m.Command == MOVE {
					sc.doMove(m)
				}
				sc.haveDone[m.ClientNum] = m.OpIndex
			}
			if sc.maxraftstate != -1 && sc.rf.GetRaftStateSize() > sc.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(sc.configs)
				e.Encode(sc.haveDone)
				bs := w.Bytes()
				sc.rf.Snapshot(msg.CommandIndex, bs)
			}
			if isLeader {
				sc.notifyCh <- m
			}
		} else {
			if sc.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				r := bytes.NewBuffer(msg.Snapshot)
				d := labgob.NewDecoder(r)
				var configs []Config
				var haveDone map[int64]int64
				if d.Decode(&configs) != nil ||
					d.Decode(&haveDone) != nil {
					log.Fatalf("[listener] %v decode error", sc.me)
				} else {
					sc.configs = configs
					sc.haveDone = haveDone
				}
				DPrintf("[listener] %v replace storage to %v", sc.me, sc.configs)
			}
		}
		sc.mapmu.Unlock()
	}
}
