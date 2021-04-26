package kvraft

import (
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Command int

const (
	GET = iota + 1
	PUT
	APPEND
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpIndex int64
	Command Command
	Key     string
	Value   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	isLeader    bool
	total       int
	storage     map[string]string
	commitIndex int
	logmu       sync.Mutex
	havedone    map[int64]struct{}
	notifyCh    chan Op
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.total = len(servers)
	kv.storage = make(map[string]string)
	kv.commitIndex = 0
	kv.havedone = make(map[int64]struct{})
	kv.isLeader = true
	kv.notifyCh = make(chan Op, 10000)
	go kv.listener()

	return kv
}

func (kv *KVServer) listener() {
	for msg := range kv.applyCh {
		kv.logmu.Lock()
		isLeader := kv.isLeader
		kv.logmu.Unlock()

		DPrintf("[listener] %v get msg=%+v, isLeader=%v", kv.me, msg, isLeader)
		if msg.CommandValid == true {
			if msg.Command == nil {
				continue
			}
			m, ok := msg.Command.(Op)
			if !ok {
				panic("assert error")
			}
			kv.logmu.Lock()
			if _, ok := kv.havedone[m.OpIndex]; !ok {
				if m.Command == PUT {
					kv.storage[m.Key] = m.Value
				} else if m.Command == APPEND {
					kv.storage[m.Key] = kv.storage[m.Key] + m.Value
				}
				kv.havedone[m.OpIndex] = struct{}{}
			}
			kv.logmu.Unlock()
			kv.commitIndex = msg.CommandIndex
			if isLeader {
				kv.notifyCh <- m
			}
		} else {
			bs := msg.Snapshot
			newstorage := make(map[string]string)
			if err := json.Unmarshal(bs, &newstorage); err != nil {
				panic(err)
			}
			kv.storage = newstorage
		}

		DPrintf("[listener] %v over get msg=%+v, isLeader=%v", kv.me, msg, isLeader)

	}
}
