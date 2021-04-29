package kvraft

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

const Debug = false

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

const serverTimeoutInterval = 500 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpIndex   int64
	ClientNum int64
	Command   Command
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	isLeader atomic.Value
	notifyCh chan Op

	// mapmu controls haveDone and storage
	mapmu sync.Mutex
	//haveDone map[int64]struct{}
	haveDone map[int64]int64
	storage  map[string]string
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
	kv.isLeader.Store(true)
	kv.notifyCh = make(chan Op, 1000)
	snapshotBytes := kv.rf.GetSnapshot()
	if len(snapshotBytes) > 0 {
		r := bytes.NewBuffer(snapshotBytes)
		d := labgob.NewDecoder(r)
		var storage map[string]string
		//var haveDone map[int64]struct{}
		var haveDone map[int64]int64
		if d.Decode(&storage) != nil ||
			d.Decode(&haveDone) != nil {
			log.Fatalf("[listener] %v decode error", kv.me)
		} else {
			kv.storage = storage
			kv.haveDone = haveDone
		}
	} else {
		kv.storage = make(map[string]string)
		kv.haveDone = make(map[int64]int64)
	}
	DPrintf("[StartKVServer] %v init storage=%v", kv.me, kv.storage)
	go kv.listener()

	return kv
}

func (kv *KVServer) listener() {
	for msg := range kv.applyCh {
		kv.mapmu.Lock()

		isLeader := kv.isLeader.Load().(bool)
		DPrintf("[listener] %v get msg=%+v, isLeader=%v", kv.me, msg, isLeader)
		if msg.CommandValid == true {
			m, ok := msg.Command.(Op)
			if !ok {
				panic("assert error")
			}
			//if _, ok := kv.haveDone[m.OpIndex]; !ok {
			//if m.Command == PUT {
			//kv.storage[m.Key] = m.Value
			//} else if m.Command == APPEND {
			//kv.storage[m.Key] = kv.storage[m.Key] + m.Value
			//}
			//kv.haveDone[m.OpIndex] = struct{}{}
			//}
			//DPrintf("%v m=%v haveDone=%v, same=%v", kv.me, m, kv.haveDone, m.OpIndex == kv.haveDone)
			if _, ok := kv.haveDone[m.ClientNum]; !ok || m.OpIndex != kv.haveDone[m.ClientNum] {
				if m.Command == PUT {
					kv.storage[m.Key] = m.Value
				} else if m.Command == APPEND {
					kv.storage[m.Key] = kv.storage[m.Key] + m.Value
				}
				kv.haveDone[m.ClientNum] = m.OpIndex
			}
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.storage)
				e.Encode(kv.haveDone)
				bs := w.Bytes()
				kv.rf.Snapshot(msg.CommandIndex, bs)
			}
			if isLeader {
				kv.notifyCh <- m
			}
		} else {
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				r := bytes.NewBuffer(msg.Snapshot)
				d := labgob.NewDecoder(r)
				var storage map[string]string
				//var haveDone map[int64]struct{}
				var haveDone map[int64]int64
				if d.Decode(&storage) != nil ||
					d.Decode(&haveDone) != nil {
					log.Fatalf("[listener] %v decode error", kv.me)
				} else {
					kv.storage = storage
					kv.haveDone = haveDone
				}
				DPrintf("[listener] %v replace storage to %v", kv.me, kv.storage)
			}
		}
		kv.mapmu.Unlock()
	}
}
