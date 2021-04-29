package kvraft

import (
	"time"
)

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	cmd := Op{
		ClientNum: args.ClientNumber,
		OpIndex:   args.SerializeNumber,
		Command:   GET,
		Key:       args.Key,
		Value:     "",
	}

	DPrintf("[KVServer.Get] get args=%+v", args)
	for true {
		index, _, isLeader := kv.rf.Start(cmd)
		kv.isLeader.Store(isLeader)
		if !isLeader {
			DPrintf("[KVServer.Get] %v is not leader", kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		DPrintf("[KVServer.Get] %v success start, index=%v", kv.me, index)

		for start := time.Now(); time.Since(start) < serverTimeoutInterval; {
			select {
			case <-time.After(serverTimeoutInterval):
				DPrintf("[KVServer.PutAppend] %v time out", kv.me)
				reply.Err = ErrWrongLeader
				return
			case notify := <-kv.notifyCh:
				if notify.OpIndex == args.SerializeNumber {
					kv.mapmu.Lock()
					reply.Value = kv.storage[args.Key]
					kv.mapmu.Unlock()
					reply.Err = OK
					DPrintf("[KVServer.Get] %v success receive key=%v, value=%v", kv.me, args.Key, reply.Value)
					return
				}
			}
		}
		DPrintf("[KVServer.PutAppend] %v time out", kv.me)
		kv.isLeader.Store(false)
		reply.Err = ErrWrongLeader
		return
	}
	return

}
