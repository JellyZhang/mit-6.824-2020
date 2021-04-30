package kvraft

import (
	"time"
)

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	cmd := Op{
		ClientNum: args.ClientNumber,
		OpIndex:   args.SerializeNumber,
	}
	if args.Op == "Put" {
		cmd.Command = PUT
	} else {
		cmd.Command = APPEND
	}

	cmd.Key = args.Key
	cmd.Value = args.Value
	DPrintf("[KVServer.PutAppend] %v get args=%+v", kv.me, args)
	kv.mapmu.Lock()
	havedone, ok := kv.haveDone[args.ClientNumber]
	kv.mapmu.Unlock()
	if ok && havedone == args.SerializeNumber {
		DPrintf("[KVServer.PutAppend] %v find already done, key=%v, value=%v", kv.me, args.Key, args.Value)
		reply.Err = OK
		return
	}

	for true {
		index, _, isLeader := kv.rf.Start(cmd)
		kv.isLeader.Store(isLeader)
		if !isLeader {
			DPrintf("[KVServer.PutAppend] %v is not leader", kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		DPrintf("[KVServer.PutAppend] %v success start, index=%v, key=%v, value=%v ", kv.me, index, args.Key, args.Value)

		for start := time.Now(); time.Since(start) < serverTimeoutInterval; {
			select {
			case <-time.After(serverTimeoutInterval):
				DPrintf("[KVServer.PutAppend] %v time out", kv.me)
				reply.Err = ErrWrongLeader
				return
			case notify := <-kv.notifyCh:
				if notify.OpIndex == args.SerializeNumber {
					reply.Err = OK
					DPrintf("[KVServer.PutAppend] %v success receive key=%v, value=%v", kv.me, args.Key, args.Value)
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
