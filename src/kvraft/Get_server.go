package kvraft

import (
	"time"
)

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	cmd := Op{
		OpIndex: args.SerializeNumber,
		Command: GET,
		Key:     args.Key,
		Value:   "",
	}

	DPrintf("[KVServer.Get] get args=%+v", args)
	for true {
		index, _, isLeader := kv.rf.Start(cmd)
		kv.logmu.Lock()
		kv.isLeader = isLeader
		kv.logmu.Unlock()
		if !isLeader {
			DPrintf("[KVServer.Get] %v is not leader", kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		DPrintf("[KVServer.Get] %v success start, index=%v", kv.me, index)

		for start := time.Now(); time.Since(start) < 500*time.Millisecond; {
			select {
			case <-time.After(500 * time.Millisecond):
				DPrintf("[KVServer.PutAppend] %v time out", kv.me)
				reply.Err = ErrWrongLeader
				return
			case notify := <-kv.notifyCh:
				//if notify.index > index {
				//DPrintf(fmt.Sprintf("%v greater panic, notify=%+v, index=%v", kv.me, notify, index))
				//continue
				//} else if notify.index < index {
				//DPrintf("warn panic, notify=%+v, index=%v", notify, index)
				//continue
				//} else if notify.index == index && notify.key == cmd.Key && notify.value == cmd.Value && notify.command == cmd.Command {
				if notify.OpIndex == args.SerializeNumber {
					reply.Index = index
					kv.logmu.Lock()
					reply.Value = kv.storage[args.Key]
					kv.havedone[args.SerializeNumber] = struct{}{}
					kv.logmu.Unlock()
					reply.Err = OK
					DPrintf("[KVServer.Get] %v success receive key=%v, value=%v", kv.me, args.Key, reply.Value)
					return
				} else {
					continue
					//DPrintf("panic, notify=%+v, index=%v", notify, index)
					//kv.logmu.Lock()
					//kv.isLeader = false
					//kv.logmu.Unlock()
					//reply.Err = ErrWrongLeader
					//return
				}

			}
		}
		DPrintf("[KVServer.PutAppend] %v time out", kv.me)
		reply.Err = ErrWrongLeader
		kv.logmu.Lock()
		kv.isLeader = false
		kv.logmu.Unlock()
		return
		//reply.Index = index
		//reply.Err = OK
		//tryCnt := 5
		//for tryCnt > 0 {
		//kv.logmu.Lock()
		//if kv.commitIndex != index {
		//DPrintf("[KVServer.Get] current commitIndex=%v, need to be %v", kv.commitIndex, index)
		//kv.logmu.Unlock()
		//tryCnt--
		//time.Sleep(100 * time.Millisecond)
		//} else {
		//reply.Value = kv.storage[args.Key]
		//DPrintf("[KVServer.Get] %v success return value=%v", kv.me, reply.Value)
		//kv.logmu.Unlock()
		//return
		//}
		//}
	}
	return

}
