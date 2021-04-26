package kvraft

import (
	"encoding/json"
	"time"
)

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	cmd := Op{
		OpIndex: args.SerializeNumber,
	}
	if args.Op == "Put" {
		cmd.Command = PUT
	} else {
		cmd.Command = APPEND
	}

	cmd.Key = args.Key
	cmd.Value = args.Value
	DPrintf("[KVServer.PutAppend] %v get args=%+v", kv.me, args)
	kv.logmu.Lock()
	_, ok := kv.havedone[args.SerializeNumber]
	kv.logmu.Unlock()
	if ok {
		DPrintf("[KVServer.PutAppend] %v find already done, key=%v, value=%v", kv.me, args.Key, args.Value)
		reply.Err = OK
		return
	}
	for true {
		index, _, isLeader := kv.rf.Start(cmd)
		kv.logmu.Lock()
		kv.isLeader = isLeader
		kv.logmu.Unlock()
		if !isLeader {
			DPrintf("[KVServer.PutAppend] %v is not leader", kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		DPrintf("[KVServer.PutAppend] %v success start, index=%v, key=%v, value=%v ", kv.me, index, args.Key, args.Value)
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
				//DPrintf("%v smaller panic, notify=%+v, index=%v", kv.me, notify, index)
				//continue
				//} else if notify.index == index && notify.key == cmd.Key && notify.value == cmd.Value && notify.command == cmd.Command {
				if notify.OpIndex == args.SerializeNumber {
					reply.Err = OK
					kv.logmu.Lock()
					kv.havedone[args.SerializeNumber] = struct{}{}
					kv.logmu.Unlock()
					if kv.maxraftstate != -1 && kv.commitIndex%10 == 0 {
						jsonString, err := json.Marshal(kv.storage)
						if err != nil {
							panic(err)
						}
						kv.rf.Snapshot(kv.commitIndex, []byte(jsonString))
					}
					DPrintf("[KVServer.PutAppend] %v success receive key=%v, value=%v", kv.me, args.Key, args.Value)
					return
				} else {
					continue
					//kv.logmu.Lock()
					//DPrintf("panic, notify=%+v, index=%v, set isLeader to false", notify, index)
					//kv.isLeader = false
					//kv.logmu.Unlock()
					//reply.Err = ErrWrongLeader
					//return
				}
			}
		}
		DPrintf("[KVServer.PutAppend] %v time out", kv.me)
		kv.logmu.Lock()
		kv.isLeader = false
		kv.logmu.Unlock()
		reply.Err = ErrWrongLeader
		return
		//tryCnt := 10
		//for tryCnt > 0 {
		//kv.logmu.Lock()
		//if kv.commitIndex != index {
		//DPrintf("[KVServer.PutAppend] current commitIndex=%v, need to be %v", kv.commitIndex, index)
		//kv.logmu.Unlock()
		//tryCnt--
		//time.Sleep(30 * time.Millisecond)
		//} else {
		//reply.Index = index
		//reply.Err = OK
		//DPrintf("[KVServer.PutAppend] %v success key=%v, value=%v", kv.me, args.Key, args.Value)
		//kv.logmu.Unlock()
		//return
		//}
		//}

	}

	return
}
