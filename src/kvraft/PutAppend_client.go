package kvraft

import "time"

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("[Ck.PutAppend] start putAppend op=%v, key=%v, value=%v ", op, key, value)

	args := &PutAppendArgs{
		ClientNumber:    ck.me,
		Key:             key,
		Value:           value,
		SerializeNumber: nrand(),
	}

	if op == "Put" {
		args.Op = "Put"
	} else {
		args.Op = "Append"
	}

	reply := &PutAppendReply{}

	// start from recorded leaderIndex, try every server
	for i := ck.leaderIndex; ; i = (i + 1) % len(ck.servers) {
		if ok := ck.servers[i].Call("KVServer.PutAppend", args, reply); !ok {
			DPrintf("[Ck.PutAppend] rpc to %v error", i)
		}
		if reply.Err == OK {
			ck.leaderIndex = i
			DPrintf("[ck.putAppend] get reply for key=%v, reply=%+v, set leaderIndex=%v", key, reply, i)
			break
		} else {
			time.Sleep(clientTryInterval)
		}
	}
	DPrintf("[Ck.putAppend] success of key=%v, value=%v", key, value)
	return
}
