package kvraft

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
	DPrintf("[Ck.PutAppend] %v start putAppend op=%v, key=%v, value=%v ", ck.me, op, key, value)

	ck.index++

	args := &PutAppendArgs{
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
	if ok := ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", args, reply); ok && reply.Err == OK {
		DPrintf("[Ck.PutAppend] %v success of key=%v, value=%v", ck.me, key, value)
		return
	}

	DPrintf("[Ck.PutAppend] %v rpc to leader error or wrongLeader", ck.me)
	for true {
		for i := 0; i < len(ck.servers); i++ {
			if ok := ck.servers[i].Call("KVServer.PutAppend", args, reply); !ok {
				DPrintf("[Ck.PutAppend] %v rpc to %v error", ck.me, i)
			}
			if reply.Err == OK {
				ck.leaderIndex = i
				DPrintf("[ck.putAppend] %v get reply for key=%v, reply=%+v, set leaderIndex=%v", ck.me, key, reply, i)
				return
			}
		}
	}
	DPrintf("[Ck.putAppend] %v success of key=%v, value=%v", ck.me, key, value)
	return

	//for true {
	//args1 := &CommitIndexArgs{}
	//reply1 := &CommitIndexReply{}
	//cnt := 0
	//for i := 0; i < len(ck.servers); i++ {
	//if ok := ck.servers[i].Call("KVServer.CommitIndex", args1, reply1); !ok {
	//DPrintf("[Ck.putAppend] rpc error in commitIndex")
	//continue
	//}
	////DPrintf("[Ck.PutAppend] get commitIndex from %v success, reply=%v", i, reply1)
	//if reply1.Index == index {
	//cnt++
	//}
	//}
	//if cnt >= (len(ck.servers)+1)/2 {
	//ck.index = index
	//DPrintf("[Ck.PutAppend] success end op=%v, key=%v, value=%v, ck.index=%v", op, key, value, ck.index)
	//return
	//}
	//}
}

//func (ck *Clerk) doPutAppend(commandIndex int, server int, key string, value string, op string) {

//args := &PutAppendArgs{
//Key:   key,
//Value: value,
//}

//if op == "Put" {
//args.Op = "Put"
//} else {
//args.Op = "Append"
//}

////reply := &PutAppendReply{}

//}
