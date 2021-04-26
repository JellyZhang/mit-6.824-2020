package kvraft

import "time"

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	//ck.mu.Lock()
	//defer ck.mu.Unlock()

	DPrintf("[Ck.Get] %v start get, key=%v", ck.me, key)

	args := &GetArgs{
		Key:             key,
		SerializeNumber: nrand(),
	}
	reply := &GetReply{}
	index := -1
	if ok := ck.servers[ck.leaderIndex].Call("KVServer.Get", args, reply); ok && reply.Err == OK {
		index = reply.Index
		DPrintf("[Ck.Get] %v success of key=%v, ans=%v,index=%v", ck.me, key, reply.Value, index)
		return reply.Value
	}
	DPrintf("[Ck.get] %v rpc to leader error or wrongLeader", ck.me)
	for true {
		for i := 0; i < len(ck.servers); i++ {
			if ok := ck.servers[i].Call("KVServer.Get", args, reply); !ok {
				DPrintf("[Ck.get] %v rpc to %v error", ck.me, i)
			}
			if reply.Err == OK {
				index = reply.Index
				ck.leaderIndex = i
				DPrintf("[ck.get] %v get reply for key=%v, reply=%+v, set leaderIndex=%v", ck.me, key, reply, i)
				break
			}
		}
		if index != -1 {
			break
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
	DPrintf("[Ck.Get] %v success of key=%v, ans=%v,index=%v", ck.me, key, reply.Value, index)
	return reply.Value
}
