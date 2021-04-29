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
	DPrintf("[Ck.Get] start get, key=%v", key)

	args := &GetArgs{
		ClientNumber:    ck.me,
		Key:             key,
		SerializeNumber: nrand(),
	}
	reply := &GetReply{}

	// start from recorded leaderIndex, try every server
	for i := ck.leaderIndex; ; i = (i + 1) % len(ck.servers) {
		if ok := ck.servers[i].Call("KVServer.Get", args, reply); !ok {
			DPrintf("[Ck.get] rpc to %v error", i)
		}
		if reply.Err == OK {
			ck.leaderIndex = i
			DPrintf("[ck.get] get reply for key=%v, reply=%+v, set leaderIndex=%v", key, reply, i)
			break
		} else {
			time.Sleep(clientTryInterval)
		}
	}
	DPrintf("[Ck.Get] success of key=%v, ans=%v", key, reply.Value)
	return reply.Value
}
