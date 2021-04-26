package kvraft

//func (ck *Clerk) getCommitIndex(server int) int {
//args := &CommitIndexArgs{}
//reply := &CommitIndexReply{}

//if ok := ck.servers[server].Call("KVServer.CommitIndex", args, reply); !ok {
//DPrintf("[Ck.getCommitIndex] rpc error")
//return 0
//}

//DPrintf("[ck.getCommitIndex] get %v's commitIndex=%v", server, reply.Index)
//return reply.Index
//}
