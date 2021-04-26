package kvraft

func (kv *KVServer) CommitIndex(args *CommitIndexArgs, reply *CommitIndexReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.logmu.Lock()
	defer kv.logmu.Unlock()

	reply.Index = kv.commitIndex
	if args.Key != "" {
		reply.Value = kv.storage[args.Key]
	}
	return
}
