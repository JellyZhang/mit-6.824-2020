package shardctrler

import "time"

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	sc.mu.Lock()
	defer sc.mu.Unlock()

	DPrintf("[Leave] args=%+v", args)
	cmd := Op{
		ClientNum: args.ClientNumber,
		OpIndex:   args.SerializeNumber,
		Command:   LEAVE,
		GIDs:      args.GIDs,
	}

	sc.mapmu.Lock()
	havedone, ok := sc.haveDone[args.ClientNumber]
	sc.mapmu.Unlock()
	if ok && havedone == args.SerializeNumber {
		reply.Err = OK
		return
	}

	for true {
		_, _, isLeader := sc.rf.Start(cmd)
		sc.isLeader.Store(isLeader)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		for start := time.Now(); time.Since(start) < serverTimeoutInterval; {
			select {
			case <-time.After(serverTimeoutInterval):
				reply.WrongLeader = true
				return
			case notify := <-sc.notifyCh:
				if notify.OpIndex == args.SerializeNumber {
					reply.Err = OK
					return
				}
			}
		}
		sc.isLeader.Store(false)
		reply.WrongLeader = true
		return
	}
	return
}
