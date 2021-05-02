package shardctrler

import "time"

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	sc.mu.Lock()
	defer sc.mu.Unlock()

	cmd := Op{
		ClientNum: args.ClientNumber,
		OpIndex:   args.SerializeNumber,
		Command:   MOVE,
		Shard:     args.Shard,
		GID:       args.GID,
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
