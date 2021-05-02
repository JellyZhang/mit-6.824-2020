package shardctrler

import (
	"time"
)

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	sc.mu.Lock()
	defer sc.mu.Unlock()
	cmd := Op{
		ClientNum: args.ClientNumber,
		OpIndex:   args.SerializeNumber,
		Command:   QUERY,
		Num:       args.Num,
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
					sc.mapmu.Lock()
					if args.Num == -1 {
						reply.Config = sc.configs[len(sc.configs)-1]
					} else {
						reply.Config = sc.configs[args.Num]
					}
					sc.mapmu.Unlock()
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
