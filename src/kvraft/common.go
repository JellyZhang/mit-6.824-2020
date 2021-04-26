package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SerializeNumber int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key             string
	SerializeNumber int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Index int
	Value string
}

type CommitIndexArgs struct {
	Key string
}

type CommitIndexReply struct {
	Err   Err
	Value string
	Index int
}
