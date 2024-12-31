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
	ClientId int64 // client id, used for deduplication in raft servers.
	Seq      int64 // sequence number of the request from client, used for deduplication in raft servers.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64 // client id, used for deduplication in raft servers.
	Seq      int64 // sequence number of the request from client, used for deduplication in raft servers.
}

type GetReply struct {
	Err   Err
	Value string
}
