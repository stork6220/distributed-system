package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int   // index of the leader server
	id     int64 // unique identifier for each clerk instance
	seq    int64 // sequence number to ensure idempotency
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.id = nrand()
	ck.seq = 0
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := &GetArgs{
		Key:      key,
		ClientId: ck.id,
		Seq:      ck.seq,
	}

	// Increment sequence number once per unique request
	ck.seq++

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.leader + i) % len(ck.servers)
			reply := &GetReply{}
			ok := ck.servers[server].Call("KVServer.Get", args, reply)
			if ok && reply.Err == OK {
				ck.leader = server // Rembember the leader
				return reply.Value
			}
			if ok && reply.Err == ErrNoKey {
				return ""
			}

			time.Sleep(10 * time.Millisecond)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.id,
		Seq:      ck.seq,
	}

	// Increment sequence number once per unique request
	ck.seq++

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.leader + i) % len(ck.servers)
			reply := &PutAppendReply{}
			ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
			if ok && reply.Err == OK {
				ck.leader = server // Rembember the leader
				return
			}
			if ok && reply.Err == ErrNoKey {
				return
			}

			time.Sleep(1 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
