package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	// 操作类型: "Get", "Put", 或 "Append"
	OpType string

	// 操作的键值对
	Key   string
	Value string

	// 用于请求去重
	ClientId  int64 // 客户端的唯一标识
	RequestId int64 // 该客户端的请求序号

	// 可选：用于调试
	Term int // 操作被提交时的任期号
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	kvStore      map[string]string
	lastIndex    int // 追踪最后应用的命令索引

	// 客户端请求追踪
	lastApplied map[int64]int64 // Track last applied sequence per client
	notifyCh    map[int]chan Op // Notification channels for completed operations
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if lastSeq, ok := kv.lastApplied[args.ClientId]; ok && args.Seq <= lastSeq {
		// Return the cached result
		value, exists := kv.kvStore[args.Key]
		kv.mu.Unlock()
		if !exists {
			reply.Err = ErrNoKey
		} else {
			reply.Value = value
			reply.Err = OK
		}
		return
	}
	kv.mu.Unlock()

	// Start consensus for new request
	op := Op{
		OpType:    "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.Seq,
	}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Create notification channel and wait for result
	ch := make(chan Op, 1)
	kv.mu.Lock()
	kv.notifyCh[index] = ch
	kv.mu.Unlock()

	select {
	case result := <-ch:
		if result.ClientId != args.ClientId || result.RequestId != args.Seq {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		value, exists := kv.kvStore[args.Key]
		kv.mu.Unlock()

		if !exists {
			reply.Err = ErrNoKey
		} else {
			reply.Value = value
			reply.Err = OK
		}
	case <-time.After(time.Second):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.notifyCh, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if lastSeq, ok := kv.lastApplied[args.ClientId]; ok && args.Seq <= lastSeq {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.Seq,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan Op, 1)
	kv.mu.Lock()
	kv.notifyCh[index] = ch
	kv.mu.Unlock()

	timer := time.NewTimer(12 * time.Millisecond)
	defer timer.Stop()

	select {
	case result := <-ch:
		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader || term != currentTerm {
			reply.Err = ErrWrongLeader
			return
		}
		if result.ClientId != args.ClientId || result.RequestId != args.Seq {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
	case <-timer.C: // 使用 timer 而不是 time.After
		reply.Err = ErrWrongLeader
	}
}

// decodeSnapshot 从快照数据中恢复服务器状态
// 返回 bool 表示解码是否成功
// decodeSnapshot 函数也可以简化
func (kv *KVServer) decodeSnapshot(snapshot []byte) bool {
	if len(snapshot) == 0 {
		return false
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvStore map[string]string
	if err := d.Decode(&kvStore); err != nil {
		fmt.Printf("Error decoding kvStore from snapshot: %v\n", err)
		return false
	}

	var lastApplied map[int64]int64
	if err := d.Decode(&lastApplied); err != nil {
		fmt.Printf("Error decoding lastApplied from snapshot: %v\n", err)
		return false
	}

	kv.kvStore = kvStore
	kv.lastApplied = lastApplied
	return true
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.kvStore = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)
	kv.lastIndex = 0
	kv.notifyCh = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg)

	// 从 persister 读取快照
	kv.decodeSnapshot(persister.ReadSnapshot())

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyLoop()
	return kv
}

// Kill marks this server as dead and initiates shutdown.
// This is used by the tester to cleanly stop the server.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill() // Terminate the underlying Raft instance
}

// killed returns true if the server has been killed.
// This is used to check whether the server should continue processing.
func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.SnapshotValid {
			// This is a snapshot
			kv.mu.Lock()
			// 只有当快照的索引大于当前已应用的索引时才应用快照
			if msg.SnapshotIndex > kv.lastIndex {
				if kv.decodeSnapshot(msg.Snapshot) {
					kv.lastIndex = msg.SnapshotIndex
				}
			}
			kv.mu.Unlock()
		} else if msg.CommandValid {
			op := msg.Command.(Op)
			kv.mu.Lock()

			if msg.CommandIndex > kv.lastIndex || msg.SnapshotValid {
				// Check if this is a new operation from this client
				if lastSeq, exist := kv.lastApplied[op.ClientId]; !exist || op.RequestId > lastSeq {
					// Apply the operation
					switch op.OpType {
					case "Put":
						kv.kvStore[op.Key] = op.Value
					case "Append":
						kv.kvStore[op.Key] += op.Value
					}
					kv.lastApplied[op.ClientId] = op.RequestId

					// 在应用命令后更新lastIndex
					kv.lastIndex = msg.CommandIndex

					// Check if we need to create a snapshot
					if kv.maxraftstate > 0 && kv.rf.RaftStateSize() > kv.maxraftstate {
						w := new(bytes.Buffer)
						e := labgob.NewEncoder(w)
						e.Encode(kv.kvStore)
						e.Encode(kv.lastApplied)
						data := w.Bytes()
						kv.rf.Snapshot(msg.CommandIndex, data)
					}
				}
			}

			kv.mu.Unlock()

			// Notify waiting RPC if any
			kv.mu.Lock()
			ch, ok := kv.notifyCh[msg.CommandIndex]
			delete(kv.notifyCh, msg.CommandIndex) // 在发送 *之前* 删除
			kv.mu.Unlock()

			if ok {
				ch <- op // Notify the waiting RPC
			}
		}
	}
}
