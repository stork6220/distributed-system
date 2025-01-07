package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // 使用 int32 是为了可以使用 atomic 操作

	configs []Config // indexed by config num

	// Your data here.
	waitChs     map[int]chan Config // 用于等待操作完成的通道映射
	lastApplied map[int64]int64     // 去重相关字段 clientId -> last requestId
}

type Op struct {
	// Your data here.
	Type string // "Join", "Leave", "Move", "Query"

	ClientId  int64
	RequestId int64

	// 操作特定字段
	Servers map[int][]string // Join
	Gids    []int            // Leave
	Shard   int              // Move
	GID     int              // Move
	Num     int              // Query
}

// helper函数处理所有RPC请求的通用逻辑
func (sc *ShardCtrler) startOp(op Op) (Config, Err, bool) {
	// 1. 尝试通过Raft开始操作
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return Config{}, "", true
	}

	// 2. 创建等待通道
	sc.mu.Lock()
	waitCh := make(chan Config)
	sc.waitChs[index] = waitCh
	sc.mu.Unlock()

	// 3. 等待操作完成或超时
	select {
	case config := <-waitCh:
		sc.cleanupWaitCh(index)
		return config, OK, false
	case <-time.After(time.Second):
		sc.cleanupWaitCh(index)
		return Config{}, "", true
	}
}

// 通道清理
func (sc *ShardCtrler) cleanupWaitCh(index int) {
	sc.mu.Lock()
	delete(sc.waitChs, index)
	sc.mu.Unlock()
}

// 修改后的RPC处理函数
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		Type:      "Query",
		Num:       args.Num,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	reply.Config, reply.Err, reply.WrongLeader = sc.startOp(op)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		Type:      "Join",
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	_, reply.Err, reply.WrongLeader = sc.startOp(op)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Type:      "Leave",
		Gids:      args.GIDs,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	_, reply.Err, reply.WrongLeader = sc.startOp(op)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		Type:      "Move",
		Shard:     args.Shard,
		GID:       args.GID,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	_, reply.Err, reply.WrongLeader = sc.startOp(op)
}

// 判断服务器是否已经被关闭
func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	// 1. 首先关闭 Raft 层
	sc.rf.Kill()

	// Your code here, if desired.
	sc.mu.Lock()
	// 2. 标记服务已关闭
	atomic.StoreInt32(&sc.dead, 1)

	// 3. 清理所有等待的请求
	for index, ch := range sc.waitChs {
		close(ch) // 通知所有等待的客户端
		delete(sc.waitChs, index)
	}
	sc.mu.Unlock()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) rebalanceShards(shards [NShards]int, groups map[int][]string) [NShards]int {
	// 如果没有组，返回全0数组
	if len(groups) == 0 {
		var newShards [NShards]int
		return newShards
	}

	// 获取所有有效的 GID 并排序
	var gids []int
	for gid := range groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// 初始化新的分片分配
	newShards := shards

	// 计算每个组应得的分片数
	shardsPerGroup := NShards / len(groups)
	extraShards := NShards % len(groups)

	// 首先，初始化计数
	counts := make(map[int]int)
	for gid := range groups {
		counts[gid] = 0
	}

	// 计算当前分配情况
	for i := 0; i < NShards; i++ {
		if _, exists := groups[newShards[i]]; exists {
			counts[newShards[i]]++
		} else {
			// 如果分片当前分配给了无效组，立即重新分配
			newShards[i] = gids[0]
			counts[gids[0]]++
		}
	}

	// 重新平衡分片
	for i := 0; i < NShards; i++ {
		// 当前分片所属组
		currentGid := newShards[i]

		// 计算当前组应有的分片数
		targetCount := shardsPerGroup
		if sort.SearchInts(gids, currentGid) < extraShards {
			targetCount++
		}

		// 如果当前组分片数过多，尝试移动到其他组
		if counts[currentGid] > targetCount {
			// 寻找分片数少于目标值的组
			for _, gid := range gids {
				targetForGid := shardsPerGroup
				if sort.SearchInts(gids, gid) < extraShards {
					targetForGid++
				}

				if counts[gid] < targetForGid {
					newShards[i] = gid
					counts[currentGid]--
					counts[gid]++
					break
				}
			}
		}
	}

	return newShards
}

// 检查请求是否是重复的
func (sc *ShardCtrler) isDuplicateRequest(op Op) bool {
	lastRequestId, exists := sc.lastApplied[op.ClientId]
	return exists && lastRequestId >= op.RequestId
}

// 通知等待该命令结果的客户端
func (sc *ShardCtrler) notifyWaitingClient(index int, config Config) {
	if ch, ok := sc.waitChs[index]; ok {
		ch <- config
		delete(sc.waitChs, index) // 清理通道映射
	}
}

func (sc *ShardCtrler) applyLoop() {
	for !sc.killed() { // 检查服务器是否已关闭
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				sc.mu.Lock()
				// 处理命令并更新配置
				config := sc.processCommand(msg.Command.(Op))
				// 通知等待的客户端
				sc.notifyWaitingClient(msg.CommandIndex, config)
				sc.mu.Unlock()
			}
		case <-time.After(100 * time.Millisecond):
			// 定期检查是否需要退出，避免永久阻塞在 channel 上
			continue
		}
	}
}

// 处理Query命令：返回指定编号或最新的配置
func (sc *ShardCtrler) handleQuery(op Op) Config {
	// 如果请求的配置编号超出范围或为负，返回最新配置
	if op.Num < 0 || op.Num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	// 否则返回指定编号的配置
	return sc.configs[op.Num]
}

// 处理Join命令：添加新的复制组并重新平衡分片
func (sc *ShardCtrler) handleJoin(op Op) Config {
	// 创建新的配置，配置号为当前配置数量
	newConfig := Config{
		Num:    len(sc.configs),
		Groups: make(map[int][]string),
	}

	// 获取最新的配置
	lastConfig := sc.configs[len(sc.configs)-1]

	// 复制现有组
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}

	// 添加新组
	for gid, servers := range op.Servers {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}

	// 重新平衡分片并设置到新配置
	newConfig.Shards = sc.rebalanceShards(lastConfig.Shards, newConfig.Groups)

	// 添加新配置到配置列表
	sc.configs = append(sc.configs, newConfig)
	return newConfig
}

// 处理Leave命令：移除指定的复制组并重新平衡分片
func (sc *ShardCtrler) handleLeave(op Op) Config {
	newConfig := Config{
		Num:    len(sc.configs),
		Groups: make(map[int][]string),
	}

	lastConfig := sc.configs[len(sc.configs)-1]

	// 复制现有组，但排除要移除的组
	for gid, servers := range lastConfig.Groups {
		shouldKeep := true
		for _, leaveGID := range op.Gids {
			if gid == leaveGID {
				shouldKeep = false
				break
			}
		}
		if shouldKeep {
			newConfig.Groups[gid] = append([]string{}, servers...)
		}
	}

	// 重新平衡分片
	newConfig.Shards = sc.rebalanceShards(lastConfig.Shards, newConfig.Groups)

	sc.configs = append(sc.configs, newConfig)
	return newConfig
}

// 处理Move命令：将指定分片移动到指定组
func (sc *ShardCtrler) handleMove(op Op) Config {
	newConfig := Config{
		Num:    len(sc.configs),
		Groups: make(map[int][]string),
	}

	lastConfig := sc.configs[len(sc.configs)-1]

	// 复制现有组
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}

	// 复制现有分片分配
	newConfig.Shards = lastConfig.Shards
	// 移动指定分片到新组
	newConfig.Shards[op.Shard] = op.GID

	sc.configs = append(sc.configs, newConfig)
	return newConfig
}

// 将命令处理逻辑独立出来，提高可读性
func (sc *ShardCtrler) processCommand(op Op) Config {
	// 检查是否是重复的请求
	if sc.isDuplicateRequest(op) {
		return sc.configs[len(sc.configs)-1]
	}

	// 记录这个请求已经被处理
	sc.lastApplied[op.ClientId] = op.RequestId

	// 根据操作类型处理命令
	switch op.Type {
	case "Query":
		return sc.handleQuery(op)
	case "Join":
		return sc.handleJoin(op)
	case "Leave":
		return sc.handleLeave(op)
	case "Move":
		return sc.handleMove(op)
	}

	return Config{}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	// Explicitly initialize all shards to GID 0 in the initial config
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}

	labgob.Register(Op{})
	labgob.Register(Config{}) // Add this line
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.waitChs = make(map[int]chan Config)
	sc.lastApplied = make(map[int64]int64)
	go sc.applyLoop()

	return sc
}
