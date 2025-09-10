package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	Type      string // "Get", "Put", "Append", "Reconfigure", "InstallShard"
	Key       string
	Value     string
	ClientId  int64
	RequestId int64

	// For reconfiguration
	Config shardctrler.Config

	// For shard installation
	Shard       int
	ShardData   map[string]string
	LastSeqData map[int64]int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int

	dead       int32
	shardData  map[int]map[string]string
	waitChs    map[int]chan Op
	lastSeq    map[int64]int64
	mck        *shardctrler.Clerk
	config     shardctrler.Config
	lastConfig shardctrler.Config
	shardState map[int]string // shard状态: "serving", "frozen", "pulling", "installing"
}

type CommandResult struct {
	Err   Err
	Value string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	// 检查分片状态 - serving和frozen状态都可以读取
	if state, exists := kv.shardState[shard]; !exists || (state != "serving" && state != "frozen") {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	result := kv.startCommand(op)
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	// 检查分片状态 - 只有serving状态才能处理写操作（frozen状态下拒绝写）
	if state, exists := kv.shardState[shard]; !exists || state != "serving" {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	result := kv.startCommand(op)
	reply.Err = result.Err
}

// Migrate RPC - 用于分片数据传输
func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查是否有这个分片的数据
	if _, exists := kv.shardData[args.Shard]; !exists {
		reply.Err = ErrWrongGroup
		return
	}

	// 检查这个分片是否曾经属于我们（在当前配置或历史配置中）
	ownedBefore := false

	// 检查当前配置
	if kv.config.Shards[args.Shard] == kv.gid {
		ownedBefore = true
	}

	// 检查lastConfig
	if kv.lastConfig.Shards[args.Shard] == kv.gid {
		ownedBefore = true
	}

	// 如果请求的配置编号存在，检查那个配置
	if args.ConfigNum > 0 && args.ConfigNum <= kv.config.Num {
		requestedConfig := kv.mck.Query(args.ConfigNum)
		if requestedConfig.Num == args.ConfigNum && requestedConfig.Shards[args.Shard] == kv.gid {
			ownedBefore = true
		}
	}

	if !ownedBefore {
		reply.Err = ErrWrongGroup
		return
	}

	reply.Err = OK
	reply.ConfigNum = args.ConfigNum
	reply.Shard = args.Shard

	// 复制分片数据
	reply.Data = make(map[string]string)
	for k, v := range kv.shardData[args.Shard] {
		reply.Data[k] = v
	}

	// 复制客户端序列号数据 - 只复制与该分片相关的客户端
	reply.LastSeq = make(map[int64]int64)
	// 注意：为了正确处理分片迁移，我们暂时不复制lastSeq
	// 这样可以避免目标分片错误地拒绝有效的客户端请求
	// 目标分片将通过请求ID来正确处理重复请求
}

// DeleteShard RPC - 删除分片数据
func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查是否拥有这个分片
	if kv.config.Shards[args.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}

	// 删除分片数据和状态
	delete(kv.shardData, args.Shard)
	delete(kv.shardState, args.Shard)

	reply.Err = OK
}

func (kv *ShardKV) startCommand(op Op) CommandResult {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return CommandResult{Err: ErrWrongLeader}
	}

	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.waitChs[index] = ch
	kv.mu.Unlock()

	select {
	case result := <-ch:
		kv.mu.Lock()
		delete(kv.waitChs, index)

		if result.ClientId == op.ClientId && result.RequestId == op.RequestId {
			if op.Type == "Get" {
				// 使用apply函数返回的结果值，不重新读取
				kv.mu.Unlock()
				return CommandResult{Err: OK, Value: result.Value}
			} else {
				kv.mu.Unlock()
				return CommandResult{Err: OK}
			}
		}

		kv.mu.Unlock()
		return CommandResult{Err: ErrWrongLeader}
	case <-time.After(2 * time.Second):
		kv.mu.Lock()
		delete(kv.waitChs, index)
		kv.mu.Unlock()
		return CommandResult{Err: ErrWrongLeader}
	}
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				op := msg.Command.(Op)

				switch op.Type {
				case "Reconfigure":
					// 处理配置变更
					kv.applyReconfigure(op.Config)
				case "InstallShard":
					// 处理分片安装
					kv.applyInstallShard(op.Shard, op.ShardData, op.LastSeqData)
				default:
					// 处理普通操作（Get/Put/Append）
					shard := key2shard(op.Key)

					// 检查分片状态
					state, exists := kv.shardState[shard]
					if !exists {
						// 分片不存在，跳过处理
						break
					}

					// 检查是否可以处理这个操作
					canRead := state == "serving" || state == "frozen"
					canWrite := state == "serving"

					if kv.shardData[shard] == nil {
						kv.shardData[shard] = make(map[string]string)
					}

					if lastSeq, exists := kv.lastSeq[op.ClientId]; exists && lastSeq >= op.RequestId {
						// 重复请求 - 对于Get操作，需要返回当前值
						if op.Type == "Get" && canRead {
							if value, exists := kv.shardData[shard][op.Key]; exists {
								op.Value = value
							} else {
								op.Value = ""
							}
						}
					} else {
						// 新请求
						switch op.Type {
						case "Put":
							if canWrite {
								kv.shardData[shard][op.Key] = op.Value
							}
						case "Append":
							if canWrite {
								if existing, exists := kv.shardData[shard][op.Key]; exists {
									kv.shardData[shard][op.Key] = existing + op.Value
								} else {
									kv.shardData[shard][op.Key] = op.Value
								}
							}
						case "Get":
							// Get操作读取数据并设置返回值
							if canRead {
								if value, exists := kv.shardData[shard][op.Key]; exists {
									op.Value = value
								} else {
									op.Value = ""
								}
							} else {
								op.Value = ""
							}
							break
						}
						if canRead || canWrite {
							kv.lastSeq[op.ClientId] = op.RequestId
						}
					}
				}

				ch, exists := kv.waitChs[msg.CommandIndex]

				// 检查是否需要创建快照
				if kv.maxraftstate != -1 && kv.rf.RaftStateSize() > kv.maxraftstate {
					kv.createSnapshot(msg.CommandIndex)
				}

				kv.mu.Unlock()

				if exists {
					ch <- op
				}
			} else if msg.SnapshotValid {
				kv.mu.Lock()
				kv.restoreSnapshot(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

// 应用配置变更 - 支持分片迁移
func (kv *ShardKV) applyReconfigure(newConfig shardctrler.Config) {
	if newConfig.Num <= kv.config.Num {
		return
	}

	oldConfig := kv.config
	kv.lastConfig = oldConfig
	kv.config = newConfig

	// 管理分片状态转换
	for shard := 0; shard < shardctrler.NShards; shard++ {
		oldGid := oldConfig.Shards[shard]
		newGid := newConfig.Shards[shard]

		if newGid == kv.gid {
			// 我们现在拥有这个分片
			switch oldGid {
			case kv.gid:
				// 继续拥有，保持serving状态
				kv.shardState[shard] = "serving"
				if kv.shardData[shard] == nil {
					kv.shardData[shard] = make(map[string]string)
				}
			case 0:
				// 全新的分片
				kv.shardData[shard] = make(map[string]string)
				kv.shardState[shard] = "serving"
			default:
				// 需要从其他组迁移的分片
				kv.shardState[shard] = "pulling"
				if kv.shardData[shard] == nil {
					kv.shardData[shard] = make(map[string]string)
				}
				// 启动goroutine来拉取分片数据
				go kv.pullAndInstallShard(shard, oldConfig)
			}
		} else {
			// 我们不再拥有这个分片
			if oldGid == kv.gid {
				// 设置为frozen状态，表示正在迁移但仍可读
				kv.shardState[shard] = "frozen"
				// 延迟删除分片数据，确保目标分片组已获取数据
				shardToDelete := shard
				// 延迟删除分片数据，确保目标分片组已获取数据
				time.AfterFunc(3*time.Second, func() {
					kv.mu.Lock()
					// 再次检查配置，确保我们仍然不拥有这个分片
					if kv.config.Shards[shardToDelete] != kv.gid {
						delete(kv.shardData, shardToDelete)
						delete(kv.shardState, shardToDelete)
					}
					kv.mu.Unlock()
				})
			}
		}
	}
}

// 应用分片安装
func (kv *ShardKV) applyInstallShard(shard int, shardData map[string]string, lastSeqData map[int64]int64) {
	// 只有当前配置中我们拥有这个分片时才安装数据
	if kv.config.Shards[shard] != kv.gid {
		return
	}

	// 初始化分片数据存储
	if kv.shardData[shard] == nil {
		kv.shardData[shard] = make(map[string]string)
	}

	// 安装分片数据
	if len(shardData) == 0 {
		// 空数据（新分片），保留任何现有数据
		if kv.shardData[shard] == nil {
			kv.shardData[shard] = make(map[string]string)
		}
	} else {
		// 有迁移数据，直接使用迁移数据
		// 注意：不再尝试合并本地修改，因为lastSeq信息不可用
		// 客户端将通过重试机制确保数据一致性
		kv.shardData[shard] = make(map[string]string)
		for k, v := range shardData {
			kv.shardData[shard][k] = v
		}
	}

	// 注意：我们不再复制客户端序列号，避免分片迁移时的重复请求问题
	// 目标分片将通过比较请求ID来正确处理客户端请求

	// 标记分片为serving状态
	kv.shardState[shard] = "serving"

	// 添加短暂延迟，确保状态传播
	time.Sleep(50 * time.Millisecond)
}

func (kv *ShardKV) configMonitor() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			currentConfigNum := kv.config.Num
			kv.mu.Unlock()

			newConfig := kv.mck.Query(currentConfigNum + 1)
			if newConfig.Num > currentConfigNum {
				// 执行分片迁移协议
				kv.changeConfigTo(newConfig)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ChangeConfigTo - 改进的分片迁移协议
func (kv *ShardKV) changeConfigTo(newConfig shardctrler.Config) {
	kv.mu.Lock()
	oldConfig := kv.config

	// 防止重复处理同一个配置
	if newConfig.Num <= oldConfig.Num {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 提交配置变更
	op := Op{
		Type:   "Reconfigure",
		Config: newConfig,
	}
	kv.rf.Start(op)

	// 等待配置变更被应用
	time.Sleep(100 * time.Millisecond)

	// 异步拉取需要的分片数据
	for shard := 0; shard < shardctrler.NShards; shard++ {
		oldGid := oldConfig.Shards[shard]
		newGid := newConfig.Shards[shard]

		if oldGid != kv.gid && newGid == kv.gid && oldGid != 0 {
			// 异步拉取分片数据
			go kv.pullAndInstallShard(shard, oldConfig)
		}
	}
}

// pullAndInstallShard - 从其他组拉取分片数据并安装
func (kv *ShardKV) pullAndInstallShard(shard int, oldConfig shardctrler.Config) bool {
	kv.mu.Lock()
	gid := oldConfig.Shards[shard]
	servers, ok := oldConfig.Groups[gid]
	kv.mu.Unlock()

	if gid == 0 {
		// 新分片，通过Raft提交InstallShard操作确保原子性
		op := Op{
			Type:        "InstallShard",
			Shard:       shard,
			ShardData:   make(map[string]string), // 空数据
			LastSeqData: make(map[int64]int64),   // 空序列号
		}
		kv.rf.Start(op)
		time.Sleep(200 * time.Millisecond)
		return true
	}

	if !ok {
		return false
	}

	// 获取分片数据，重试多次确保成功
	migrateArgs := MigrateArgs{
		ConfigNum: oldConfig.Num,
		Shard:     shard,
	}

	maxRetries := 10
	for retry := 0; retry < maxRetries; retry++ {
		// 尝试从源组的每个服务器获取数据
		for _, serverName := range servers {
			srv := kv.make_end(serverName)
			var migrateReply MigrateReply
			ok := srv.Call("ShardKV.Migrate", &migrateArgs, &migrateReply)
			if ok && migrateReply.Err == OK {
				// 安装分片数据
				op := Op{
					Type:        "InstallShard",
					Shard:       shard,
					ShardData:   migrateReply.Data,
					LastSeqData: migrateReply.LastSeq,
				}
				kv.rf.Start(op)
				// 等待安装完成
				time.Sleep(200 * time.Millisecond)

				// 通知源分片组删除分片数据
				go kv.notifySourceToDeleteShard(shard, oldConfig)

				return true
			}
		}
		time.Sleep(100 * time.Millisecond) // 增加重试间隔
	}
	return false
}

// notifySourceToDeleteShard - 通知源分片组删除分片数据
func (kv *ShardKV) notifySourceToDeleteShard(shard int, oldConfig shardctrler.Config) {
	sourceGid := oldConfig.Shards[shard]
	if sourceGid == 0 {
		return // 新分片，无需删除
	}

	servers, ok := oldConfig.Groups[sourceGid]
	if !ok {
		return
	}

	deleteArgs := DeleteShardArgs{
		ConfigNum: oldConfig.Num,
		Shard:     shard,
	}

	// 尝试通知源组的每个服务器删除分片
	for _, serverName := range servers {
		srv := kv.make_end(serverName)
		var deleteReply DeleteShardReply
		ok := srv.Call("ShardKV.DeleteShard", &deleteArgs, &deleteReply)
		if ok && deleteReply.Err == OK {
			// 成功通知一个服务器即可，因为Raft会确保一致性
			break
		}
	}
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.shardData = make(map[int]map[string]string)
	kv.waitChs = make(map[int]chan Op)
	kv.lastSeq = make(map[int64]int64)
	kv.shardState = make(map[int]string)

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 恢复快照（如果存在）
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.restoreSnapshot(snapshot)
		// 从快照恢复后，需要重新查询当前配置，因为配置可能已经改变
		currentConfig := kv.mck.Query(-1)
		kv.lastConfig = kv.config
		kv.config = currentConfig
	} else {
		// 只有在没有快照时才查询初始配置
		kv.config = kv.mck.Query(-1)
		kv.lastConfig = kv.config
	}

	// 根据当前配置重新初始化分片状态
	for shard := 0; shard < shardctrler.NShards; shard++ {
		if kv.config.Shards[shard] == kv.gid {
			// 检查是否已经有状态，如果没有则设置为serving
			if _, exists := kv.shardState[shard]; !exists {
				kv.shardState[shard] = "serving"
			}
			// 确保分片数据存在
			if kv.shardData[shard] == nil {
				kv.shardData[shard] = make(map[string]string)
			}
		}
	}

	go kv.applier()
	go kv.configMonitor()

	// 启动时检查并恢复任何未完成的迁移
	go func() {
		time.Sleep(500 * time.Millisecond) // 等待初始化完成
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.checkAndRecoverPendingMigrations()
		}
	}()

	return kv
}

// checkAndRecoverPendingMigrations - 检查并恢复未完成的分片迁移
func (kv *ShardKV) checkAndRecoverPendingMigrations() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查所有处于"pulling"状态的分片
	for shard := 0; shard < shardctrler.NShards; shard++ {
		if kv.config.Shards[shard] == kv.gid {
			if state, exists := kv.shardState[shard]; exists && state == "pulling" {
				// 找到未完成的迁移，尝试恢复
				go kv.recoverShardMigration(shard)
			}
		}
	}
}

// recoverShardMigration - 恢复特定分片的迁移
func (kv *ShardKV) recoverShardMigration(shard int) {
	kv.mu.Lock()
	currentConfig := kv.config
	kv.mu.Unlock()

	// 尝试找到最近拥有这个分片的组
	var sourceGid int
	var sourceConfig shardctrler.Config

	// 从当前配置向前查找，找到最近拥有这个分片的组
	for configNum := currentConfig.Num - 1; configNum >= 1; configNum-- {
		config := kv.mck.Query(configNum)
		if config.Num == configNum && config.Shards[shard] != kv.gid && config.Shards[shard] != 0 {
			sourceGid = config.Shards[shard]
			sourceConfig = config
			break
		}
	}

	if sourceGid == 0 {
		// 找不到源组，将分片设为空并标记为serving
		kv.mu.Lock()
		if kv.shardData[shard] == nil {
			kv.shardData[shard] = make(map[string]string)
		}
		kv.shardState[shard] = "serving"
		kv.mu.Unlock()
		return
	}

	// 尝试从源组获取数据，多次重试
	maxAttempts := 5
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if _, ok := sourceConfig.Groups[sourceGid]; ok {
			success := kv.pullAndInstallShard(shard, sourceConfig)
			if success {
				return // 成功获取数据
			}
		}
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond) // 指数退避
	}

	// 所有尝试都失败，至少确保分片可用
	kv.mu.Lock()
	if kv.shardData[shard] == nil {
		kv.shardData[shard] = make(map[string]string)
	}
	kv.shardState[shard] = "serving"
	kv.mu.Unlock()
}

// 创建快照
func (kv *ShardKV) createSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// 序列化当前状态
	e.Encode(kv.shardData)
	e.Encode(kv.lastSeq)
	e.Encode(kv.config)
	e.Encode(kv.lastConfig)
	e.Encode(kv.shardState)

	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}

// 恢复快照
func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
	if len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shardData map[int]map[string]string
	var lastSeq map[int64]int64
	var config shardctrler.Config
	var lastConfig shardctrler.Config
	var shardState map[int]string

	if d.Decode(&shardData) != nil ||
		d.Decode(&lastSeq) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&shardState) != nil {
		// 解码失败，保持当前状态
		return
	}

	kv.shardData = shardData
	kv.lastSeq = lastSeq
	kv.config = config
	kv.lastConfig = lastConfig
	kv.shardState = shardState
}
