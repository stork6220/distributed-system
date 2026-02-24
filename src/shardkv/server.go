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

const (
	opGet         = "Get"
	opPut         = "Put"
	opAppend      = "Append"
	opReconfigure = "Reconfigure"
	opInstall     = "InstallShard"
	opShardGC     = "ShardGC"
	opFreezeShard = "FreezeShard"

	shardServing = "serving"
	shardFrozen  = "frozen"
	shardPulling = "pulling"
)

type Op struct {
	Type      string // opGet/opPut/opAppend/opReconfigure/opInstall/opShardGC/opFreezeShard
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
	Err       Err // 用于在 applier 中记录操作结果

	// For reconfiguration
	Config shardctrler.Config

	// For shard installation and GC
	ConfigNum       int
	SourceConfigNum int
	Shard           int
	ShardData       map[string]string
	LastSeqData     map[int64]int64
	IsGCSource      bool // true: 源组删除数据; false: 目标组清理 shardsToGC
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

	dead                 int32
	shardData            map[int]map[string]string
	waitChs              map[int]chan Op
	lastSeq              map[int64]int64
	mck                  *shardctrler.Clerk
	config               shardctrler.Config
	lastConfig           shardctrler.Config
	shardState           map[int]string // shard状态: shardServing/shardFrozen/shardPulling
	frozenShardConfig    map[int]int    // frozen分片对应的源配置号: shard -> sourceConfigNum
	pullingShards        map[int]bool   // 正在拉取的分片，防止重复拉取
	shardsToGC           map[int]int    // 需要通知源组删除的分片: shard -> sourceConfigNum
	lastConfigNumStarted int            // 最近提交到 Raft 的配置版本号
}

type CommandResult struct {
	Err   Err
	Value string
}

// ===== Client / migration RPC handlers =====

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	shard := key2shard(args.Key)
	DPrintf("gid=%d me=%d Get key=%s shard=%d configNum=%d shardOwner=%d state=%s",
		kv.gid, kv.me, args.Key, shard, kv.config.Num, kv.config.Shards[shard], kv.shardState[shard])
	if err := kv.ensureServingShardLocked(shard); err != OK {
		DPrintf("gid=%d me=%d Get: ErrWrongGroup state=%s", kv.gid, kv.me, kv.shardState[shard])
		reply.Err = err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:      opGet,
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
	if err := kv.ensureServingShardLocked(shard); err != OK {
		reply.Err = err
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

// Migrate exports shard data to the destination group.
func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	needFreeze := false
	kv.mu.Lock()
	if kv.config.Num == args.ConfigNum &&
		kv.config.Shards[args.Shard] == kv.gid &&
		kv.shardState[args.Shard] == shardServing {
		// We are still in the source config for this shard.
		// Freeze first to prevent concurrent writes during export.
		needFreeze = true
	}
	kv.mu.Unlock()

	if needFreeze {
		freezeOp := Op{
			Type:      opFreezeShard,
			ConfigNum: args.ConfigNum,
			Shard:     args.Shard,
		}
		result := kv.startCommand(freezeOp)
		if result.Err != OK {
			reply.Err = ErrWrongLeader
			return
		}
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("gid=%d me=%d Migrate: args.ConfigNum=%d args.Shard=%d kv.config.Num=%d state=%s hasData=%v",
		kv.gid, kv.me, args.ConfigNum, args.Shard, kv.config.Num, kv.shardState[args.Shard], kv.shardData[args.Shard] != nil)

	if kv.config.Num < args.ConfigNum {
		DPrintf("gid=%d me=%d Migrate: reject (config too old)", kv.gid, kv.me)
		reply.Err = ErrWrongGroup
		return
	}
	if kv.config.Num == args.ConfigNum {
		// In the same config, export is allowed only after freezing.
		if kv.shardState[args.Shard] != shardFrozen {
			DPrintf("gid=%d me=%d Migrate: reject (same config but not frozen)", kv.gid, kv.me)
			reply.Err = ErrWrongGroup
			return
		}
	} else {
		// In newer configs, reject if this shard has been re-owned and is serving.
		if kv.config.Shards[args.Shard] == kv.gid && kv.shardState[args.Shard] == shardServing {
			DPrintf("gid=%d me=%d Migrate: reject (re-owned and serving in newer config)", kv.gid, kv.me)
			reply.Err = ErrWrongGroup
			return
		}
	}
	if _, exists := kv.shardData[args.Shard]; !exists {
		DPrintf("gid=%d me=%d Migrate: reject (no data)", kv.gid, kv.me)
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

	// 复制客户端序列号数据
	reply.LastSeq = make(map[int64]int64)
	for clientId, seq := range kv.lastSeq {
		reply.LastSeq[clientId] = seq
	}
}

// DeleteShard asks source group to GC old shard data.
func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.config.Num <= args.ConfigNum {
		// Not advanced enough yet, or still owning this shard in current view.
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:       opShardGC,
		ConfigNum:  args.ConfigNum,
		Shard:      args.Shard,
		IsGCSource: true, // 我们是源组，需要删除数据
	}

	result := kv.startCommand(op)
	reply.Err = result.Err
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

		if isSameOp(op, result) {
			// 使用 applier 中设置的错误码
			kv.mu.Unlock()
			return CommandResult{Err: result.Err, Value: result.Value}
		}

		DPrintf("gid=%d me=%d startCommand: mismatched result index=%d type=%s vs %s clientId=%d reqId=%d vs clientId=%d reqId=%d",
			kv.gid, kv.me, index, result.Type, op.Type, result.ClientId, result.RequestId, op.ClientId, op.RequestId)

		kv.mu.Unlock()
		return CommandResult{Err: ErrWrongLeader}
	case <-time.After(3 * time.Second):
		kv.mu.Lock()
		delete(kv.waitChs, index)
		kv.mu.Unlock()
		return CommandResult{Err: ErrWrongLeader}
	}
}

// ===== Apply / state-machine helpers =====

func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			op := msg.Command.(Op)

			switch op.Type {
			case opReconfigure:
				kv.applyReconfigure(op.Config)
				op.Err = OK
			case opInstall:
				kv.applyInstallShard(op.ConfigNum, op.SourceConfigNum, op.Shard, op.ShardData, op.LastSeqData)
				op.Err = OK
			case opShardGC:
				kv.applyShardGC(op.ConfigNum, op.Shard, op.IsGCSource)
				op.Err = OK
			case opFreezeShard:
				kv.applyFreezeShard(op.ConfigNum, op.Shard)
				op.Err = OK
			default:
				kv.applyClientOp(&op)
			}

			ch, exists := kv.waitChs[msg.CommandIndex]
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

// ensureServingShardLocked checks owner+state for client traffic.
// Caller must hold kv.mu.
func (kv *ShardKV) ensureServingShardLocked(shard int) Err {
	if kv.config.Shards[shard] != kv.gid {
		return ErrWrongGroup
	}
	state, exists := kv.shardState[shard]
	if !exists || state != shardServing {
		return ErrWrongGroup
	}
	return OK
}

// applyClientOp applies Get/Put/Append.
// Caller must hold kv.mu.
func (kv *ShardKV) applyClientOp(op *Op) {
	shard := key2shard(op.Key)
	if kv.ensureServingShardLocked(shard) != OK {
		op.Err = ErrWrongGroup
		return
	}
	if kv.shardData[shard] == nil {
		kv.shardData[shard] = make(map[string]string)
	}

	if lastSeq, exists := kv.lastSeq[op.ClientId]; exists && lastSeq >= op.RequestId {
		op.Err = OK
		if op.Type == opGet {
			if value, exists := kv.shardData[shard][op.Key]; exists {
				op.Value = value
			} else {
				op.Value = ""
			}
		}
		return
	}

	switch op.Type {
	case opPut:
		kv.shardData[shard][op.Key] = op.Value
		op.Err = OK
		kv.lastSeq[op.ClientId] = op.RequestId
	case opAppend:
		if existing, exists := kv.shardData[shard][op.Key]; exists {
			kv.shardData[shard][op.Key] = existing + op.Value
		} else {
			kv.shardData[shard][op.Key] = op.Value
		}
		op.Err = OK
		kv.lastSeq[op.ClientId] = op.RequestId
	case opGet:
		if value, exists := kv.shardData[shard][op.Key]; exists {
			op.Value = value
		} else {
			op.Value = ""
		}
		op.Err = OK
		kv.lastSeq[op.ClientId] = op.RequestId
	default:
		op.Err = ErrWrongLeader
	}
}

// applyReconfigure applies one-step config transition.
func (kv *ShardKV) applyReconfigure(newConfig shardctrler.Config) {
	if newConfig.Num != kv.config.Num+1 {
		return
	}

	DPrintf("gid=%d me=%d applyReconfigure: old=%d new=%d", kv.gid, kv.me, kv.config.Num, newConfig.Num)

	oldConfig := kv.config
	kv.config = newConfig

	for shard := 0; shard < shardctrler.NShards; shard++ {
		if newConfig.Shards[shard] == kv.gid {
			if oldConfig.Shards[shard] == 0 {
				kv.shardState[shard] = shardServing
				delete(kv.frozenShardConfig, shard)
				if kv.shardData[shard] == nil {
					kv.shardData[shard] = make(map[string]string)
				}
			} else if oldConfig.Shards[shard] != kv.gid {
				// Pull only when shard comes from another group.
				if kv.shardState[shard] != shardServing {
					kv.shardState[shard] = shardPulling
					delete(kv.frozenShardConfig, shard)
					// Keep old data until new data is installed; this helps A->B->A loops.
				}
			}
		} else {
			if oldConfig.Shards[shard] == kv.gid {
				if kv.shardState[shard] == shardServing {
					kv.shardState[shard] = shardFrozen
					kv.frozenShardConfig[shard] = oldConfig.Num
					DPrintf("gid=%d me=%d shard %d becomes frozen", kv.gid, kv.me, shard)
				} else if kv.shardState[shard] == shardPulling {
					// If pulling shard is moved away again, clean transient state.
					kv.shardState[shard] = ""
					delete(kv.shardData, shard)
					delete(kv.frozenShardConfig, shard)
					delete(kv.pullingShards, shard)
				}
			} else {
				// Keep frozen shards until GC confirms safe deletion.
				if kv.shardState[shard] != shardFrozen {
					kv.shardState[shard] = ""
					delete(kv.shardData, shard)
					delete(kv.frozenShardConfig, shard)
				}
			}
		}
	}
	kv.lastConfig = oldConfig
}

// applyInstallShard installs migrated shard data.
func (kv *ShardKV) applyInstallShard(configNum int, sourceConfigNum int, shard int, shardData map[string]string, lastSeqData map[int64]int64) {
	DPrintf("gid=%d me=%d applyInstallShard: configNum=%d sourceConfigNum=%d shard=%d dataLen=%d state=%s",
		kv.gid, kv.me, configNum, sourceConfigNum, shard, len(shardData), kv.shardState[shard])

	// Allow install for current or older configs when shard is still pulling.
	if configNum > kv.config.Num {
		DPrintf("gid=%d me=%d applyInstallShard: skip (configNum too new: %d > %d)",
			kv.gid, kv.me, configNum, kv.config.Num)
		return
	}

	// Install only when local shard state is pulling.
	state := kv.shardState[shard]
	if state != shardPulling {
		DPrintf("gid=%d me=%d applyInstallShard: skip (state is %s, not pulling)", kv.gid, kv.me, state)
		return
	}

	// Replace old map to avoid stale keys.
	kv.shardData[shard] = make(map[string]string)
	for k, v := range shardData {
		kv.shardData[shard][k] = v
	}

	// 合并客户端序列号数据
	for clientId, seq := range lastSeqData {
		if currentSeq, exists := kv.lastSeq[clientId]; !exists || seq > currentSeq {
			kv.lastSeq[clientId] = seq
		}
	}
	DPrintf("gid=%d me=%d applyInstallShard: installed %d keys, setting to serving", kv.gid, kv.me, len(shardData))

	// Mark serving and clear transient markers.
	kv.shardState[shard] = shardServing
	delete(kv.frozenShardConfig, shard)
	// 清除拉取标记
	delete(kv.pullingShards, shard)

	// Schedule source GC after install.
	if sourceConfigNum > 0 {
		kv.shardsToGC[shard] = sourceConfigNum
	}
}

// gcMonitor periodically notifies source groups to delete transferred shards.
func (kv *ShardKV) gcMonitor() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			shardsToGC := make(map[int]int)
			for shard, configNum := range kv.shardsToGC {
				shardsToGC[shard] = configNum
			}
			kv.mu.Unlock()

			if len(shardsToGC) > 0 {
				// Collect unique config numbers to minimize Query calls
				uniqueConfigNums := make(map[int]bool)
				for _, configNum := range shardsToGC {
					uniqueConfigNums[configNum] = true
				}

				configCache := make(map[int]shardctrler.Config)
				for configNum := range uniqueConfigNums {
					configCache[configNum] = kv.mck.Query(configNum)
				}

				var wg sync.WaitGroup
				for shard, configNum := range shardsToGC {
					config := configCache[configNum]
					if config.Num == configNum {
						wg.Add(1)
						go func(s int, cNum int, conf shardctrler.Config) {
							defer wg.Done()
							kv.notifySourceToDeleteShard(s, cNum, conf)
						}(shard, configNum, config)
					}
				}
				wg.Wait()
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// applyShardGC applies source/target side GC entries.
func (kv *ShardKV) applyShardGC(configNum int, shard int, isSource bool) {
	DPrintf("gid=%d me=%d applyShardGC: configNum=%d shard=%d state=%s isSource=%v",
		kv.gid, kv.me, configNum, shard, kv.shardState[shard], isSource)

	// GC may target an older config; reject only if request is from the future.
	if configNum > kv.config.Num {
		return
	}

	if isSource {
		// Source: delete only when frozen marker matches the same migration round.
		if kv.shardState[shard] == shardFrozen {
			frozenConfig, ok := kv.frozenShardConfig[shard]
			if !ok || frozenConfig != configNum {
				return
			}
			kv.shardState[shard] = ""
			delete(kv.shardData, shard)
			delete(kv.frozenShardConfig, shard)
			DPrintf("gid=%d me=%d applyShardGC: deleted shard=%d", kv.gid, kv.me, shard)
		}
	} else {
		// Destination: clear local pending-GC marker.
		if cNum, ok := kv.shardsToGC[shard]; ok && cNum == configNum {
			delete(kv.shardsToGC, shard)
			DPrintf("gid=%d me=%d applyShardGC: cleaned shardsToGC for shard=%d", kv.gid, kv.me, shard)
		}
	}
}

func (kv *ShardKV) applyFreezeShard(configNum int, shard int) {
	if kv.config.Num != configNum {
		return
	}
	if kv.config.Shards[shard] != kv.gid {
		return
	}
	if kv.shardState[shard] == shardServing {
		kv.shardState[shard] = shardFrozen
		kv.frozenShardConfig[shard] = configNum
	}
}

// ===== Background reconfiguration / migration =====

func (kv *ShardKV) configMonitor() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			currentConfigNum := kv.config.Num
			currentConfig := kv.config

			// Start pulling tasks for local pulling shards.
			hasPulling := false
			for shard := 0; shard < shardctrler.NShards; shard++ {
				if kv.shardState[shard] == shardPulling && currentConfig.Shards[shard] == kv.gid {
					hasPulling = true
					if !kv.pullingShards[shard] {
						kv.pullingShards[shard] = true
						go kv.tryPullShard(shard, currentConfigNum)
					}
				}
			}
			kv.mu.Unlock()

			// Advance config only when all shard pulls for current config are done.
			if !hasPulling {
				nextConfigNum := currentConfigNum + 1
				if kv.lastConfigNumStarted < nextConfigNum {
					newConfig := kv.mck.Query(nextConfigNum)
					if newConfig.Num == nextConfigNum {
						kv.lastConfigNumStarted = nextConfigNum
						kv.changeConfigTo(newConfig)
					}
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// tryPullShard fetches shard data from lastConfig's owner.
func (kv *ShardKV) tryPullShard(shard int, currentConfigNum int) {
	DPrintf("gid=%d tryPullShard: shard=%d currentConfigNum=%d", kv.gid, shard, currentConfigNum)

	// Clear in-flight marker on failure so monitor can retry.
	success := false
	defer func() {
		if !success {
			kv.mu.Lock()
			delete(kv.pullingShards, shard)
			kv.mu.Unlock()
		}
	}()

	kv.mu.Lock()
	oldConfig := kv.lastConfig
	kv.mu.Unlock()

	// Because configs are processed sequentially, lastConfig is the source.
	sourceGid := oldConfig.Shards[shard]
	if sourceGid == 0 {
		// Brand new shard; install empty data.
		DPrintf("gid=%d tryPullShard: shard %d is new, installing empty", kv.gid, shard)
		op := Op{
			Type:            opInstall,
			ConfigNum:       currentConfigNum,
			SourceConfigNum: 0,
			Shard:           shard,
			ShardData:       make(map[string]string),
			LastSeqData:     make(map[int64]int64),
		}
		result := kv.startCommand(op)
		if result.Err == OK {
			success = true
		}
		return
	}

	// Pull from source group and install.
	DPrintf("gid=%d tryPullShard: found source gid=%d at config %d", kv.gid, sourceGid, oldConfig.Num)
	success = kv.pullAndInstallShard(shard, oldConfig, currentConfigNum)
}

// changeConfigTo proposes next config via Raft.
func (kv *ShardKV) changeConfigTo(newConfig shardctrler.Config) {
	kv.mu.Lock()
	// Guard against duplicate proposals.
	if newConfig.Num <= kv.config.Num {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:   opReconfigure,
		Config: newConfig,
	}
	result := kv.startCommand(op)
	if result.Err != OK {
		// Allow monitor to retry on proposal failure.
		kv.mu.Lock()
		kv.lastConfigNumStarted = kv.config.Num
		kv.mu.Unlock()
	}
}

// pullAndInstallShard repeatedly pulls from source group then installs.
func (kv *ShardKV) pullAndInstallShard(shard int, oldConfig shardctrler.Config, targetConfigNum int) bool {
	kv.mu.Lock()
	gid := oldConfig.Shards[shard]
	servers, ok := oldConfig.Groups[gid]
	kv.mu.Unlock()

	DPrintf("gid=%d pullAndInstallShard: shard=%d sourceGid=%d sourceConfigNum=%d targetConfigNum=%d",
		kv.gid, shard, gid, oldConfig.Num, targetConfigNum)

	if gid == 0 {
		return false
	}

	if !ok {
		DPrintf("gid=%d pullAndInstallShard: no servers found for gid=%d", kv.gid, gid)
		return false
	}

	migrateArgs := MigrateArgs{
		ConfigNum: oldConfig.Num,
		Shard:     shard,
	}

	maxRetries := 10
	for retry := 0; retry < maxRetries; retry++ {
		// Try each source server.
		for _, serverName := range servers {
			srv := kv.make_end(serverName)
			var migrateReply MigrateReply
			ok := srv.Call("ShardKV.Migrate", &migrateArgs, &migrateReply)
			if ok && migrateReply.Err == OK {
				op := Op{
					Type:            opInstall,
					ConfigNum:       targetConfigNum,
					SourceConfigNum: oldConfig.Num,
					Shard:           shard,
					ShardData:       migrateReply.Data,
					LastSeqData:     migrateReply.LastSeq,
				}
				result := kv.startCommand(op)
				if result.Err == OK {
					return true
				}
				// Likely lost leadership; let monitor retry later.
				DPrintf("gid=%d pullAndInstallShard: startCommand InstallShard failed, shard=%d err=%v", kv.gid, shard, result.Err)
				return false
			}
			if ok && migrateReply.Err == ErrWrongGroup {
				// Source not ready yet or already GC'ed this round; retry later.
				DPrintf("gid=%d pullAndInstallShard: source gid=%d not ready for shard=%d at config=%d", kv.gid, gid, shard, oldConfig.Num)
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	DPrintf("gid=%d pullAndInstallShard: failed after retries, shard=%d", kv.gid, shard)
	return false
}

// notifySourceToDeleteShard asks source group to GC and then clears local marker.
func (kv *ShardKV) notifySourceToDeleteShard(shard int, configNum int, oldConfig shardctrler.Config) {
	sourceGid := oldConfig.Shards[shard]
	if sourceGid == 0 {
		return // new shard has no source data to GC
	}

	servers, ok := oldConfig.Groups[sourceGid]
	if !ok {
		return
	}

	deleteArgs := DeleteShardArgs{
		ConfigNum: configNum,
		Shard:     shard,
	}

	// Notify any source replica; Raft in source group will serialize actual delete.
	for _, serverName := range servers {
		srv := kv.make_end(serverName)
		var deleteReply DeleteShardReply
		ok := srv.Call("ShardKV.DeleteShard", &deleteArgs, &deleteReply)
		if ok && deleteReply.Err == OK {
			// On success, clear destination-side pending GC marker.
			op := Op{
				Type:       opShardGC,
				ConfigNum:  configNum,
				Shard:      shard,
				IsGCSource: false, // 我们是目标组，只清理 shardsToGC
			}
			kv.startCommand(op)
			break
		}
	}
}

func isSameOp(op1 Op, op2 Op) bool {
	if op1.Type != op2.Type {
		return false
	}
	switch op1.Type {
	case opGet, opPut, opAppend:
		return op1.ClientId == op2.ClientId && op1.RequestId == op2.RequestId
	case opReconfigure:
		return op1.Config.Num == op2.Config.Num
	case opFreezeShard:
		return op1.ConfigNum == op2.ConfigNum && op1.Shard == op2.Shard
	case opInstall, opShardGC:
		return op1.ConfigNum == op2.ConfigNum && op1.Shard == op2.Shard
	}
	return false
}

// ===== Lifecycle / snapshot =====

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
	kv.frozenShardConfig = make(map[int]int)
	kv.pullingShards = make(map[int]bool)
	kv.shardsToGC = make(map[int]int)
	kv.lastConfigNumStarted = 0

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Restore snapshot if present.
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.restoreSnapshot(snapshot)
	}
	kv.lastConfigNumStarted = kv.config.Num

	go kv.applier()
	go kv.configMonitor()
	go kv.gcMonitor()

	return kv
}

// createSnapshot persists shardkv state through raft snapshot.
func (kv *ShardKV) createSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// Encode current state.
	e.Encode(kv.shardData)
	e.Encode(kv.lastSeq)
	e.Encode(kv.config)
	e.Encode(kv.lastConfig)
	e.Encode(kv.shardState)
	e.Encode(kv.frozenShardConfig)
	e.Encode(kv.shardsToGC)

	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}

// restoreSnapshot loads shardkv state from snapshot bytes.
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
	var frozenShardConfig map[int]int
	var shardsToGC map[int]int

	if d.Decode(&shardData) != nil ||
		d.Decode(&lastSeq) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&shardState) != nil ||
		d.Decode(&frozenShardConfig) != nil ||
		d.Decode(&shardsToGC) != nil {
		// Keep current state on decode failure.
		return
	}

	kv.shardData = shardData
	kv.lastSeq = lastSeq
	kv.config = config
	kv.lastConfig = lastConfig
	kv.shardState = shardState
	kv.frozenShardConfig = frozenShardConfig
	kv.shardsToGC = shardsToGC
	if kv.frozenShardConfig == nil {
		kv.frozenShardConfig = make(map[int]int)
	}
	if kv.shardsToGC == nil {
		kv.shardsToGC = make(map[int]int)
	}
}
