package shardkv

import (
	"bytes"
	"context"
	"log"
	"sync"
	"time"

	"github.com/RoxasKing/learn-distributed-system/labgob"
	"github.com/RoxasKing/learn-distributed-system/labrpc"
	"github.com/RoxasKing/learn-distributed-system/raft"
	sm "github.com/RoxasKing/learn-distributed-system/shardmaster"
)

const pollNewConfigInterval = 50 * time.Millisecond

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Uid int64     // avoid re-executing same request
	Typ string    // Get/Put/Append, or Cfg/Add/Del
	Key string    // if Typ == Get/Put/Append, not null
	Val string    // if Typ == Put/Append, not null
	Cfg sm.Config // if Typ == Cfg, not null
	Add AddShard  // if Typ == Add, not null
	Del DelShard  // if Typ == Del, not null
}

type AddShard struct {
	Num     int // config number
	Shards  []int
	ShardKV [sm.NShards]map[string]string
	UidList [sm.NShards][]int64
}

type DelShard struct {
	Num    int // config number
	Shards []int
}

type migShard struct {
	migShards map[int][]int
	shardKV   [sm.NShards]map[string]string
	uidList   [sm.NShards][]int64
}

type replyMsg struct {
	err Err
	val string
}

type reqInfo struct {
	idx int           // start return's index
	op  Op            // opration
	ch  chan replyMsg // reply channel
}

type checkKeyParam struct {
	key string
	ch  chan Err
}

type migrateShardParam struct {
	args *MigrateShardArgs
	ch   chan *MigrateShardReply
}

type ShardKV struct {
	// mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister   *raft.Persister               // for snapshot
	config      sm.Config                     // current config
	valid       [sm.NShards]bool              // shard valid or not for current group
	validCh     chan chan [sm.NShards]bool    // get shard valid status
	shardKV     [sm.NShards]map[string]string // store key-value pair, one map per shard
	uidList     [sm.NShards][]int64           // recently executed opration's uid
	executed    map[int64]bool                // prevented repeated execution
	lastOpIndex int                           // last executed opration's index
	lastOpTerm  int                           // last executed opration's term
	limitCh     chan struct{}                 // ensure that requests join the queue sequentially
	checkKeyCh  chan checkKeyParam            // check if request can processed by this group
	reqCh       chan *reqInfo                 // receive opration requests that need to join the queue
	reqQueue    []*reqInfo                    // opration request queue waiting for reply
	getCfgCh    chan chan sm.Config           // get current config
	cfgNotify   chan sm.Config                // start checking config
	taskTimer   *time.Ticker                  // config check timer
	killCtx     context.Context               // receive a message that kills all for select loop
	killFunc    func()                        // methods for killing all for select loops

	received    [sm.NShards]bool              // mark received shards
	recShards   []int                         // shards that need to be received
	recShardKV  [sm.NShards]map[string]string // received ShardKV
	recUidList  [sm.NShards][]int64           // received UidList
	getRecShdCh chan chan AddShard            // get all received shard data
	finishRec   bool                          // finish receive or not
	getFinRecCh chan chan bool                // get finishRec status

	migShards   map[int][]int      // shards will migrate to new group
	getMigShdCh chan chan migShard // get all shards that will migrate to new group

	migrateCh chan migrateShardParam // receive MigrateShard request
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	reply.Err, reply.Value = kv.requestProcess(Op{
		Uid: args.Uid,
		Typ: "Get",
		Key: args.Key,
	}, args.Num)

	DPrintf("Gid(%d) Server {%d} Reply (%v) (%v)\n", kv.gid, kv.me, args.Uid, reply.Err)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	reply.Err, _ = kv.requestProcess(Op{
		Uid: args.Uid,
		Typ: args.Op,
		Key: args.Key,
		Val: args.Value,
	}, args.Num)

	DPrintf("Gid(%d) Server {%d} Reply (%v) (%v)\n", kv.gid, kv.me, args.Uid, reply.Err)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killFunc()
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.config = sm.Config{}
	kv.valid = [sm.NShards]bool{}
	kv.validCh = make(chan chan [sm.NShards]bool)
	kv.shardKV = [sm.NShards]map[string]string{}
	for i := 0; i < sm.NShards; i++ {
		kv.shardKV[i] = make(map[string]string)
	}
	kv.uidList = [sm.NShards][]int64{}
	kv.executed = make(map[int64]bool)
	kv.lastOpIndex = 0
	kv.lastOpTerm = 0
	kv.limitCh = make(chan struct{}, 1)
	kv.checkKeyCh = make(chan checkKeyParam)
	kv.reqCh = make(chan *reqInfo)
	kv.reqQueue = []*reqInfo{}
	kv.getCfgCh = make(chan chan sm.Config)
	kv.cfgNotify = make(chan sm.Config)
	kv.taskTimer = time.NewTicker(pollNewConfigInterval)
	kv.killCtx, kv.killFunc = context.WithCancel(context.Background())

	kv.received = [sm.NShards]bool{}
	kv.recShards = []int{}
	kv.recShardKV = [sm.NShards]map[string]string{}
	kv.recUidList = [sm.NShards][]int64{}
	kv.getRecShdCh = make(chan chan AddShard)
	kv.finishRec = true
	kv.getFinRecCh = make(chan chan bool)

	kv.migShards = make(map[int][]int)
	kv.getMigShdCh = make(chan chan migShard)

	kv.migrateCh = make(chan migrateShardParam)

	// initialize from snapshot before a crash
	kv.readSnapshot(persister.ReadSnapshot())

	go run(kv)
	go timer(kv)

	DPrintf("Gid(%d) Server {%d} Start!\n", kv.gid, kv.me)

	return kv
}

//---------------------------------------------------------------------------------------------------------------------

func run(kv *ShardKV) {
	for {
		select {

		case info := <-kv.reqCh:
			kv.pushToOpQueue(info)

		case p := <-kv.checkKeyCh:
			p.ch <- kv.checkKeyProcess(p.key)

		case msg := <-kv.applyCh:
			kv.applyMsgProcess(msg)

		case ch := <-kv.validCh:
			ch <- kv.valid

		case ch := <-kv.getCfgCh:
			ch <- kv.config

		case ch := <-kv.getFinRecCh:
			ch <- kv.finishRec

		case ch := <-kv.getRecShdCh:
			ch <- kv.getRecShard()

		case ch := <-kv.getMigShdCh:
			ch <- kv.getMigShard()

		case p := <-kv.migrateCh:
			p.ch <- kv.migrateShardProcess(p.args)

		case cfg := <-kv.cfgNotify:
			kv.checkConfig(cfg)

		case <-kv.killCtx.Done():
			kv.rejectAllRequest()
			DPrintf("Gid(%d) Server {%d} killed!\n", kv.gid, kv.me)
			return
		}
	}
}

func timer(kv *ShardKV) {
	for {
		select {
		case <-kv.taskTimer.C:
			// get latest config
			clerk := sm.MakeClerk(kv.masters)
			newCfg := clerk.Query(-1)
			kv.cfgNotify <- newCfg

		case <-kv.killCtx.Done():
			return
		}
	}
}

// --------------------------------------------------------------------------------------------------------------------

func (kv *ShardKV) requestProcess(op Op, num int) (Err, string) {
	kv.limitCh <- struct{}{}

	// check if the request is to the wrong group,
	// or need to wait new shard add to the state database
	errCh := make(chan Err, 1)
	kv.checkKeyCh <- checkKeyParam{key: op.Key, ch: errCh}
	err := <-errCh

	if err != OK {
		<-kv.limitCh
		return err, ""
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader || index == -1 {
		<-kv.limitCh
		return ErrWrongLeader, ""
	}

	DPrintf("Gid(%d) Server {%d} Start (%v) (%v) (%v)\n", kv.gid, kv.me, op.Uid, op.Typ, index)

	ch := make(chan replyMsg, 1)

	kv.reqCh <- &reqInfo{
		idx: index,
		op:  op,
		ch:  ch,
	}
	<-kv.limitCh

	msg := <-ch
	return msg.err, msg.val
}

func (kv *ShardKV) checkKeyProcess(key string) Err {
	shard := key2shard(key)
	if kv.config.Shards[shard] != kv.gid {
		return ErrWrongGroup
	} else if !kv.valid[shard] {
		return ErrNotReadyYet
	}
	return OK
}

func (kv *ShardKV) pushToOpQueue(info *reqInfo) {
	if info.idx == kv.lastOpIndex {
		op, ch := info.op, info.ch
		if op.Typ == "Get" {
			kv.getProcess(op, ch)
		} else {
			kv.putAppendProcess(op, ch)
		}
		return
	}

	kv.reqQueue = append(kv.reqQueue, info)
}
func (kv *ShardKV) rejectAllRequest() {
	for _, info := range kv.reqQueue {
		info.ch <- replyMsg{err: ErrWrongLeader}
	}
	kv.reqQueue = []*reqInfo{}
}

func (kv *ShardKV) applyMsgProcess(msg raft.ApplyMsg) {
	if !msg.CommandValid {
		if msg.StateChange {
			kv.rejectAllRequest()
			return
		}
		kv.readSnapshot(msg.Snapshot)
		return
	}

	kv.lastOpIndex = msg.CommandIndex
	kv.lastOpTerm = msg.CommandTerm

	op, _ := msg.Command.(Op)

	var ch chan replyMsg
	if len(kv.reqQueue) > 0 && kv.reqQueue[0].idx == msg.CommandIndex {
		ch = kv.reqQueue[0].ch
		kv.reqQueue = kv.reqQueue[1:]
	}

	if (op.Typ == "Get" || op.Typ == "Put" || op.Typ == "Append") && ch == nil {
		ch = make(chan replyMsg, 1)
	}

	switch op.Typ {
	case "Get":
		kv.getProcess(op, ch)

	case "Put", "Append":
		kv.putAppendProcess(op, ch)

	case "Cfg":
		kv.updateConfig(op.Cfg)

	case "Add":
		kv.addNewShard(op.Add)

	case "Del":
		kv.delOldShard(op.Del)
	}

	if msg.IsLeader && kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate*2 {
		kv.makeSnapshot(kv.lastOpIndex, kv.lastOpTerm)
	}

	DPrintf("Gid(%d) Server {%d} [APPLY] uid(%v) typ(%v) key(%v) val(%v) idx(%v)",
		kv.gid, kv.me, op.Uid, op.Typ, op.Key, op.Val, msg.CommandIndex)

	if op.Typ == "Add" || op.Typ == "Del" {
		shards := []int{}
		for shard := 0; shard < sm.NShards; shard++ {
			if kv.valid[shard] {
				shards = append(shards, shard)
			}
		}
		DPrintf("Gid(%d) Server {%d} config num(%d), valid shards(%v)\n", kv.gid, kv.me, kv.config.Num, shards)
	}
}

func (kv *ShardKV) getProcess(op Op, ch chan replyMsg) {
	if err := kv.checkKeyProcess(op.Key); err != OK {
		ch <- replyMsg{err: err}
		return
	}

	shard := key2shard(op.Key)
	if val, ok := kv.shardKV[shard][op.Key]; !ok {
		ch <- replyMsg{err: ErrNoKey}
	} else {
		ch <- replyMsg{err: OK, val: val}
	}
}

func (kv *ShardKV) putAppendProcess(op Op, ch chan replyMsg) {
	if err := kv.checkKeyProcess(op.Key); err != OK {
		ch <- replyMsg{err: err}
		return
	} else if kv.executed[op.Uid] {
		ch <- replyMsg{err: OK}
		return
	}

	shard := key2shard(op.Key)
	if op.Typ == "Put" {
		kv.shardKV[shard][op.Key] = op.Val
	} else if op.Typ == "Append" {
		kv.shardKV[shard][op.Key] += op.Val
	}

	kv.uidList[shard] = append(kv.uidList[shard], op.Uid)
	if len(kv.uidList[shard]) > 200 {
		kv.uidList[shard] = kv.uidList[shard][1:]
	}
	kv.executed[op.Uid] = true
	ch <- replyMsg{err: OK}
}

func (kv *ShardKV) updateConfig(newCfg sm.Config) {
	// current config is up to date
	if kv.config.Num >= newCfg.Num {
		DPrintf("Gid(%d) Server {%d}current config num(%d) >= new config num(%d) , [SKIP]\n",
			kv.gid, kv.me, kv.config.Num, newCfg.Num)
		return
	}

	// update to new config
	kv.config = newCfg

	kv.received = [sm.NShards]bool{}
	kv.recShards = []int{}
	kv.migShards = make(map[int][]int)
	for shard, gid := range newCfg.Shards {
		if gid == kv.gid && !kv.valid[shard] {
			kv.recShards = append(kv.recShards, shard)
			kv.recShardKV = [sm.NShards]map[string]string{}
			kv.recUidList = [sm.NShards][]int64{}
		} else if gid != kv.gid && kv.valid[shard] {
			kv.migShards[gid] = append(kv.migShards[gid], shard)
		}
	}
	kv.finishRec = len(kv.recShards) == 0
	DPrintf("Gid(%d) Server {%d} [Update] [Config], num(%d)\n", kv.gid, kv.me, newCfg.Num)
}

func (kv *ShardKV) addNewShard(add AddShard) {
	if add.Num != kv.config.Num {
		DPrintf("Gid(%d) Server {%d} [Abort] [Add] shards(%d), add num(%d) != config num(%d)\n",
			kv.gid, kv.me, add.Shards, add.Num, kv.config.Num)
	}
	for _, shard := range add.Shards {
		if kv.valid[shard] {
			DPrintf("Gid(%d) Server {%d} [Skip] [Add] shard(%d), already owned\n", kv.gid, kv.me, shard)
			continue
		}
		if kv.config.Shards[shard] != kv.gid {
			DPrintf("Gid(%d) Server {%d} [Skip] [Add] shard(%d), useless for this group\n", kv.gid, kv.me, shard)
			continue
		}
		kv.valid[shard] = true
		kv.shardKV[shard] = make(map[string]string)
		for key, val := range add.ShardKV[shard] {
			kv.shardKV[shard][key] = val
		}
		kv.uidList[shard] = append(kv.uidList[shard], add.UidList[shard]...)
		// kv.uidList[shard] = make([]int64, len(add.UidList))
		// copy(kv.uidList[shard], add.UidList[shard])
		for _, uid := range add.UidList[shard] {
			kv.executed[uid] = true
		}
		if len(add.UidList[shard]) > 0 {
			DPrintf("add shard(%d) last uid(%d)\n", shard, add.UidList[shard][len(add.UidList[shard])-1])
		}
		DPrintf("Gid(%d) Server {%d} [Add] shard(%d)\n", kv.gid, kv.me, shard)
	}
}

func (kv *ShardKV) delOldShard(del DelShard) {
	if del.Num != kv.config.Num {
		DPrintf("Gid(%d) Server {%d} [Abort] [Del] shards(%d), add num(%d) != config num(%d)\n",
			kv.gid, kv.me, del.Shards, del.Num, kv.config.Num)
	}
	for _, shard := range del.Shards {
		if !kv.valid[shard] {
			DPrintf("Gid(%d) Server {%d} [Skip] [Del] shard(%d), not owned\n", kv.gid, kv.me, shard)
			continue
		}
		if kv.config.Shards[shard] == kv.gid {
			DPrintf("Gid(%d) Server {%d} [Skip] [Del] shard(%d), useful for this group\n", kv.gid, kv.me, shard)
			continue
		}
		kv.valid[shard] = false
		kv.shardKV[shard] = make(map[string]string)
		kv.uidList[shard] = []int64{}
		DPrintf("Gid(%d) Server {%d} [Del] shard(%d)\n", kv.gid, kv.me, shard)
	}
}

func (kv *ShardKV) makeSnapshot(lastIncludedIndex, lastIncludedTerm int) {
	w := new(bytes.Buffer)
	d := labgob.NewEncoder(w)
	_ = d.Encode(lastIncludedIndex)
	_ = d.Encode(lastIncludedTerm)
	_ = d.Encode(kv.config)
	_ = d.Encode(kv.valid)
	_ = d.Encode(kv.shardKV)
	_ = d.Encode(kv.uidList)
	snapshot := w.Bytes()

	go kv.rf.LogCompaction(lastIncludedIndex, lastIncludedTerm, snapshot)

	DPrintf("Gid(%d) Server {%d} [MAKE] snapshot, size(%d), lastIndex(%d)\n",
		kv.gid, kv.me, len(snapshot), lastIncludedIndex)
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex, lastIncludedTerm int
	var config sm.Config
	var valid [sm.NShards]bool
	var shardKV [sm.NShards]map[string]string
	var uidList [sm.NShards][]int64

	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&valid) != nil ||
		d.Decode(&shardKV) != nil ||
		d.Decode(&uidList) != nil {
		return
	}

	if lastIncludedIndex <= kv.lastOpIndex {
		return
	}

	kv.config = config
	kv.valid = valid
	kv.shardKV = shardKV
	kv.uidList = uidList
	for shard := 0; shard < sm.NShards; shard++ {
		for _, uid := range uidList[shard] {
			kv.executed[uid] = true
		}
	}
	kv.lastOpIndex = lastIncludedIndex
	kv.lastOpTerm = lastIncludedTerm
	DPrintf("Gid(%d) Server {%d} [READ] snapshot\n", kv.gid, kv.me)
}

func (kv *ShardKV) checkConfig(newCfg sm.Config) {
	if newCfg.Num <= kv.config.Num {
		return
	}

	DPrintf("Gid(%d) Server {%d} [Find] [New] [Config] num(%d)\n", kv.gid, kv.me, newCfg.Num)

	kv.taskTimer.Stop()

	oldCfg := kv.config
	go kv.changeConfigProcess(oldCfg, newCfg)
}

func (kv *ShardKV) changeConfigProcess(oldCfg, newCfg sm.Config) {
	DPrintf("Gid(%d) Server {%d} [Start] [Change] config, num(%d) shards(%d)\n",
		kv.gid, kv.me, newCfg.Num, newCfg.Shards)
	defer DPrintf("Gid(%d) Server {%d} [Finish] [Change] [Config] num(%d) shards(%d)\n",
		kv.gid, kv.me, newCfg.Num, newCfg.Shards)
	defer func() { kv.taskTimer.Reset(pollNewConfigInterval) }()

	newCfg = kv.startNewConfig(oldCfg, newCfg)

	// if old config is 0, not need to migrate or wait for shard, start add directly
	if oldCfg.Num == 0 {
		shards := []int{}
		shardKV := [sm.NShards]map[string]string{}
		uidList := [sm.NShards][]int64{}
		for shard, gid := range newCfg.Shards {
			if gid == kv.gid {
				shards = append(shards, shard)
				shardKV[shard] = make(map[string]string)
				uidList[shard] = []int64{}
			}
		}

		if len(shards) == 0 {
			return
		}

		add := AddShard{
			Num:     newCfg.Num,
			Shards:  shards,
			ShardKV: shardKV,
			UidList: uidList,
		}
		kv.startAddShard(add)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)

	migCh := make(chan migShard, 1)
	kv.getMigShdCh <- migCh
	mig := <-migCh

	// migrate invalid shard, and start "Del"
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		var wg2 sync.WaitGroup
		for gid, shards := range mig.migShards {
			wg2.Add(1)
			shardKV := [sm.NShards]map[string]string{}
			uidList := [sm.NShards][]int64{}
			for _, shard := range shards {
				shardKV[shard] = mig.shardKV[shard]
				uidList[shard] = mig.uidList[shard]
			}
			args := &MigrateShardArgs{
				Num:     newCfg.Num,
				Shards:  shards,
				ShardKV: shardKV,
				UidList: uidList,
			}
			servers := newCfg.Groups[gid]
			go kv.sendMigrateShardRPC(args, servers, &wg2)
		}
		wg2.Wait()
	}(&wg)

	// check received shard, and start "Add"
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		for {
			time.Sleep(100 * time.Millisecond)
			finishRecCh := make(chan bool, 1)
			kv.getFinRecCh <- finishRecCh
			finishRec := <-finishRecCh
			if finishRec {
				break
			}
		}

		ch := make(chan AddShard, 1)
		kv.getRecShdCh <- ch
		add := <-ch

		if len(add.Shards) == 0 {
			return
		}

		kv.startAddShard(add)
	}(&wg)

	wg.Wait()
}

func (kv *ShardKV) sendMigrateShardRPC(args *MigrateShardArgs, servers []string, wg *sync.WaitGroup) {
	defer wg.Done()
LOOP:
	for {
		outOfdate := false
		for _, server := range servers {
			reply := &MigrateShardReply{}
			srv := kv.make_end(server)
			ok := srv.Call("ShardKV.MigrateShard", args, reply)

			if ok && reply.Complete {
				break LOOP
			}

			if ok && reply.Err == ErrLowVer {
				outOfdate = true
			}
		}
		if outOfdate {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	del := DelShard{
		Num:    args.Num,
		Shards: args.Shards,
	}
	kv.startDelShard(del)
}

func (kv *ShardKV) startNewConfig(oldCfg, newCfg sm.Config) sm.Config {
START:
	if _, _, isLeader := kv.rf.Start(Op{Typ: "Cfg", Cfg: newCfg}); isLeader {
		DPrintf("Gid(%d) Server {%d} [Start] [Cfg] num(%d), shard(%d)\n", kv.gid, kv.me, newCfg.Num, newCfg.Shards)
	}

	time.Sleep(100 * time.Millisecond)

	// get current config
	cfgCh := make(chan sm.Config, 1)
	kv.getCfgCh <- cfgCh
	cfg := <-cfgCh

	if cfg.Num == oldCfg.Num {
		clerk := sm.MakeClerk(kv.masters)
		newCfg = clerk.Query(-1)
		goto START
	}

	return cfg
}

func (kv *ShardKV) startAddShard(add AddShard) {
START:
	if _, _, isLeader := kv.rf.Start(Op{Typ: "Add", Add: add}); isLeader {
		DPrintf("Gid(%d) Server {%d} [Start] [Add] num(%d), shard(%d)\n", kv.gid, kv.me, add.Num, add.Shards)
	}

	time.Sleep(100 * time.Millisecond)

	// get current valid shards
	validCh := make(chan [sm.NShards]bool, 1)
	kv.validCh <- validCh
	valid := <-validCh

	for _, shard := range add.Shards {
		if !valid[shard] {
			goto START
		}
	}
}

func (kv *ShardKV) startDelShard(del DelShard) {
START:
	if _, _, isLeader := kv.rf.Start(Op{Typ: "Del", Del: del}); isLeader {
		DPrintf("Gid(%d) Server {%d} [Start] [Del] num(%d), shard(%d)\n", kv.gid, kv.me, del.Num, del.Shards)
	}

	time.Sleep(100 * time.Millisecond)

	// get current valid shards
	validCh := make(chan [sm.NShards]bool, 1)
	kv.validCh <- validCh
	valid := <-validCh

	for _, shard := range del.Shards {
		if valid[shard] {
			goto START
		}
	}
}

func (kv *ShardKV) getRecShard() AddShard {
	shards := []int{}
	shardKV := [sm.NShards]map[string]string{}
	uidList := [sm.NShards][]int64{}
	for _, shard := range kv.recShards {
		if kv.received[shard] {
			shards = append(shards, shard)
			shardKV[shard] = kv.recShardKV[shard]
			uidList[shard] = kv.recUidList[shard]
		}
	}
	return AddShard{
		Num:     kv.config.Num,
		Shards:  shards,
		ShardKV: shardKV,
		UidList: uidList,
	}
}

func (kv *ShardKV) getMigShard() migShard {
	shardKV := [sm.NShards]map[string]string{}
	uidList := [sm.NShards][]int64{}
	for _, shards := range kv.migShards {
		for _, shard := range shards {
			shardKV[shard] = make(map[string]string)
			for k, v := range kv.shardKV[shard] {
				shardKV[shard][k] = v
			}
			uidList[shard] = make([]int64, len(kv.uidList[shard]))
			copy(uidList[shard], kv.uidList[shard])
			if len(uidList[shard]) > 0 {
				DPrintf("migrate shard(%d) last uid(%d)\n", shard, uidList[shard][len(uidList[shard])-1])
			}
		}
	}
	return migShard{
		migShards: kv.migShards,
		shardKV:   shardKV,
		uidList:   uidList,
	}
}

type MigrateShardArgs struct {
	Num     int // config number
	Shards  []int
	ShardKV [sm.NShards]map[string]string
	UidList [sm.NShards][]int64
}

type MigrateShardReply struct {
	Err      Err
	Complete bool
}

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	ch := make(chan *MigrateShardReply, 1)
	kv.migrateCh <- migrateShardParam{args: args, ch: ch}
	msg := <-ch

	reply.Err = msg.Err
	reply.Complete = msg.Complete
}

func (kv *ShardKV) migrateShardProcess(args *MigrateShardArgs) *MigrateShardReply {
	complete := true
	for _, shard := range args.Shards {
		if !kv.valid[shard] {
			complete = false
			break
		}
	}

	if args.Num < kv.config.Num {
		DPrintf("Gid(%d) Server {%d} [Reply] [MigrateShard] Request{num:(%d) shards:(%v)}, [Err](%v), [Complete](%v)",
			kv.gid, kv.me, args.Num, args.Shards, ErrLowVer, complete)
		return &MigrateShardReply{Err: ErrLowVer, Complete: complete}
	}

	if args.Num > kv.config.Num {
		kv.finishRec = true
		DPrintf("Gid(%d) Server {%d} [Reply] [MigrateShard] Request{num:(%d) shards:(%v)}, [Err](%v), [Complete](%v)",
			kv.gid, kv.me, args.Num, args.Shards, ErrHighVer, complete)
		return &MigrateShardReply{Err: ErrHighVer, Complete: complete}
	}

	for _, shard := range args.Shards {
		if kv.valid[shard] {
			DPrintf("Gid(%d) Server {%d} [SKIP] [receive] shard(%d), already valid\n", kv.gid, kv.me, shard)
			continue
		}
		if kv.received[shard] {
			DPrintf("Gid(%d) Server {%d} [SKIP] [receive] shard(%d), alread received\n", kv.gid, kv.me, shard)
			continue
		}
		kv.received[shard] = true
		kv.recShardKV[shard] = args.ShardKV[shard]
		kv.recUidList[shard] = args.UidList[shard]
		DPrintf("Gid(%d) Server {%d} [receive] shard(%d)\n", kv.gid, kv.me, shard)
	}

	count := 0
	for _, shard := range kv.recShards {
		if kv.received[shard] {
			count++
		}
	}

	// have received all new shards
	if count == len(kv.recShards) {
		kv.finishRec = true
	}

	DPrintf("Gid(%d) Server {%d} [Reply] [MigrateShard] Request{num:(%d) shards:(%v)}, [Err](%v), [Complete](%v)",
		kv.gid, kv.me, args.Num, args.Shards, OK, complete)
	return &MigrateShardReply{Err: OK, Complete: complete}
}
