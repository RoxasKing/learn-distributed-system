package shardmaster

import (
	"context"
	"log"
	"sort"

	"github.com/RoxasKing/learn-distributed-system/labgob"
	"github.com/RoxasKing/learn-distributed-system/labrpc"
	"github.com/RoxasKing/learn-distributed-system/raft"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	// mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs     []Config         // indexed by config num
	Shards      [NShards]int     // shard -> gid
	Groups      map[int][]string // gid -> servers[]
	executedCmd map[int64]bool   // avoid duplicate request
	waitCh      chan struct{}    // execute oprationHandle one by one
	replyCh     chan *replyInfo  // receive replyInfo
	replyQueue  []*replyInfo     // list of replyInfo
	killCtx     context.Context  // receive a message that kills all for select loop
	killFunc    func()           // methods for killing all for select loops
}

type OpType int

const (
	JOIN OpType = iota + 1
	LEAVE
	MOVE
	QUERY
)

type Op struct {
	// Your data here.
	Uid     int64            // avoid re-executing same request
	Type    OpType           // opration type: Join/Leave/Move/Query
	Servers map[int][]string // if Type == JOIN, not null
	GIDs    []int            // if Type == LEAVE, not null
	Shard   int              // if Type == MOVE, not null
	GID     int              // if Type == MOVE, not null
	Num     int              // if Type == QUERY, not null
}

type replyMsg struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type replyInfo struct {
	idx int
	op  Op
	ch  chan *replyMsg
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	replyMsg := sm.oprationHandle(Op{Uid: args.Uid, Type: JOIN, Servers: args.Servers})

	reply.WrongLeader = replyMsg.WrongLeader
	reply.Err = replyMsg.Err

	DPrintf("ShardMaster {%d} Reply [JOIN] (%v) (%v)\n", sm.me, reply.WrongLeader, reply.Err)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	replyMsg := sm.oprationHandle(Op{Uid: args.Uid, Type: LEAVE, GIDs: args.GIDs})

	reply.WrongLeader = replyMsg.WrongLeader
	reply.Err = replyMsg.Err

	DPrintf("ShardMaster {%d} Reply [LEAVE] (%v) (%v)\n", sm.me, reply.WrongLeader, reply.Err)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	replyMsg := sm.oprationHandle(Op{Uid: args.Uid, Type: MOVE, Shard: args.Shard, GID: args.GID})

	reply.WrongLeader = replyMsg.WrongLeader
	reply.Err = replyMsg.Err

	DPrintf("ShardMaster {%d} Reply [MOVE] (%v) (%v)\n", sm.me, reply.WrongLeader, reply.Err)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	replyMsg := sm.oprationHandle(Op{Uid: args.Uid, Type: QUERY, Num: args.Num})

	reply.WrongLeader = replyMsg.WrongLeader
	reply.Err = replyMsg.Err
	reply.Config = replyMsg.Config

	DPrintf("ShardMaster {%d} Reply [QUERY] (%v) (%v)\n", sm.me, reply.WrongLeader, reply.Err)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.killFunc()
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	sm.Shards = [NShards]int{}
	sm.Groups = make(map[int][]string)
	sm.executedCmd = make(map[int64]bool)
	sm.waitCh = make(chan struct{}, 1)
	sm.replyCh = make(chan *replyInfo)
	sm.replyQueue = []*replyInfo{}
	sm.killCtx, sm.killFunc = context.WithCancel(context.Background())

	go run(sm)

	return sm
}

func run(sm *ShardMaster) {
	for {
		select {
		case info := <-sm.replyCh:
			sm.pushToReplyQueue(info)

		case msg := <-sm.applyCh:
			sm.applyHandle(msg)

		case <-sm.killCtx.Done():
			sm.rejectAllRequest()
			DPrintf("ShardMaster {%d} killed!\n", sm.me)
			return
		}
	}
}

func (sm *ShardMaster) oprationHandle(op Op) replyMsg {
	sm.waitCh <- struct{}{}
	defer func() { <-sm.waitCh }()

	index, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		return replyMsg{WrongLeader: true}
	}

	ch := make(chan *replyMsg, 1)
	sm.replyCh <- &replyInfo{idx: index, op: op, ch: ch}

	msg := <-ch
	return replyMsg{WrongLeader: msg.WrongLeader, Err: msg.Err, Config: msg.Config}
}

func (sm *ShardMaster) pushToReplyQueue(info *replyInfo) {
	if sm.executedCmd[info.op.Uid] {
		if info.op.Type == QUERY {
			info.ch <- &replyMsg{WrongLeader: false, Err: OK, Config: sm.queryHandle(info.op.Num)}
		} else {
			info.ch <- &replyMsg{WrongLeader: false, Err: OK}
		}
		return
	}

	sm.replyQueue = append(sm.replyQueue, info)
}

func (sm *ShardMaster) applyHandle(msg raft.ApplyMsg) {
	if !msg.CommandValid {
		if msg.StateChange {
			sm.rejectAllRequest()
		}
		return
	}

	op, _ := msg.Command.(Op)

	if !sm.executedCmd[op.Uid] {
		switch op.Type {
		case JOIN:
			sm.joinHandle(op.Servers)
		case LEAVE:
			sm.leaveHandle(op.GIDs)
		case MOVE:
			sm.moveHandle(op.Shard, op.GID)
		}
		sm.executedCmd[op.Uid] = true
	}

	if len(sm.replyQueue) > 0 && sm.replyQueue[0].idx == msg.CommandIndex {
		ch := sm.replyQueue[0].ch
		if op.Type == QUERY {
			ch <- &replyMsg{WrongLeader: false, Err: OK, Config: sm.queryHandle(op.Num)}
		} else {
			ch <- &replyMsg{WrongLeader: false, Err: OK}
		}
		sm.replyQueue = sm.replyQueue[1:]
	}

	DPrintf("ShardMaster {%d} Apply (%v) (%v) (%v)\n", sm.me, op.Uid, op.Type, msg.CommandIndex)
}

type gidShardsPair struct {
	gid    int
	shards []int
}

func (sm *ShardMaster) joinHandle(servers map[int][]string) {
	// gid -> shards pair list
	list := []gidShardsPair{}
	for gid, srvs := range servers {
		// save new gid -> servers map
		sm.Groups[gid] = srvs
		list = append(list, gidShardsPair{gid: gid})
	}

	idxs := []int{}
	shardsMap := make(map[int][]int)
	for shard, gid := range sm.Shards {
		if gid == 0 { // unassigned shard
			idxs = append(idxs, shard)
			continue
		}
		shardsMap[gid] = append(shardsMap[gid], shard)
	}

	for gid, shards := range shardsMap {
		list = append(list, gidShardsPair{gid: gid, shards: shards})
	}

	sort.Slice(list, func(i, j int) bool { return len(list[i].shards) < len(list[j].shards) })

	list[len(list)-1].shards = append(list[len(list)-1].shards, idxs...)

	// reassign shards
	sm.reassignShards(list)

	// add new config log
	newConfig := sm.makeConfig()
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) leaveHandle(gids []int) {
	// mark the group's gid to be removed
	mark := make(map[int]bool)
	for _, gid := range gids {
		mark[gid] = true
		delete(sm.Groups, gid)
	}

	idxs := []int{} // collect shards that need to be reassigned
	shardsMap := map[int][]int{}
	for shardIdx, gid := range sm.Shards {
		if mark[gid] {
			idxs = append(idxs, shardIdx)
		} else {
			shardsMap[gid] = append(shardsMap[gid], shardIdx)
		}
	}

	// gid -> shards map list
	list := []gidShardsPair{}
	// groups with no shards assigned
	for gid := range sm.Groups {
		if _, ok := shardsMap[gid]; !ok {
			shardsMap[gid] = []int{}
		}
	}
	// groups with shards assigned
	for gid, shards := range shardsMap {
		list = append(list, gidShardsPair{gid: gid, shards: shards})
	}

	// if contain no groups
	if len(list) == 0 {
		// initial shards map
		sm.Shards = [NShards]int{}
		// add new config log
		newConfig := sm.makeConfig()
		sm.configs = append(sm.configs, newConfig)
		return
	}

	sort.Slice(list, func(i, j int) bool { return len(list[i].shards) < len(list[j].shards) })

	list[len(list)-1].shards = append(list[len(list)-1].shards, idxs...)

	// reassign shards
	sm.reassignShards(list)

	// add new config log
	newConfig := sm.makeConfig()
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) moveHandle(shard, gid int) {
	sm.Shards[shard] = gid

	// add new config log
	newConfig := sm.makeConfig()
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) queryHandle(num int) Config {
	if num == -1 || num > len(sm.configs)-1 {
		return sm.configs[len(sm.configs)-1]
	}
	return sm.configs[num]
}

func (sm *ShardMaster) makeConfig() Config {
	num := len(sm.configs)           // config number
	shards := [NShards]int{}         // shard -> gid
	groups := make(map[int][]string) // gid -> servers[]
	for i := range sm.Shards {
		shards[i] = sm.Shards[i]
	}
	for k, v := range sm.Groups {
		groups[k] = v
	}

	return Config{Num: num, Shards: shards, Groups: groups}
}

// reassign the shards as evenly as possible
func (sm *ShardMaster) reassignShards(list []gidShardsPair) {
	min, max := 0, len(list)-1
	for len(list[min].shards)+1 < len(list[max].shards) {
		last := len(list[max].shards) - 1
		shardIdx := list[max].shards[last]
		list[max].shards = list[max].shards[:last]
		list[min].shards = append(list[min].shards, shardIdx)

		// re-sort
		sort.Slice(list, func(i, j int) bool { return len(list[i].shards) < len(list[j].shards) })
	}

	// remap shard index
	for _, pair := range list {
		for _, shard := range pair.shards {
			sm.Shards[shard] = pair.gid
		}
	}
}

func (sm *ShardMaster) rejectAllRequest() {
	for _, info := range sm.replyQueue {
		info.ch <- &replyMsg{WrongLeader: true}
	}
	sm.replyQueue = []*replyInfo{}
}
