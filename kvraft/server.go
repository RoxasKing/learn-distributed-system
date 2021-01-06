package kvraft

import (
	"bytes"
	"context"
	"log"
	"sync/atomic"

	"github.com/RoxasKing/learn-distributed-system/labgob"
	"github.com/RoxasKing/learn-distributed-system/labrpc"
	"github.com/RoxasKing/learn-distributed-system/raft"
)

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
	Uid int64  // avoid re-executing same request
	Typ string // Get, Put or Append
	Key string
	Val string
}

type replyMsg struct {
	err Err
	val string
}

type replyInfo struct {
	idx int           // start return's index
	op  Op            // opration
	ch  chan replyMsg // reply channel
}

type KVServer struct {
	// mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister   *raft.Persister   // for snapshot
	kvStore     map[string]string // store key-value pair
	excutedCmd  map[int64]bool    // avoid duplicate request
	uidList     []int64           // latest executed opration's uid
	lastOpIndex int               // last executed opration's index
	lastOpTerm  int               // last executed opration's term
	replyQueue  []*replyInfo      // list of replyInfo
	replyCh     chan *replyInfo   // receive replyInfo
	waitCh      chan struct{}     // ensure that requests join the queue in order
	snapshotCh  chan struct{}     // start snapshot signal
	snapshoting bool              // if snapshot is not finished, value is true
	killCtx     context.Context   // receive a message that kills all for select loop
	killFunc    func()            // methods for killing all for select loops

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	replyMsg := kv.oprationHandle(Op{
		Uid: args.Uid,
		Typ: "Get",
		Key: args.Key,
	})

	reply.Err = replyMsg.err
	reply.Value = replyMsg.val

	DPrintf("Server {%d} Reply (%v) (%v)\n", kv.me, args.Uid, reply.Err)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	replyMsg := kv.oprationHandle(Op{
		Uid: args.Uid,
		Typ: args.Op,
		Key: args.Key,
		Val: args.Value,
	})

	reply.Err = replyMsg.err

	DPrintf("Server {%d} Reply (%v) (%v)\n", kv.me, args.Uid, reply.Err)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killFunc()
}

// func (kv *KVServer) killed() bool {
// 	z := atomic.LoadInt32(&kv.dead)
// 	return z == 1
// }

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvStore = make(map[string]string)
	kv.excutedCmd = make(map[int64]bool)
	kv.uidList = []int64{}
	kv.lastOpIndex = 0
	kv.lastOpTerm = 0
	kv.replyQueue = []*replyInfo{}
	kv.replyCh = make(chan *replyInfo)
	kv.waitCh = make(chan struct{}, 1)
	kv.snapshotCh = make(chan struct{})
	kv.snapshoting = false
	kv.killCtx, kv.killFunc = context.WithCancel(context.Background())

	// initialize from snapshot before a crash
	kv.readSnapshot(persister.ReadSnapshot())

	go run(kv)

	return kv
}

func run(kv *KVServer) {
	for {
		select {
		case info := <-kv.replyCh:
			kv.pushToReplyQueue(info)

		case msg := <-kv.applyCh:
			kv.applyHandle(msg)

		case <-kv.snapshotCh:
			kv.makeSnapshot(kv.lastOpIndex, kv.lastOpTerm)

		case <-kv.killCtx.Done():
			kv.rejectAllRequest()
			DPrintf("KVServer {%d} killed!\n", kv.me)
			return
		}
	}
}

func (kv *KVServer) oprationHandle(op Op) replyMsg {
	kv.waitCh <- struct{}{}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader || index == -1 {
		<-kv.waitCh
		return replyMsg{err: ErrWrongLeader}
	}

	DPrintf("Server {%d} Start (%v) (%v) (%v)\n", kv.me, op.Uid, op.Typ, index)

	ch := make(chan replyMsg, 1)

	kv.replyCh <- &replyInfo{
		idx: index,
		op:  op,
		ch:  ch,
	}
	<-kv.waitCh

	msg := <-ch
	return replyMsg{msg.err, msg.val}
}

func (kv *KVServer) pushToReplyQueue(info *replyInfo) {
	if info.idx == kv.lastOpIndex {
		if info.op.Typ == "Get" {
			info.ch <- replyMsg{err: OK, val: kv.kvStore[info.op.Key]}
		} else {
			info.ch <- replyMsg{err: OK}
		}
		return
	}

	kv.replyQueue = append(kv.replyQueue, info)
}

func (kv *KVServer) applyHandle(msg raft.ApplyMsg) {
	if !msg.CommandValid {
		if msg.StateChange {
			kv.rejectAllRequest()
			return
		}
		kv.readSnapshot(msg.Snapshot)
		kv.snapshoting = false
		return
	}

	op, _ := msg.Command.(Op)

	if !kv.excutedCmd[op.Uid] && op.Typ != "Get" {
		switch op.Typ {
		case "Put":
			kv.kvStore[op.Key] = op.Val
		case "Append":
			kv.kvStore[op.Key] += op.Val
		}
		kv.excutedCmd[op.Uid] = true
		kv.uidList = append(kv.uidList, op.Uid)
		if len(kv.uidList) > 49 {
			kv.uidList = kv.uidList[1:]
		}
	}

	kv.lastOpIndex = msg.CommandIndex
	kv.lastOpTerm = msg.CommandTerm

	if len(kv.replyQueue) > 0 && kv.replyQueue[0].idx == msg.CommandIndex {
		ch := kv.replyQueue[0].ch
		if op.Typ == "Get" {
			ch <- replyMsg{err: OK, val: kv.kvStore[op.Key]}
		} else {
			ch <- replyMsg{err: OK}
		}
		kv.replyQueue = kv.replyQueue[1:]
	}

	if msg.IsLeader && !kv.snapshoting && kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate*2 {
		go func() { kv.snapshotCh <- struct{}{} }()
		kv.snapshoting = true
	}

	DPrintf("Server {%d} Apply (%v) (%v) (%v)", kv.me, op.Uid, op.Typ, msg.CommandIndex)
}

func (kv *KVServer) makeSnapshot(lastIncludedIndex, lastIncludedTerm int) {
	w := new(bytes.Buffer)
	d := labgob.NewEncoder(w)
	_ = d.Encode(lastIncludedIndex)
	_ = d.Encode(lastIncludedTerm)
	_ = d.Encode(kv.kvStore)
	_ = d.Encode(kv.uidList)
	data := w.Bytes()

	go kv.rf.LogCompaction(lastIncludedIndex, lastIncludedTerm, data)

	DPrintf("Server {%d} make snapshot, size(%d), lastIndex(%d)\n", kv.me, len(data), lastIncludedIndex)
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex, lastIncludedTerm int
	var kvStore map[string]string
	var uidList []int64

	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&kvStore) != nil ||
		d.Decode(&uidList) != nil {
		return
	}

	// snapshot is newer than current state
	if lastIncludedIndex > kv.lastOpIndex {
		kv.kvStore = kvStore
		for _, uid := range uidList {
			kv.excutedCmd[uid] = true
		}
		kv.uidList = uidList
		kv.lastOpIndex = lastIncludedIndex
		kv.lastOpTerm = lastIncludedTerm
	}

	DPrintf("Server {%d} installed snapshot\n", kv.me)
}

func (kv *KVServer) rejectAllRequest() {
	for _, info := range kv.replyQueue {
		info.ch <- replyMsg{err: ErrWrongLeader}
	}
	kv.replyQueue = []*replyInfo{}
}
