package kvraft

import (
	"context"
	"log"
	"sync/atomic"
	"time"

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

type subInfo struct {
	idx int
	uid int64
	typ string
	key string
}

type subReq struct {
	ch   chan replyMsg
	info subInfo
}

type KVServer struct {
	// mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore   map[string]string         // store key-value pair
	subReqCh  chan subReq               // recieve subscribe request
	subscribe map[chan replyMsg]subInfo // subscribe applyMsg
	uidList   []int64                   // store every cmd's uid
	excutedOp map[int64]bool            // avoid execute twice
	killCtx   context.Context           // receive a message that kills all for select loop
	killFunc  func()                    // methods for killing all for select loops
	waitCh    chan struct{}             // if linearizability is required, use it
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvStore = make(map[string]string)
	kv.subReqCh = make(chan subReq)
	kv.subscribe = make(map[chan replyMsg]subInfo)
	kv.uidList = []int64{}
	kv.excutedOp = make(map[int64]bool)
	kv.killCtx, kv.killFunc = context.WithCancel(context.Background())
	kv.waitCh = make(chan struct{}, 1)

	go run(kv)

	return kv
}

func run(kv *KVServer) {
	for {
		select {
		case req := <-kv.subReqCh:
			kv.subReqHandle(req)
		case msg := <-kv.applyCh:
			kv.applyHandle(msg)
		case <-kv.killCtx.Done():
			return
		}
	}
}

func (kv *KVServer) oprationHandle(op Op) replyMsg {
	// kv.waitCh <- struct{}{}
	// defer func() { <-kv.waitCh }()

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader || index == -1 {
		return replyMsg{err: ErrWrongLeader}
	}

	DPrintf("Server {%d} Start (%v) (%v) (%v)\n", kv.me, op.Uid, op.Typ, index)

	ch := make(chan replyMsg, 1)

	kv.subReqCh <- subReq{
		ch: ch,
		info: subInfo{
			idx: index,
			uid: op.Uid,
			typ: op.Typ,
			key: op.Key,
		},
	}

	// Warn: avoid wait too long
	select {
	case msg := <-ch:
		return replyMsg{msg.err, msg.val}
	case <-time.After(1 * time.Second):
		return replyMsg{err: ErrWrongLeader}
	}
}

func (kv *KVServer) subReqHandle(req subReq) {
	ch, info := req.ch, req.info

	if len(kv.uidList) < info.idx {
		kv.subscribe[ch] = info
		return
	}

	if kv.uidList[req.info.idx-1] != req.info.uid {
		req.ch <- replyMsg{err: ErrWrongLeader}
		return
	}

	if info.typ == "Get" {
		req.ch <- replyMsg{err: OK, val: kv.kvStore[info.key]}
	} else {
		req.ch <- replyMsg{err: OK}
	}
}

func (kv *KVServer) applyHandle(msg raft.ApplyMsg) {
	if !msg.CommandValid {
		return
	}

	idx := msg.CommandIndex
	op, _ := msg.Command.(Op)

	defer DPrintf("Server {%d} Apply (%v) (%v) (%v)", kv.me, op.Uid, op.Typ, msg.CommandIndex)

	if !kv.excutedOp[op.Uid] {
		switch op.Typ {
		case "Put":
			kv.kvStore[op.Key] = op.Val
		case "Append":
			kv.kvStore[op.Key] += op.Val
		}
		kv.uidList = append(kv.uidList, op.Uid)
		kv.excutedOp[op.Uid] = true
	}

	for ch, info := range kv.subscribe {
		if info.idx != idx {
			continue
		}
		delete(kv.subscribe, ch)

		if info.uid != op.Uid {
			ch <- replyMsg{err: ErrWrongLeader}
			return
		}

		if info.typ == "Get" {
			ch <- replyMsg{err: OK, val: kv.kvStore[op.Key]}
		} else {
			ch <- replyMsg{err: OK}
		}
		return
	}
}
