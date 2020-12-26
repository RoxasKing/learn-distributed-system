package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"github.com/RoxasKing/learn-distributed-system/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	mu       sync.Mutex
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
	ck.leaderId = -1
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	args := &GetArgs{
		Key: key,
		Uid: nrand(),
	}

	serverId := 0

	ck.mu.Lock()
	if ck.leaderId != -1 {
		serverId = ck.leaderId
	}
	ck.mu.Unlock()

	for {
		DPrintf("[Clerk] Calling  (%v) (%v)\n", args.Uid, "Get")
		reply := &GetReply{}
		if ok := ck.servers[serverId].Call("KVServer.Get", args, reply); ok && reply.Err != ErrWrongLeader {

			ck.mu.Lock()
			ck.leaderId = serverId // remember the current leaderId
			ck.mu.Unlock()

			DPrintf("[Clerk] Finished (%v) (%v)\n", args.Uid, "Get")
			return reply.Value
		}

		serverId = (serverId + 1) % len(ck.servers)
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Uid:   nrand(),
	}

	serverId := 0

	ck.mu.Lock()
	if ck.leaderId != -1 {
		serverId = ck.leaderId
	}
	ck.mu.Unlock()

	for {
		DPrintf("[Clerk] Calling  (%v) (%v)\n", args.Uid, op)
		reply := &PutAppendReply{}
		if ok := ck.servers[serverId].Call("KVServer.PutAppend", args, reply); ok && reply.Err == OK {

			ck.mu.Lock()
			ck.leaderId = serverId // remember the current leaderId
			ck.mu.Unlock()

			DPrintf("[Clerk] Finished (%v) (%v)\n", args.Uid, op)
			return
		}

		serverId = (serverId + 1) % len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
