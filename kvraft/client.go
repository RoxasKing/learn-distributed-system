package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"github.com/RoxasKing/learn-distributed-system/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
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
	ck.leaderId = 0
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

	for {
		DPrintf("[Clerk] Calling  (%v) (%v) (%v)\n", args.Uid, "Get", key)
		reply := &GetReply{}
		if ok := ck.servers[ck.leaderId].Call("KVServer.Get", args, reply); ok && reply.Err != ErrWrongLeader {
			DPrintf("[Clerk] Finished (%v) (%v) (%v)\n", args.Uid, "Get", key)
			return reply.Value
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(12 * time.Millisecond)
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

	for {
		DPrintf("[Clerk] Calling  (%v) (%v) (%v) (%v)\n", args.Uid, op, key, value)
		reply := &PutAppendReply{}
		if ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", args, reply); ok && reply.Err == OK {
			DPrintf("[Clerk] Finished (%v) (%v) (%v) (%v)\n", args.Uid, op, key, value)
			return
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(12 * time.Millisecond)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
