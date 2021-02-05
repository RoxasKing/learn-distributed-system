package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"github.com/RoxasKing/learn-distributed-system/labrpc"
	sm "github.com/RoxasKing/learn-distributed-system/shardmaster"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *sm.Clerk
	config   sm.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = sm.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.Uid = nrand()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			// RETRY:
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				args.Num = ck.config.Num
				var reply GetReply
				DPrintf("[Clerk] [Request] num(%v) uid(%v) typ(%v) key(%v) val(%v)\n",
					ck.config.Num, args.Uid, "Get", args.Key, "")
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader

				// if ok && reply.Err == ErrLowVer {
				// 	break
				// }

				// group's config is out of date, need to wait update
				// if ok && reply.Err == ErrHighVer {
				// 	time.Sleep(50 * time.Millisecond)
				// 	goto RETRY
				// }

				// true group, but group is not ready yet, need to wait new shards installed
				// if ok && reply.Err == ErrNotReadyYet {
				// 	time.Sleep(30 * time.Millisecond)
				// 	goto RETRY
				// }
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.Uid = nrand()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// RETRY:
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				args.Num = ck.config.Num
				var reply PutAppendReply
				DPrintf("[Clerk] [Request] num(%v) uid(%v) typ(%v) key(%v) val(%v)\n",
					ck.config.Num, args.Uid, args.Op, args.Key, args.Value)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader

				// if ok && reply.Err == ErrLowVer {
				// 	break
				// }

				// group's config is out of date, need to wait update
				// if ok && reply.Err == ErrHighVer {
				// 	time.Sleep(50 * time.Millisecond)
				// 	goto RETRY
				// }

				// true group, but group is not ready yet, need to wait new shards installed
				// if ok && reply.Err == ErrNotReadyYet {
				// 	time.Sleep(30 * time.Millisecond)
				// 	goto RETRY
				// }
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
