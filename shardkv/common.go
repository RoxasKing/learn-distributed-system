package shardkv

import sm "github.com/RoxasKing/learn-distributed-system/shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= sm.NShards
	return shard
}

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongGroup  Err = "ErrWrongGroup"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrNotReadyYet Err = "ErrNotReadyYet"
	ErrHighVer     Err = "ErrHighVer"
	ErrLowVer      Err = "ErrLowVer"
	ErrAbortReq    Err = "ErrAbortReq"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Uid int64 // avoid duplicate requests
	Num int   // clerck's config number, server's config number must equal to it
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Uid int64 // avoid duplicate requests
	Num int   // clerck's config number, server's config number must equal to it
}

type GetReply struct {
	Err   Err
	Value string
}
