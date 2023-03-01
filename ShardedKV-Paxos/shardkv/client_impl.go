package shardkv

import (
	"time"

	"proj5/common"
	"proj5/shardmaster"
)

// additions to Clerk state
type ClerkImpl struct {
	config    shardmaster.Config
	clientID  int64
	RequestID int64
}

// initialize ck.impl.*
func (ck *Clerk) InitImpl() {
	ck.impl.clientID = common.Nrand()
	ck.impl.RequestID = 491
}

// fetch the current value for a key.
// return "" if the key does not exist.
// keep retrying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	requestId := ck.impl.RequestID
	ck.impl.RequestID += 1
	args := &GetArgs{Key: key, Impl: GetArgsImpl{ClientID: ck.impl.clientID, RequestID: requestId}}
	for {
		// fmt.Printf("getting %v \n", key)
		ck.impl.config = ck.sm.Query(-1)
		groupID := ck.impl.config.Shards[common.Key2Shard(key)]
		servers, ok := ck.impl.config.Groups[groupID]
		if ok {
			for _, server := range servers {
				reply := &GetReply{}
				ok2 := common.Call(server, "ShardKV.Get", args, reply)
				if reply.Err == ErrWrongGroup {
					time.Sleep(100 * time.Millisecond)
					break
				}
				if ok2 {
					return reply.Value
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// send a Put or Append request.
// keep retrying forever until success.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	requestId := ck.impl.RequestID
	ck.impl.RequestID += 1
	args := &PutAppendArgs{Key: key, Value: value, Op: op, Impl: PutAppendArgsImpl{ClientID: ck.impl.clientID, RequestID: requestId}}
	for {
		ck.impl.config = ck.sm.Query(-1)
		groupID := ck.impl.config.Shards[common.Key2Shard(key)]
		servers, ok := ck.impl.config.Groups[groupID]
		if ok {
			for _, server := range servers {
				reply := &PutAppendReply{}
				ok2 := common.Call(server, "ShardKV.PutAppend", args, reply)
				if reply.Err == ErrWrongGroup {
					//fmt.Printf("getting wrong group")
					time.Sleep(100 * time.Millisecond)
					break
				}
				if ok2 && reply.Err == OK {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
