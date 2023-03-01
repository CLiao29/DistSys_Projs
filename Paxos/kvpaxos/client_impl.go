package kvpaxos

import (
	"sync"

	"proj3/common"
)

// any additions to Clerk state
type ClerkImpl struct {
	mu sync.Mutex
}

// initialize ck.impl state
func (ck *Clerk) InitImpl() {
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	ck.impl.mu.Lock()
	defer ck.impl.mu.Unlock()
	id := common.Nrand()
	for {
		for _, server := range ck.servers {
			args := &GetArgs{Key: key, Impl: GetArgsImpl{ID: id}}
			reply := &GetReply{}
			ok := common.Call(server, "KVPaxos.Get", args, reply)
			if ok {
				return reply.Value
			}
		}
	}
}

// shared by Put and Append; op is either "Put" or "Append"
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.impl.mu.Lock()
	defer ck.impl.mu.Unlock()
	id := common.Nrand()
	for {
		for _, server := range ck.servers {
			reply := &PutAppendReply{}
			args := &PutAppendArgs{Key: key, Value: value, Op: op, Impl: PutAppendArgsImpl{ID: id}}
			ok := common.Call(server, "KVPaxos.PutAppend", args, reply)
			if ok && reply.Err == OK {
				return
			}
		}
	}
}
