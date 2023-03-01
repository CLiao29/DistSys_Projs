package hashkv

import (
	"sync"
	"time"

	"proj4/common"
)

// additions to Clerk state
type ClerkImpl struct {
	mu sync.Mutex
}

// initialize ck.impl.*
func (ck *Clerk) InitImpl() {
}

// fetch the current value for a key.
// return "" if the key does not exist.
// keep retrying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	ck.impl.mu.Lock()
	defer ck.impl.mu.Unlock()
	requestId := common.Nrand()
	args := &GetArgs{Key: key, Impl: GetArgsImpl{ID: requestId}}
	for {
		//fmt.Printf("I am client, my servers are  %v \n", ck.servers)
		for _, server := range ck.servers {
			reply := &GetReply{}
			ok := common.Call(server, "HashKV.Get", args, reply)
			if reply.Err == ErrWrongServer {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if ok {
				return reply.Value
			}
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// send a Put or Append request.
// keep retrying forever until success.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.impl.mu.Lock()
	defer ck.impl.mu.Unlock()
	requestId := common.Nrand()
	args := &PutAppendArgs{Key: key, Value: value, Op: op, Impl: PutAppendArgsImpl{ID: requestId}}
	for {
		//fmt.Printf("I am client, my servers are  %v \n", ck.servers)
		for _, server := range ck.servers {
			reply := &GetReply{}
			ok := common.Call(server, "HashKV.PutAppend", args, reply)
			if reply.Err == ErrWrongServer {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if ok && reply.Err == OK {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
