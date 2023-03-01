package pbservice

import (
	"time"

	"proj2/viewservice"
)

const UNASSIGNED string = ""

// additions to Clerk state.
type ClerkImpl struct {
	primary string
}

// your ck.impl.* initializations here.
func (ck *Clerk) initImpl() {
	ck.impl.primary = UNASSIGNED
}

// fetch a key's value from the current primary;
// if the key has never been set, return "".
// Get() must keep trying until either the
// primary replies with the value or the primary
// says the key doesn't exist, i.e., has never been Put().
func (ck *Clerk) Get(key string) string {
	for {
		ck.UpdatePrimaryIfNeeded()
		//log.Printf("key %v", key)
		getArg, getReply := GetArgs{key, GetArgsImpl{}}, GetReply{}
		ok := call(ck.impl.primary, "PBServer.Get", &getArg, &getReply)
		//log.Printf("ok %v, reply %v", ok, getReply)
		if !ok || getReply.Err == ErrWrongServer {
			ck.impl.primary = UNASSIGNED
			continue
		}
		if getReply.Err == ErrNoKey {
			return ""
		}
		return getReply.Value
	}
}

// send a Put() or Append() RPC
// must keep trying until it succeeds.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	id := nrand()
	for {
		ck.UpdatePrimaryIfNeeded()
		putAppendArg, putAppendReply := PutAppendArgs{key, value, PutAppendArgsImpl{op, id}}, PutAppendReply{}
		ok := call(ck.impl.primary, "PBServer.PutAppend", &putAppendArg, &putAppendReply)
		if ok && putAppendReply.Err == OK {
			break
		}
		ck.impl.primary = UNASSIGNED
		time.Sleep(viewservice.PingInterval)
	}
}

// Update primary from vs
func (ck *Clerk) UpdatePrimaryIfNeeded() {
	if ck.impl.primary != UNASSIGNED {
		return
	}
	view, ok := ck.vs.Get()
	for !ok {
		view, ok = ck.vs.Get()
		time.Sleep(viewservice.PingInterval)
	}
	ck.impl.primary = view.Primary
}
