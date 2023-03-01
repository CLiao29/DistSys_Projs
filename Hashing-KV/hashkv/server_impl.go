package hashkv

import (
	"proj4/common"
)

// additions to HashKV state
type HashKVImpl struct {
	database       map[string]string
	appliedRequest map[int64]int
	// temp value for unreliable networks (resend db to new succ)
	tempDB map[string]map[string]string
}

// initialize kv.impl.*
func (kv *HashKV) InitImpl() {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.impl.database = make(map[string]string)
	kv.impl.appliedRequest = make(map[int64]int)
	kv.impl.tempDB = make(map[string]map[string]string)

	if kv.predID == kv.ID && kv.succID == kv.ID {
		return
	}
	//Update succ's pred, ask for the shards.
	UpdatePredArgs := &ServerUpdateArgs{
		UpdateType: UpdatePred,
		MyServer:   kv.me,
		MyServerID: kv.ID,
		MyPrev:     kv.pred,
		MyPrevID:   kv.predID,
	}

	//update pred's succ
	UpdateSuccArgs := &ServerUpdateArgs{
		UpdateType: UpdateSucc,
		MyServer:   kv.me,
		MyServerID: kv.ID,
	}
	ok1, ok2 := false, false
	for {
		for !ok1 {
			UpdatePredReply := &ServerUpdateReply{}
			ok1 = common.Call(kv.succ, "HashKV.ServerUpdate", UpdatePredArgs, UpdatePredReply)
			if ok1 && UpdatePredReply.Err == OK {
				db := UpdatePredReply.Database
				kv.editDatabase(db, kv.ID, kv.predID, true, true)
				for k, v := range UpdatePredReply.DoneRequest {
					kv.impl.appliedRequest[k] = v
				}
				arg := &AckDbArgs{MyServer: kv.me, MyServerID: kv.ID}
				reply := &AckDbReply{}

				okack := common.Call(kv.succ, "HashKV.AckDb", arg, reply)

				for !okack && reply.Err != OK {
					reply := &AckDbReply{}
					okack = common.Call(kv.succ, "HashKV.AckDb", arg, reply)
				}
			}
		}

		for !ok2 {
			UpdateSuccReply := &ServerUpdateReply{}
			ok2 = common.Call(kv.pred, "HashKV.ServerUpdate", UpdateSuccArgs, UpdateSuccReply)
		}
		if ok1 && ok2 {
			break
		}
	}
}

// hand off shards before shutting down
func (kv *HashKV) KillImpl() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//Update Succ's pred
	PredArgs := &HandOffArgs{
		UpdateType:  UpdatePred,
		Database:    kv.impl.database,
		MyServer:    kv.me,
		MyServerID:  kv.ID,
		NewServer:   kv.pred,
		NewServerID: kv.predID,
		DoneRequest: kv.impl.appliedRequest,
	}
	//update pred's succ
	SuccArgs := &HandOffArgs{
		UpdateType:  UpdateSucc,
		MyServer:    kv.me,
		MyServerID:  kv.ID,
		NewServer:   kv.succ,
		NewServerID: kv.succID,
	}
	ok1, ok2 := false, false
	for {
		if !ok1 {
			PredReply := &HandOffReply{}
			ok1 = common.Call(kv.succ, "HashKV.HandOff", PredArgs, PredReply)
		}
		if !ok2 {
			SuccReply := &HandOffReply{}
			ok2 = common.Call(kv.pred, "HashKV.HandOff", SuccArgs, SuccReply)
		}
		if ok1 && ok2 {
			break
		}
	}
	kv.impl.tempDB = nil
	kv.impl.appliedRequest = nil
}

// RPC handler for client Get requests
func (kv *HashKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard_ok := isMyShard(args.Key, kv.ID, kv.predID)

	if !shard_ok || kv.isdead() || len(kv.impl.tempDB) != 0 {
		reply.Err = ErrWrongServer
		return nil
	}
	reply.Err = OK
	reply.Value = kv.impl.database[args.Key]
	return nil
}

// RPC handler for client Put and Append requests
func (kv *HashKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard_ok := isMyShard(args.Key, kv.ID, kv.predID)
	if !shard_ok || kv.isdead() || len(kv.impl.tempDB) != 0 {
		reply.Err = ErrWrongServer
		return nil
	}
	isApplied := kv.requestIsApplied(args.Impl.ID)
	if !isApplied {
		kv.applyOP(args.Op, args.Key, args.Value, args.Impl.ID)
	}

	reply.Err = OK
	return nil
}

func (kv *HashKV) requestIsApplied(ID int64) bool {
	return kv.impl.appliedRequest[ID] != 0
}

func isMyShard(key string, myID string, predID string) bool {
	hashK := common.Key2Shard(key)
	predShard := common.Key2Shard(predID)
	myShard := common.Key2Shard(myID)

	if predShard == myShard {
		return myID <= predID
	}

	if predShard > myShard {
		return !(hashK > myShard && hashK <= predShard)
	}
	return hashK > predShard && hashK <= myShard
}

func (kv *HashKV) editDatabase(db map[string]string, myID string, predID string, noCheck bool, Add bool) map[string]string {
	//no need to check when receiving handoffs
	// if add == false, then delete (for init)
	res := make(map[string]string)
	for k, v := range db {
		if noCheck || isMyShard(k, myID, predID) {
			if Add {
				kv.impl.database[k] = v
			}
			res[k] = v
			delete(db, k)
		}
	}
	return res
}

func (kv *HashKV) applyOP(op string, key string, value string, id int64) {

	if op == "Put" {
		kv.impl.database[key] = value
	} else if op == "Append" {
		kv.impl.database[key] += value
	}
	kv.impl.appliedRequest[id] = 1
}

// Add RPC handlers for any other RPCs you introduce
func (kv *HashKV) HandOff(args *HandOffArgs, reply *HandOffReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	updateType := args.UpdateType
	delete(kv.impl.tempDB, args.MyServerID)

	//I am new Pred's new Succ
	if updateType == UpdatePred {
		kv.pred = args.NewServer
		kv.predID = args.NewServerID
		kv.editDatabase(args.Database, kv.ID, kv.predID, true, true)
		for k, v := range args.DoneRequest {
			kv.impl.appliedRequest[k] = v
		}
	}

	//Update my new Succ, no other actions needed.
	if updateType == UpdateSucc {
		kv.succ = args.NewServer
		kv.succID = args.NewServerID
		reply.Err = OK
	}

	return nil
}

func (kv *HashKV) ServerUpdate(args *ServerUpdateArgs, reply *ServerUpdateReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	updateType := args.UpdateType

	//Update my new Pred
	if updateType == UpdatePred {
		kv.pred = args.MyServer
		kv.predID = args.MyServerID
		if _, ok := kv.impl.tempDB[args.MyServerID]; !ok {
			kv.impl.tempDB[args.MyServerID] = kv.editDatabase(kv.impl.database, args.MyServerID, args.MyPrevID, false, false)
		}
		reply.Database = kv.impl.tempDB[args.MyServerID]
		reply.DoneRequest = kv.impl.appliedRequest
		reply.Err = OK
	}

	//Update my new Succ, no other actions needed.
	if updateType == UpdateSucc {
		kv.succ = args.MyServer
		kv.succID = args.MyServerID
		reply.Err = OK
	}
	return nil
}

func (kv *HashKV) AckDb(args *AckDbArgs, reply *AckDbReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.impl.tempDB, args.MyServerID)

	reply.Err = OK
	return nil
}
