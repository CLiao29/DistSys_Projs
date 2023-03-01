package shardkv

import (
	"log"
	"time"

	"proj5/common"
)

// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters
type Op struct {
	OPtype    common.OPtype
	RequestID int64
	// db operations
	ClientID int64
	Key      string
	Value    string
	//shard reconfig operations
	ShardNum int
	Shardmap map[string]string
	Requests map[int64][]int64
}

// Method used by PaxosRSM to determine if two Op values are identical
func equals(v1 interface{}, v2 interface{}) bool {
	v1op := v1.(Op)
	v2op := v2.(Op)

	if v1op.OPtype != v2op.OPtype || v1op.ClientID != v2op.ClientID || v1op.RequestID != v2op.RequestID {
		return false
	}

	if v1op.OPtype == common.UPDATE {
		return true
	}

	if v1op.OPtype == common.SHARDRM {
		return v1op.ShardNum == v2op.ShardNum
	}

	if v1op.OPtype == common.ShardADD {
		if v1op.ShardNum != v2op.ShardNum {
			return false
		}
		for k, _ := range v1op.Shardmap {
			if v1op.Shardmap[k] != v2op.Shardmap[k] {
				return false
			}
		}
		for ckid, _ := range v1op.Requests {
			if _, ok := v2op.Requests[ckid]; !ok {
				return false
			}
			if len(v1op.Requests[ckid]) != len(v2op.Requests[ckid]) {
				return false
			}
		}
		return true
	}
	//get put append
	if v1op.OPtype == common.PUT || v1op.OPtype == common.APPEND {
		return v1op.Key == v2op.Key && v1op.Value == v2op.Value
	}
	if v1op.OPtype == common.GET {
		return v1op.Key == v2op.Key
	}
	return false
}

// additions to ShardKV state
type ShardKVImpl struct {
	//
	myID          int64
	currRequestID int64
	//
	appliedRequest map[int64][]int64
	requestGC      map[int64]int64
	database       map[string]string
	// shard that my group is responsible for
	shards []int
	//
	mapToRemove map[string]string
}

// initialize kv.impl.*
func (kv *ShardKV) InitImpl() {
	kv.impl.myID = common.Nrand()
	kv.impl.currRequestID = 0
	kv.impl.shards = make([]int, 0)
	kv.impl.appliedRequest = make(map[int64][]int64)
	kv.impl.requestGC = make(map[int64]int64)
	kv.impl.database = make(map[string]string)
}

// RPC handler for client Get requests
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.keyInMyShard(args.Key) {
		reply.Err = ErrWrongGroup
		updateop := Op{OPtype: common.UPDATE, RequestID: common.Nrand()}
		kv.rsm.AddOp(updateop)
		return nil
	}
	op := Op{OPtype: common.GET, Key: args.Key, ClientID: args.Impl.ClientID, RequestID: args.Impl.RequestID}
	kv.rsm.AddOp(op)
	val, ok := kv.impl.database[args.Key]
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	reply.Value = val
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.keyInMyShard(args.Key) {
		reply.Err = ErrWrongGroup
		updateop := Op{OPtype: common.UPDATE, RequestID: common.Nrand()}
		kv.rsm.AddOp(updateop)
		return nil
	}
	if args.Op != common.APPEND && args.Op != common.PUT {
		log.Fatalf("PutAppend: wrong op type\n")
	}
	if kv.isRequestApplied(args.Impl.ClientID, args.Impl.RequestID) {
		reply.Err = OK
		return nil
	}
	op := Op{OPtype: common.OPtype(args.Op), Key: args.Key, Value: args.Value, ClientID: args.Impl.ClientID, RequestID: args.Impl.RequestID}
	kv.rsm.AddOp(op)
	reply.Err = OK
	return nil
}

// Execute operation encoded in decided value v and update local state
func (kv *ShardKV) ApplyOp(v interface{}) {
	vop := v.(Op)
	if vop.OPtype == common.UPDATE {
		return
	}
	//for reconfig
	if vop.OPtype == common.SHARDRM {
		//remove shards that I am responsible for
		var index int
		for i, v := range kv.impl.shards {
			if v == vop.ShardNum {
				index = i
				break
			}
		}
		kv.impl.shards = append(kv.impl.shards[:index], kv.impl.shards[index+1:]...)
		// map to hand off
		kv.impl.mapToRemove = make(map[string]string)
		for k, _ := range kv.impl.database {
			if common.Key2Shard(k) == vop.ShardNum {
				kv.impl.mapToRemove[k] = kv.impl.database[k]
				delete(kv.impl.database, k)
			}
		}
	} else if vop.OPtype == common.ShardADD {
		// add the shards
		appendFlag := true
		for _, val := range kv.impl.shards {
			if val == vop.ShardNum {
				appendFlag = false
				break
			}
		}

		if appendFlag {
			kv.impl.shards = append(kv.impl.shards, vop.ShardNum)
		}
		// add the map
		for k, v := range vop.Shardmap {
			kv.impl.database[k] = v
		}
		// add new requests,
		for c, _ := range vop.Requests {
			//new client
			if _, ok := kv.impl.appliedRequest[c]; !ok {
				kv.impl.appliedRequest[c] = make([]int64, 0)
			}
			for _, v := range vop.Requests[c] {
				flag := true
				for i, _ := range kv.impl.appliedRequest[c] {
					if v == kv.impl.appliedRequest[c][i] {
						flag = false
						break
					}
				}
				if flag {
					kv.impl.appliedRequest[c] = append(kv.impl.appliedRequest[c], v)
				}
			}
		}
	}
	//put append
	if vop.OPtype == common.PUT {
		kv.impl.database[vop.Key] = vop.Value
	}
	if vop.OPtype == common.APPEND {
		kv.impl.database[vop.Key] += vop.Value
	}

	//requests
	if _, ok := kv.impl.appliedRequest[vop.ClientID]; !ok {
		kv.impl.appliedRequest[vop.ClientID] = make([]int64, 0)
	}
	if vop.RequestID > kv.impl.requestGC[vop.ClientID] {
		kv.impl.appliedRequest[vop.ClientID] = append(kv.impl.appliedRequest[vop.ClientID], vop.RequestID)
	}
	kv.gcAppliedRequest(vop.ClientID, vop.RequestID)
}

func (kv *ShardKV) keyInMyShard(key string) bool {
	s := common.Key2Shard(key)
	for _, shard := range kv.impl.shards {
		if shard == s {
			return true
		}
	}
	return false
}

func (kv *ShardKV) isRequestApplied(clientid int64, requestid int64) bool {
	pRequests, ok := kv.impl.appliedRequest[clientid]
	if !ok {
		return false
	} else {
		for _, request := range pRequests {
			if request == requestid {
				return true
			}
		}
		return false
	}
}

func (kv *ShardKV) gcAppliedRequest(clientid int64, requestid int64) {
	pRequests, ok := kv.impl.appliedRequest[clientid]
	if !ok {
		return
	} else {
		afterGC := make([]int64, 0)
		for _, request := range pRequests {
			if request >= requestid {
				afterGC = append(afterGC, request)
			}
		}
		kv.impl.appliedRequest[clientid] = afterGC
		kv.impl.requestGC[clientid] = requestid
	}
}

//
// Add RPC handlers for any other RPCs you introduce
//

func (kv *ShardKV) HandoffShards(args *common.HandoffShardsArgs, reply *common.HandoffShardsReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.isdead() {
		reply.Err = ErrWrongGroup
		return nil
	}
	if kv.isRequestApplied(args.ClientID, args.RequestID) {
		reply.Err = OK
		return nil
	}
	kv.impl.currRequestID++
	// go to paxos
	op := Op{OPtype: common.SHARDRM, ShardNum: args.ShardNum, ClientID: args.ClientID, RequestID: args.RequestID}
	kv.rsm.AddOp(op)

	//ask any server in another group to receive
	if len(args.ToServers) > 0 {
		shardmap := make(map[string]string)
		requests := make(map[int64][]int64)
		for k, v := range kv.impl.mapToRemove {
			shardmap[k] = v
		}
		for k, v := range kv.impl.appliedRequest {
			requests[k] = make([]int64, 0)
			for index, _ := range v {
				requests[k] = append(requests[k], v[index])
			}
		}

		receiveArgs := &common.ReceiveShardsArgs{ShardNum: args.ShardNum, Shardmap: shardmap, Requests: requests, ClientID: kv.impl.myID, RequestID: kv.impl.currRequestID}
		for {

			for _, server := range args.ToServers {
				receiveReply := &common.ReceiveShardsReply{}
				ok := common.Call(server, "ShardKV.ReceiveShards", receiveArgs, receiveReply)
				if ok && receiveReply.Err == OK {
					kv.impl.mapToRemove = make(map[string]string)
					reply.Err = OK
					return nil
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	kv.impl.mapToRemove = make(map[string]string)
	reply.Err = OK
	return nil
}

func (kv *ShardKV) ReceiveShards(args *common.ReceiveShardsArgs, reply *common.ReceiveShardsReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.isdead() {
		reply.Err = ErrWrongGroup
		return nil
	}
	if kv.isRequestApplied(args.ClientID, args.RequestID) {
		reply.Err = OK
		return nil
	}
	//go to paxos
	op := Op{OPtype: common.ShardADD, ShardNum: args.ShardNum, Shardmap: args.Shardmap, Requests: args.Requests, ClientID: args.ClientID, RequestID: args.RequestID}
	kv.rsm.AddOp(op)
	reply.Err = OK
	return nil
}
