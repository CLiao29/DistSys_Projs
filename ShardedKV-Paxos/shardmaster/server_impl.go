package shardmaster

import (
	"log"
	"sort"

	"proj5/common"
)

// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters.
type Op struct {
	OP      common.OPtype
	Num     int
	GID     int64
	Servers []string
	Shard   int
	//distinguish multiple requests with same arguments
	requestID int64
}

// Method used by PaxosRSM to determine if two Op values are identical
func equals(v1 interface{}, v2 interface{}) bool {
	v1op := v1.(Op)
	v2op := v2.(Op)
	if v1op.OP != v2op.OP {
		return false
	}
	if v1op.OP == common.JOIN {
		if v1op.GID != v2op.GID {
			return false
		}
		if v1op.requestID != v2op.requestID {
			return false
		}
		if len(v1op.Servers) != len(v2op.Servers) {
			return false
		}
		for i := range v1op.Servers {
			if v1op.Servers[i] != v2op.Servers[i] {
				return false
			}
		}
		return true
	}
	if v1op.OP == common.LEAVE {
		return v1op.GID == v2op.GID && v1op.requestID == v2op.requestID
	}
	if v1op.OP == common.MOVE {
		return v1op.Shard == v2op.Shard && v1op.GID == v2op.GID && v1op.requestID == v2op.requestID
	}
	if v1op.OP == common.QUERY {
		return v1op.Num == v2op.Num && v1op.requestID == v2op.requestID
	}

	return false
}

// additions to ShardMaster state
type ShardMasterImpl struct {
	smID          int64
	currRequestID int64
	removeServers map[int64][]string
}

// initialize sm.impl.*
func (sm *ShardMaster) InitImpl() {
	sm.impl.smID = common.Nrand()
	sm.impl.currRequestID = 0
	sm.impl.removeServers = make(map[int64][]string)
}

// RPC handlers for Join, Leave, Move, and Query RPCs
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	id := common.Nrand()
	op := Op{OP: common.JOIN, GID: args.GID, Servers: args.Servers, requestID: id}
	sm.rsm.AddOp(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	id := common.Nrand()
	op := Op{OP: common.LEAVE, GID: args.GID, requestID: id}
	sm.rsm.AddOp(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	id := common.Nrand()
	op := Op{OP: common.MOVE, GID: args.GID, Shard: args.Shard, requestID: id}
	sm.rsm.AddOp(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	id := common.Nrand()
	op := Op{OP: common.QUERY, Num: args.Num, requestID: id}
	sm.rsm.AddOp(op)
	//finish, now query

	if args.Num == -1 || args.Num >= len(sm.configs)-1 {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	return nil
}

// Execute operation encoded in decided value v and update local state
func (sm *ShardMaster) ApplyOp(v interface{}) {
	op := v.(Op)
	if op.OP == common.QUERY {
		//query, return
		return
	}

	newConfig := sm.newConfigFromIndex(len(sm.configs) - 1)
	if op.OP == common.JOIN {
		newConfig.Groups[op.GID] = make([]string, 0)
		newConfig.Groups[op.GID] = append(newConfig.Groups[op.GID], op.Servers...)
		sm.balanceJoin(&newConfig, op.GID)
	} else if op.OP == common.LEAVE {
		sm.impl.removeServers[op.GID] = make([]string, 0)
		sm.impl.removeServers[op.GID] = append(sm.impl.removeServers[op.GID], newConfig.Groups[op.GID]...)
		delete(newConfig.Groups, op.GID)
		sm.balanceLeave(&newConfig, op.GID)
		delete(sm.impl.removeServers, op.GID)
	} else if op.OP == common.MOVE {
		from := newConfig.Shards[op.Shard]
		newConfig.Shards[op.Shard] = op.GID
		sm.sendHandoffRPC(newConfig, op.Shard, from, op.GID, false)
	} else {
		log.Fatal("op type wrong")
	}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) newConfigFromIndex(n int) Config {
	fromConfig := sm.configs[n]

	toConfig := Config{}
	toConfig.Num = fromConfig.Num + 1

	for i := 0; i < common.NShards; i++ {
		toConfig.Shards[i] = fromConfig.Shards[i]
	}
	//new map, add
	toConfig.Groups = make(map[int64][]string)
	for k, v := range fromConfig.Groups {
		toConfig.Groups[k] = append(toConfig.Groups[k], v...)
	}
	return toConfig
}

func (sm *ShardMaster) balanceJoin(conf *Config, gid int64) {
	shardStat := make(map[int64]int)
	// get stats of shards , store keys to iterate in a specific order!!!!!!
	shardStatkeys := make([]int64, 0)
	shardStat[gid] = 0
	shardStatkeys = append(shardStatkeys, gid)
	for _, g := range conf.Shards {
		if shardStat[g] == 0 {
			shardStatkeys = append(shardStatkeys, g)
		}
		shardStat[g] += 1
	}
	for groupid, _ := range conf.Groups {
		for shardStat[groupid] < common.NShards/len(conf.Groups) {
			max := -1
			var max_gid int64
			for _, g := range shardStatkeys {
				if g == groupid {
					continue
				}
				if shardStat[g] > max {
					max_gid = g
					max = shardStat[g]
				}
			}
			for i, _ := range conf.Shards {
				if conf.Shards[i] == max_gid {
					conf.Shards[i] = groupid
					sm.sendHandoffRPC(*conf, i, max_gid, groupid, false)
					break
				}
			}
			shardStat[groupid] += 1
			shardStat[max_gid] -= 1
		}
	}
}

func (sm *ShardMaster) balanceLeave(conf *Config, gid int64) {
	shardStat := make(map[int64]int)
	shardStatkeys := make([]int64, 0)

	//get stats of shards
	to_be_moved := 0
	for groupid, _ := range conf.Groups {
		shardStat[groupid] = 0
		shardStatkeys = append(shardStatkeys, groupid)
	}
	sort.Slice(shardStatkeys, func(i, j int) bool { return shardStatkeys[i] < shardStatkeys[j] })
	for i, _ := range conf.Shards {
		if conf.Shards[i] == gid {
			to_be_moved += 1
		} else {
			shardStat[conf.Shards[i]] += 1
		}
	}
	count := 0
	for count < to_be_moved {
		min := common.NShards + 1
		var min_gid int64
		for _, g := range shardStatkeys {
			if shardStat[g] < min {
				min_gid = g
				min = shardStat[g]
			}
		}
		for i, _ := range conf.Shards {
			if conf.Shards[i] == gid {
				conf.Shards[i] = min_gid
				sm.sendHandoffRPC(*conf, i, gid, min_gid, true)
				break
			}
		}
		shardStat[min_gid] += 1
		count += 1
	}

	// balance base on multiple weird Moves.
	for groupid, _ := range conf.Groups {
		for shardStat[groupid] < common.NShards/len(conf.Groups) {
			max := -1
			var max_gid int64
			for _, g := range shardStatkeys {
				if g == groupid {
					continue
				}
				if shardStat[g] > max {
					max_gid = g
					max = shardStat[g]
				}
			}
			for i, _ := range conf.Shards {
				if conf.Shards[i] == max_gid {
					conf.Shards[i] = groupid

					sm.sendHandoffRPC(*conf, i, max_gid, groupid, false)
					break
				}
			}
			shardStat[groupid] += 1
			shardStat[max_gid] -= 1
		}
	}

}

// sendHandoffRPC from group a to group b
func (sm *ShardMaster) sendHandoffRPC(conf Config, shardNum int, from int64, to int64, remove_flag bool) {
	if from == to {
		// in case of boring move...
		return
	}
	sm.impl.currRequestID += 1
	toservers := make([]string, 0)
	fromservers := make([]string, 0)
	toservers = append(toservers, conf.Groups[to]...)
	if remove_flag {
		fromservers = append(fromservers, sm.impl.removeServers[from]...)
	} else {
		fromservers = append(fromservers, conf.Groups[from]...)
	}
	//init assignment
	if len(fromservers) <= 0 {
		args := &common.ReceiveShardsArgs{ShardNum: shardNum, Shardmap: make(map[string]string), Requests: make(map[int64][]int64), ClientID: sm.impl.smID, RequestID: sm.impl.currRequestID}
		for {
			for _, server := range toservers {
				reply := &common.ReceiveShardsReply{}
				ok := common.Call(server, "ShardKV.ReceiveShards", args, reply)
				if ok {
					return
				}
			}
		}
	}

	args := &common.HandoffShardsArgs{ShardNum: shardNum, ToServers: toservers, ClientID: sm.impl.smID, RequestID: sm.impl.currRequestID}
	for {
		for _, server := range fromservers {
			reply := &common.HandoffShardsReply{}
			ok := common.Call(server, "ShardKV.HandoffShards", args, reply)
			if ok {
				return
			}
		}
	}
}
