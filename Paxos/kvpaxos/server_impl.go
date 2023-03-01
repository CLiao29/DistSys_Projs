package kvpaxos

// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters,
// otherwise RPC will break.
type Op struct {
	OP string
	ID int64
	K  string
	V  string
}

// additions to KVPaxos state
type KVPaxosImpl struct {
	appliedRequest map[int64]bool
	database       map[string]string
}

// initialize kv.impl.*
func (kv *KVPaxos) InitImpl() {
	kv.impl.appliedRequest = make(map[int64]bool)
	kv.impl.database = make(map[string]string)
}

// Handler for Get RPCs
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.isApplied(args.Impl.ID) == false {
		get_op := Op{OP: "Get", K: args.Key, ID: args.Impl.ID}
		kv.rsm.AddOp(get_op)
	}

	val, ok := kv.impl.database[args.Key]
	if !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
	}
	reply.Value = val
	return nil
}

// Handler for Put and Append RPCs
func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.isApplied(args.Impl.ID) {
		reply.Err = OK
		return nil
	}

	pa_op := Op{OP: args.Op, K: args.Key, V: args.Value, ID: args.Impl.ID}
	kv.rsm.AddOp(pa_op)
	reply.Err = OK
	return nil
}

// Execute operation encoded in decided value v and update local state
func (kv *KVPaxos) ApplyOp(v interface{}) {
	op := v.(Op)
	key, value, op_name, id := op.K, op.V, op.OP, op.ID
	if op_name == "Put" {
		kv.impl.database[key] = value
	} else if op_name == "Append" {
		kv.impl.database[key] += value
	}
	kv.impl.appliedRequest[id] = true
}

func (kv *KVPaxos) isApplied(id int64) bool {
	_, ok := kv.impl.appliedRequest[id]
	return ok
}
