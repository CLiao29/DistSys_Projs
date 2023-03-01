package shardkv

// Field names must start with capital letters,
// otherwise RPC will break.

// additional state to include in arguments to PutAppend RPC.
type PutAppendArgsImpl struct {
	ClientID  int64
	RequestID int64
}

// additional state to include in arguments to Get RPC.
type GetArgsImpl struct {
	ClientID  int64
	RequestID int64
}

// for new RPCs that you add, declare types for arguments and reply


