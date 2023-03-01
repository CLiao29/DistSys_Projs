package pbservice

import "proj2/viewservice"

// In all data types that represent arguments to RPCs, field names
// must start with capital letters, otherwise RPC will break.

// additional state to include in arguments to PutAppend RPC.
type PutAppendArgsImpl struct {
	Op string
	Id int64
}

// additional state to include in arguments to Get RPC.
type GetArgsImpl struct {
}

//
// for new RPCs that you add, declare types for arguments and reply.
//

type SyncDBArg struct {
	View            viewservice.View
	Database        map[string]string
	AppliedRequests map[int64]bool
}

type SyncReply struct {
	Ok bool
}

type SyncGetArg struct {
	View viewservice.View
}

type SyncGetReply struct {
	Ok bool
}

type SyncPutAppendArg struct {
	View viewservice.View
	Args PutAppendArgs
}

type SyncPutAppendReply struct {
	Ok bool
}
