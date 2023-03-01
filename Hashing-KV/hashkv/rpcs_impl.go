package hashkv

// Field names must start with capital letters,
// otherwise RPC will break.

// additional state to include in arguments to PutAppend RPC.
type PutAppendArgsImpl struct {
	ID int64
}

// additional state to include in arguments to Get RPC.
type GetArgsImpl struct {
	ID int64
}

//
// for new RPCs that you add, declare types for arguments and reply
//

type UpdateType string

const (
	UpdatePred = "UpdatePred"
	UpdateSucc = "UpdateSucc"
)

type HandOffArgs struct {
	UpdateType  UpdateType
	Database    map[string]string
	MyServer    string
	MyServerID  string
	NewServer   string
	NewServerID string
	DoneRequest map[int64]int
}

type HandOffReply struct {
	Err Err
}

type ServerUpdateArgs struct {
	UpdateType UpdateType
	MyServer   string
	MyServerID string
	MyPrev     string
	MyPrevID   string
}

type ServerUpdateReply struct {
	Database map[string]string
	DoneRequest map[int64]int
	Err      Err
}

type AckDbArgs struct {
	MyServer   string
	MyServerID string
}

type AckDbReply struct {
	Err Err
}
