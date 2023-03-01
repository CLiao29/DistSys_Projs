package common

//
// define here any data types that you need to access in two packages without
// creating circular dependencies
//

const (
	JOIN     = "Join"
	LEAVE    = "Leave"
	MOVE     = "Move"
	QUERY    = "Query"
	GET      = "Get"
	PUT      = "Put"
	APPEND   = "Append"
	SHARDRM  = "ShardRemove"
	ShardADD = "ShardAdd"
	UPDATE   = "Update"
)

type OPtype string

type HandoffShardsArgs struct {
	ShardNum  int
	ToServers []string
	ClientID  int64
	RequestID int64
}

type HandoffShardsReply struct {
	Err string
}

type ReceiveShardsArgs struct {
	ShardNum  int
	Shardmap  map[string]string
	Requests  map[int64][]int64
	ClientID  int64
	RequestID int64
}

type ReceiveShardsReply struct {
	Err string
}
