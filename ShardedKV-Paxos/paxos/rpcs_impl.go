package paxos

// In all data types that represent RPC arguments/reply, field names
// must start with capital letters, otherwise RPC will break.

const (
	OK     = "OK"
	Reject = "Reject"
)

type Response string

type Proposal struct {
	N int
	V interface{}
}

type Instance struct {
	Seq    int
	N_p    int
	N_a    int
	V_a    interface{}
	Status Fate
}

type PrepareArgs struct {
	Seq int
	N   int
}

type PrepareReply struct {
	Res     Response
	ResN    int
	AccProp *Proposal // (n_a, v_a)
}

type AcceptArgs struct {
	Prop *Proposal
	Seq  int
}

type AcceptReply struct {
	Res  Response
	ResN int
}

type DecidedArgs struct {
	Seq     int
	Prop    *Proposal
	Peer    int
	DoneSeq int
}

type DecidedReply struct {
	Res Response
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	ins := px.getInstance(args.Seq)

	if args.N > ins.N_p {
		ins.N_p = args.N
		reply.Res = OK
		reply.ResN = args.N
		reply.AccProp = &Proposal{N: ins.N_a, V: ins.V_a}
	} else {
		reply.Res = Reject
		reply.ResN = ins.N_p
	}
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	ins := px.getInstance(args.Seq)
	prop := args.Prop

	if prop.N >= ins.N_p {
		ins.N_p = prop.N
		ins.N_a = prop.N
		ins.V_a = prop.V
		reply.Res = OK
	} else {
		reply.Res = Reject
		reply.ResN = ins.N_p
	}
	return nil
}

func (px *Paxos) Learn(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	ins := px.getInstance(args.Seq)

	ins.N_p = args.Prop.N
	ins.N_a = args.Prop.N
	ins.V_a = args.Prop.V
	ins.Status = Decided
	px.impl.peerDone[args.Peer] = args.DoneSeq
	if args.DoneSeq > px.impl.maxSeen_Seq {
		px.impl.maxSeen_Seq = args.DoneSeq
	}
	reply.Res = OK
	return nil
}

//
// add RPC handlers for any RPCs you introduce.
//