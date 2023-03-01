package paxos

import "proj5/common"

// additions to Paxos state.
type PaxosImpl struct {
	peerDone    []int
	instances   map[int]*Instance
	maxSeen_N   int
	maxSeen_Seq int
}

// your px.impl.* initializations here.
func (px *Paxos) initImpl() {
	px.impl.instances = make(map[int]*Instance)
	px.impl.maxSeen_N = 0
	px.impl.maxSeen_Seq = 0
	// 	A peer's z_i is -1 if it has never called Done().
	px.impl.peerDone = make([]int, len(px.peers))
	for p, _ := range px.peers {
		px.impl.peerDone[p] = -1
	}
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	go func() {
		status, _ := px.Status(seq)

		if seq > px.impl.maxSeen_Seq {
			px.impl.maxSeen_Seq = seq
		}
		for status != Decided {
			if status == Forgotten {
				return
			}
			prepCount, accCount, rejCount := 0, 0, 0
			curr_na := -1
			curr_va := v
			newN := px.getN()

			/*Phase 1: Prepare*/
			for i, peer := range px.peers {
				if px.isdead() {
					return
				}
				args := &PrepareArgs{Seq: seq, N: newN}
				reply := &PrepareReply{}
				if px.me == i {
					px.Prepare(args, reply)
				} else {
					common.Call(peer, "Paxos.Prepare", args, reply)
				}

				if reply.Res == OK {
					prepCount++
					if reply.AccProp.N > curr_na {
						curr_na = reply.AccProp.N
						curr_va = reply.AccProp.V
					}

				} else {
					rejCount++
					if reply.ResN > px.impl.maxSeen_N {
						px.impl.maxSeen_N = reply.ResN
					}
				}
				if prepCount > len(px.peers)/2 || rejCount > len(px.peers)/2 {
					break
				}
			}

			/* Phase 2: Accept*/
			if prepCount > len(px.peers)/2 {
				rejCount = 0
				for i, peer := range px.peers {
					if px.isdead() {
						return
					}
					args := &AcceptArgs{Prop: &Proposal{N: newN, V: curr_va}, Seq: seq}
					reply := &AcceptReply{}
					if px.me == i {
						px.Accept(args, reply)
					} else {
						common.Call(peer, "Paxos.Accept", args, reply)
					}

					if reply.Res == OK {
						accCount++
					} else {
						rejCount++
						if reply.ResN > px.impl.maxSeen_N {
							px.impl.maxSeen_N = reply.ResN
						}
					}
					if accCount > len(px.peers)/2 || rejCount > len(px.peers)/2 {
						break
					}
				}
			}

			/*	Phase 3 Decide & Learn */
			if accCount > len(px.peers)/2 {
				for i, peer := range px.peers {
					if px.isdead() {
						return
					}
					args := &DecidedArgs{Seq: seq, Prop: &Proposal{N: newN, V: curr_va}, Peer: px.me, DoneSeq: px.impl.peerDone[px.me]}
					reply := &DecidedReply{}
					if px.me == i {
						px.Learn(args, reply)
					} else {
						common.Call(peer, "Paxos.Learn", args, reply)
					}
				}
			}
			status, _ = px.Status(seq)
		}
	}()
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq >= px.impl.peerDone[px.me] {
		px.impl.peerDone[px.me] = seq
	}
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	return px.impl.maxSeen_Seq
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peer's Min does not reflect another peer's Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers' Min()s will not increase
// even if all reachable peers call Done(). The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances. ok
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	min_z := px.impl.peerDone[px.me]
	for p, _ := range px.peers {
		if min_z == -1 {
			break
		}
		if px.impl.peerDone[p] < min_z {
			min_z = px.impl.peerDone[p]
		}
	}
	px.forget(min_z)

	return min_z + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so, what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {

	if seq < px.Min() {
		return Forgotten, nil
	}

	px.mu.Lock()
	defer px.mu.Unlock()
	if val, ok := px.impl.instances[seq]; ok {
		return val.Status, val.V_a
	}

	return Pending, nil
}

func (px *Paxos) getN() int {
	x := px.me + 1
	for x <= px.impl.maxSeen_N {
		x += 3
	}
	return x
}

func (px *Paxos) getInstance(seq int) *Instance {
	if val, ok := px.impl.instances[seq]; ok {
		return val
	} else {
		px.impl.instances[seq] = &Instance{Seq: seq, N_p: -1, N_a: -1, V_a: nil, Status: Pending}
		return px.impl.instances[seq]
	}
}

func (px *Paxos) forget(seq int) {
	for k, v := range px.impl.instances {
		if k <= seq && v.Status == Decided {
			delete(px.impl.instances, k)
		}
	}
}
