package paxosrsm

import (
	"time"

	"proj3/paxos"
)

// additions to PaxosRSM state
type PaxosRSMImpl struct {
	seq int
}

// initialize rsm.impl.*
func (rsm *PaxosRSM) InitRSMImpl() {
	rsm.impl.seq = 0
}

// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
func (rsm *PaxosRSM) AddOp(v interface{}) {
	for {
		status, v_decided := rsm.px.Status(rsm.impl.seq)
		if status != paxos.Decided {
			rsm.px.Start(rsm.impl.seq, v)
			v_decided = rsm.wait(rsm.impl.seq)
		}
		rsm.applyOp(v_decided)
		rsm.px.Done(rsm.impl.seq)
		rsm.impl.seq++
		if v == v_decided {
			break
		}
	}
}

func (rsm *PaxosRSM) wait(seq int) interface{} {
	to := 10 * time.Millisecond
	for {
		status, v := rsm.px.Status(seq)
		if status == paxos.Decided {
			return v
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}
