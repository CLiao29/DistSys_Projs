package viewservice

import (
	"fmt"
	"sync"
)

type EventType int32

const (
	PING         EventType = iota
	PRIMARY_FAIL EventType = iota
	BACKUP_FAIL  EventType = iota
)

// additions to ViewServer state.
type ViewServerImpl struct {
	machines       map[string]int
	used           map[string]int
	view           *View
	nextView       *View
	primaryViewNum uint
	muGlobal       sync.Mutex
}

const UNASSIGNED = ""

// your vs.impl.* initializations here.
func (vs *ViewServer) initImpl() {
	vs.impl.machines = make(map[string]int)
	vs.impl.used = make(map[string]int)
	vs.impl.view = &View{0, UNASSIGNED, UNASSIGNED}
	vs.impl.nextView = nil
	vs.impl.primaryViewNum = 0
}

// server Ping() RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.impl.muGlobal.Lock()
	defer vs.impl.muGlobal.Unlock()
	_, exist := vs.impl.machines[args.Me] // check if the pinging server exist
	vs.impl.machines[args.Me] = DeadPings // restart ttl
	if !exist {
		vs.impl.used[args.Me] = 0
		vs.handleNewMachine()
	} else if args.Viewnum == 0 {
		vs.impl.used[args.Me] = 0
		vs.handleMachineFailure(args.Me)
	}
	if args.Me == vs.impl.view.Primary && args.Viewnum != 0 {
		vs.impl.primaryViewNum = args.Viewnum
	}
	vs.tryAdvanceView()

	// fmt.Println("----------------------")
	// fmt.Println(vs.impl.primaryViewNum)
	// fmt.Println(vs.impl.view)
	// fmt.Println(vs.impl.nextView)
	// fmt.Println(vs.impl.machines)
	// fmt.Println("----------------------")
	if vs.impl.nextView != nil && vs.impl.nextView.Viewnum-vs.impl.primaryViewNum == 1 {
		reply.View = *vs.impl.nextView
	} else {
		reply.View = *vs.impl.view
	}
	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.impl.muGlobal.Lock()
	defer vs.impl.muGlobal.Unlock()
	reply.View = *vs.impl.view
	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	vs.impl.muGlobal.Lock()
	defer vs.impl.muGlobal.Unlock()
	newMachines := make(map[string]int)
	failedMachines := []string{}
	for k, v := range vs.impl.machines {
		v--
		if v > 0 {
			newMachines[k] = v
		} else {
			failedMachines = append(failedMachines, k)
		}
	}
	vs.impl.machines = newMachines
	for _, machine := range failedMachines {
		vs.handleMachineFailure(machine)
	}
	vs.tryAdvanceView()
}

/* helper functions, global lock should not be acquired*/

func (vs *ViewServer) handleMachineFailure(failedMachine string) {
	if failedMachine == vs.impl.view.Primary {
		// fmt.Println("handle primary failure", vs.impl.view.Primary)
		if vs.impl.view.Backup == UNASSIGNED {
			fmt.Println("no backup to be promoted to primary")
		}

		vs.impl.nextView = &View{vs.impl.view.Viewnum + 1, vs.impl.view.Backup, UNASSIGNED}
		newBackup := vs.getIdleMachine()
		if newBackup != "" {
			vs.impl.nextView.Backup = newBackup
		}
	} else if failedMachine == vs.impl.view.Backup { //may need to consider when backup fails before proper view advance
		newBackup := vs.getIdleMachine()
		if newBackup != "" {
			vs.impl.nextView = &View{vs.impl.view.Viewnum + 1, vs.impl.view.Primary, newBackup}
		} else {
			vs.impl.nextView = &View{vs.impl.view.Viewnum + 1, vs.impl.view.Primary, UNASSIGNED}
		}
	}
}

func (vs *ViewServer) handleNewMachine() {
	if vs.impl.view.Primary == UNASSIGNED {
		newPrimary := vs.getIdleMachine()
		vs.impl.nextView = &View{vs.impl.view.Viewnum + 1, newPrimary, UNASSIGNED}
	} else if vs.impl.view.Backup == UNASSIGNED {
		newBackup := vs.getIdleMachine()
		vs.impl.nextView = &View{vs.impl.view.Viewnum + 1, vs.impl.view.Primary, newBackup}
	}
}

func (vs *ViewServer) getIdleMachine() string {
	for k, _ := range vs.impl.machines {
		if vs.impl.used[k] == 0 {
			return k
		}
	}
	return ""
}

func (vs *ViewServer) tryAdvanceView() {
	if vs.impl.primaryViewNum == vs.impl.view.Viewnum && vs.impl.nextView != nil {
		vs.impl.view = vs.impl.nextView
		vs.impl.nextView = nil
		vs.impl.used[vs.impl.view.Primary] = 1
		vs.impl.used[vs.impl.view.Backup] = 1
	}
}

/* end of helper functions*/
