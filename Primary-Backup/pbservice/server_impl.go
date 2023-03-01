package pbservice

import (
	"sync"
	"time"

	"proj2/viewservice"
)

// additions to PBServer state.
// errorpb
type PBServerImpl struct {
	database        map[string]string
	appliedRequests map[int64]bool
	view            viewservice.View
	muGlobal        sync.Mutex
	muView          sync.Mutex
}

// your pb.impl.* initializations here.
func (pb *PBServer) initImpl() {
	pb.impl.database = make(map[string]string)
	pb.impl.appliedRequests = make(map[int64]bool)
	pb.impl.view = viewservice.View{0, UNASSIGNED, UNASSIGNED}
}

// server Get() RPC handler.
func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.impl.muGlobal.Lock()
	defer pb.impl.muGlobal.Unlock()
	pb.impl.muView.Lock()
	defer pb.impl.muView.Unlock()
	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return nil
	}
	if pb.hasBackup() {
		syncArgs, syncReply := SyncGetArg{pb.impl.view}, SyncGetReply{}
		ok := call(pb.getBackup(), "PBServer.SyncGet", &syncArgs, &syncReply)
		if !ok || !syncReply.Ok {
			reply.Err = ErrWrongServer
			return nil
		}
	}
	val, ok := pb.impl.database[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return nil
	}

	reply.Value = val
	reply.Err = OK
	return nil
}

// server PutAppend() RPC handler.
func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.impl.muGlobal.Lock()
	defer pb.impl.muGlobal.Unlock()
	pb.impl.muView.Lock()
	defer pb.impl.muView.Unlock()
	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return nil
	}
	if pb.isApplied(args.Impl.Id) {
		reply.Err = OK
		return nil
	}
	pb.doPutAppend(*args)
	if pb.hasBackup() {
		syncArgs, syncReply := SyncPutAppendArg{pb.impl.view, *args}, SyncPutAppendReply{}
		//log.Printf("*** sync arg %v", syncArgs)
		ok := call(pb.getBackup(), "PBServer.SyncPutAppend", &syncArgs, &syncReply)
		for !ok || !syncReply.Ok {
			pb.impl.muView.Unlock()
			time.Sleep(viewservice.PingInterval)
			pb.impl.muView.Lock()
			if pb.hasBackup() {
				syncArgs, syncReply = SyncPutAppendArg{pb.impl.view, *args}, SyncPutAppendReply{}
				ok = call(pb.getBackup(), "PBServer.SyncPutAppend", &syncArgs, &syncReply)
			}
		}
	}
	reply.Err = OK
	return nil
}

// ping the viewserver periodically.
// if view changed:
//
//	transition to new view.
//	manage transfer of state from primary to new backup.
func (pb *PBServer) tick() {
	pb.impl.muView.Lock()
	defer pb.impl.muView.Unlock()
	currView, ok := pb.vs.Ping(pb.impl.view.Viewnum)
	// log.Printf("%v ticking, myView %v, received view %v", pb.me, pb.impl.view, currView)
	if ok != nil {
		return
	}
	viewChange := pb.viewChanged(&currView, &pb.impl.view)
	if !viewChange {
		return
	}
	oldView := pb.impl.view
	pb.impl.view = currView
	if !pb.isPrimary() {
		return
	}
	// now we are primary and should sync with the backup if it exists
	if pb.hasBackup() {
		syncArgs, syncReply := SyncDBArg{pb.impl.view, pb.impl.database, pb.impl.appliedRequests}, SyncReply{}
		// log.Printf("primary sync args %v", syncArgs)
		ok := call(pb.getBackup(), "PBServer.SyncDB", &syncArgs, &syncReply)
		if !ok || !syncReply.Ok {
			pb.impl.view = oldView
			// log.Printf("primary synced failed, view %v", pb.impl.view)
		} else {
			pb.vs.Ping(pb.impl.view.Viewnum)
			// log.Printf("primary synced succeeded, view %v", pb.impl.view)
		}
	}
	//log.Printf("@@@ %v", pb)
}

func (pb *PBServer) doPutAppend(args PutAppendArgs) {
	key, value, op, id := args.Key, args.Value, args.Impl.Op, args.Impl.Id
	if op == "Put" {
		pb.impl.database[key] = value
	} else if op == "Append" {
		pb.impl.database[key] += value
	}
	pb.impl.appliedRequests[id] = true

}

//	func (pb *PBServer) isBackup() bool {
//		return pb.me == pb.impl.view.Backup
//	}
func (pb *PBServer) isPrimary() bool {
	return pb.me == pb.impl.view.Primary
}

func (pb *PBServer) hasBackup() bool {
	return pb.impl.view.Backup != UNASSIGNED
}

func (pb *PBServer) getBackup() string {
	return pb.impl.view.Backup
}

func (pb *PBServer) isApplied(id int64) bool {
	_, ok := pb.impl.appliedRequests[id]
	return ok
}
func (pb *PBServer) viewChanged(view1 *viewservice.View, view2 *viewservice.View) bool {
	if view1.Viewnum != view2.Viewnum {
		return true
	}
	if view1.Primary != view2.Primary {
		return true
	}
	if view1.Backup != view2.Backup {
		return true
	}
	return false
}

//
// add RPC handlers for any new RPCs that you include in your design.
//

func (pb *PBServer) SyncDB(args *SyncDBArg, reply *SyncReply) error {
	pb.impl.muGlobal.Lock()
	defer pb.impl.muGlobal.Unlock()
	pb.impl.muView.Lock()
	defer pb.impl.muView.Unlock()
	// log.Printf("received view %v, myview %v", args.View, pb.impl.view)
	if args.View.Viewnum == pb.impl.view.Viewnum {
		pb.impl.database = args.Database
		pb.impl.appliedRequests = args.AppliedRequests
		reply.Ok = true
	} else {
		reply.Ok = false
	}
	return nil
}

func (pb *PBServer) SyncGet(args *SyncGetArg, reply *SyncGetReply) error {
	pb.impl.muGlobal.Lock()
	defer pb.impl.muGlobal.Unlock()
	pb.impl.muView.Lock()
	defer pb.impl.muView.Unlock()
	if args.View.Viewnum == pb.impl.view.Viewnum {
		reply.Ok = true
	} else {
		reply.Ok = false
	}
	return nil
}

func (pb *PBServer) SyncPutAppend(args *SyncPutAppendArg, reply *SyncPutAppendReply) error {
	pb.impl.muGlobal.Lock()
	defer pb.impl.muGlobal.Unlock()
	pb.impl.muView.Lock()
	defer pb.impl.muView.Unlock()
	// log.Printf("received view %v, myview %v", args.View, pb.impl.view)
	if args.View.Viewnum == pb.impl.view.Viewnum {
		if !pb.isApplied(args.Args.Impl.Id) {
			pb.doPutAppend(args.Args)
		}
		reply.Ok = true
	} else {
		reply.Ok = false
	}
	return nil
}
