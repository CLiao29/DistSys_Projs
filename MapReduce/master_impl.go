package mapreduce

import (
	"fmt"
	"time"
)

// any additional state that you want to add to type WorkerInfo
type WorkerInfoImpl struct {
	free bool
}

// run the MapReduce job across all the workers
func (mr *MapReduce) RunMasterImpl() {

	fmt.Printf("start mapping, %d jobs in total\n", mr.nMap)
	// assign workers to map
	for mr.impl.mapLeft > 0 {
		if len(mr.registerChannel) != 0 {
			mr.UpdateWorker()
		}
		mr.UpdateCompleted()

		for _, w := range mr.Workers {
			if w.impl.free {

				allocate := -1
				for _, job := range mr.impl.mapJobs {
					if !mr.impl.mapOccupied[job] {
						allocate = job
						break
					}
				}
				if allocate == -1 {
					break
				}
				 
				w.impl.free = false
				mr.impl.mapOccupied[allocate] = true
				fmt.Printf("Job  %d assigned to %s\n", allocate, w.address)
				//mr.mu.Unlock()
				go func(wk *WorkerInfo) {
					var reply DoJobReply
					mr.DoJob(wk, Map, allocate, mr.nReduce, &reply)
					//mr.mu.Lock()
					if reply.OK {
						mr.impl.mapCompleted[allocate] = true
						wk.impl.free = true
					} else {
						mr.impl.mapOccupied[allocate] = false
						time.Sleep(50 * time.Millisecond)
						wk.impl.free = true
					}
					//mr.mu.Unlock()
				}(w)
			}
		}
	}

	fmt.Printf("start reducing, %d jobs in total", mr.nReduce)
	//assign workers to reduce
	for mr.impl.rdcLeft > 0 {
		if len(mr.registerChannel) != 0 {
			mr.UpdateWorker()
		}
		mr.UpdateCompleted()
		for _, w := range mr.Workers {
			if w.impl.free {
				allocate := -1
				for _, job := range mr.impl.rdcJobs {
					if !mr.impl.rdcOccupied[job] {
						allocate = job
						break
					}
				}
				if allocate == -1 {
					break
				}
				mr.mu.Lock()
				w.impl.free = false
				mr.impl.rdcOccupied[allocate] = true
				mr.mu.Unlock()

				go func(wk *WorkerInfo) {
					var reply DoJobReply
					mr.DoJob(wk, Reduce, allocate, mr.nMap, &reply)
					//time.Sleep(time.Duration(3) * time.Second)
					//mr.mu.Lock()
					if reply.OK {
						mr.impl.rdcCompleted[allocate] = true
						wk.impl.free = true
					} else {
						mr.impl.rdcOccupied[allocate] = false
						time.Sleep(50 * time.Millisecond)
						wk.impl.free = true
					}
					//mr.mu.Unlock()
				}(w)
			}
		}
	}

}

func (mr *MapReduce) DoJob(w *WorkerInfo, operation JobType, job int, otherPhase int, reply *DoJobReply) {
	DPrintf("DoWork %s %s\n", operation, w.address)
	args := &DoJobArgs{File: mr.file, Operation: operation, JobNumber: job, NumOtherPhase: otherPhase}
	ok := call(w.address, "Worker.DoJob", args, &reply)
	if !ok {
		fmt.Printf("DoWork: RPC %s DoJob %s error\n", w.address, operation)
	}

}

func (mr *MapReduce) UpdateWorker() {

	args := <-mr.registerChannel
	newInfo := WorkerInfo{address: args, impl: WorkerInfoImpl{free: true}}
	mr.Workers[args] = &newInfo
	fmt.Printf("one worker updated\n")
}

func (mr *MapReduce) UpdateCompleted() {
	mcomplete := 0
	rcomplete := 0
	for i := 0; i < mr.nMap; i++ {
		if mr.impl.mapCompleted[i] {
			mcomplete += 1
		}
	}
	for i := 0; i < mr.nReduce; i++ {
		if mr.impl.rdcCompleted[i] {
			rcomplete += 1
		}
	}
	mr.impl.mapLeft = mr.nMap - mcomplete
	mr.impl.rdcLeft = mr.nReduce - rcomplete
}
