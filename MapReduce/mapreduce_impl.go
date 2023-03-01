package mapreduce

// any additional state that you want to add to type MapReduce
type MapReduceImpl struct {
	mapJobs      []int
	mapOccupied  []bool
	mapCompleted []bool
	mapLeft      int
	rdcJobs      []int
	rdcOccupied  []bool
	rdcCompleted []bool
	rdcLeft      int
}

// additional initialization of mr.* state beyond that in InitMapReduce
func (mr *MapReduce) InitMapReduceImpl(nmap int, nreduce int,
	file string, master string) {

	mr.Workers = make(map[string]*WorkerInfo)

	// put them into fields
	mJobs := make([]int, mr.nMap)
	mOccupied := make([]bool, mr.nMap)
	mCompleted := make([]bool, mr.nMap)
	for i := 0; i < mr.nMap; i++ {
		mJobs[i] = i
		mOccupied[i] = false
		mCompleted[i] = false
	}

	rJobs := make([]int, mr.nReduce)
	rOccupied := make([]bool, mr.nReduce)
	rCompleted := make([]bool, mr.nMap)
	for i := 0; i < mr.nReduce; i++ {
		rJobs[i] = i
		rOccupied[i] = false
		rCompleted[i] = false
	}

	mr.impl = MapReduceImpl{mapJobs: mJobs, rdcJobs: rJobs,
		mapOccupied: mOccupied, rdcOccupied: rOccupied,
		mapCompleted: mCompleted, rdcCompleted: rCompleted,
		mapLeft: mr.nMap, rdcLeft: mr.nReduce}

}
