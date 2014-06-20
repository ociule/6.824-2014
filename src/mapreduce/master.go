package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	fmt.Println("Waiting for", mr.nMap, "workers")
	// @TODO this will not move forward unless we have enough workers
	for len(mr.Workers) < mr.nMap {
		worker := <-mr.registerChannel
		wi := new(WorkerInfo)
		wi.address = worker
		mr.Workers[worker] = wi
		fmt.Print(len(mr.Workers))
	}

	// Send nMap map jobs to nMap workers
	//for i := 0; i < nMap; i++ {
	//}
	fmt.Println("")
	return mr.KillWorkers()
}
