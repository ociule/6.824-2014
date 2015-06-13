package mapreduce

import "container/list"
import "fmt"
import "time"

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

func (mr *MapReduce) asyncSendJob(doJobsDone chan<- DoJobReply, worker string, operation JobType, nJob int, nOtherJob int) {
        args := &DoJobArgs{}
        args.File = mr.file
        args.Operation = operation
        args.JobNumber = nJob
        args.NumOtherPhase = nOtherJob
        var reply DoJobReply

        ok := call(worker, "Worker.DoJob", args, &reply)
        mr.FreeWorkers[worker] = struct{}{}
        if ok == false {
            fmt.Printf("DoJob: RPC %s DoJob error\n", worker)
        } else {
            doJobsDone <- reply
        }
}

func (mr *MapReduce) SendJobsToFreeWorkers(doJobDone chan<- DoJobReply, mapJobsLeft, reduceJobsLeft int) {
    if len(mr.FreeWorkers) > 0 && mapJobsLeft > 0 {
        mapJob := mr.nMap - mapJobsLeft
        var freeWorker string
        for worker, _ := range(mr.FreeWorkers) {
            freeWorker = worker
        }

        // The worker is no longer free
        delete(mr.FreeWorkers, freeWorker)
        DPrintf("sending map job %d to worker %s\n", mapJob, freeWorker)
        go mr.asyncSendJob(doJobDone, freeWorker, Map, mapJob, mr.nReduce)

    }
}

func (mr *MapReduce) RunMaster() *list.List {
    mapJobsLeft := mr.nMap
    reduceJobsLeft := mr.nReduce

    doJobDone := make(chan DoJobReply)
	for {
        select {
		case worker := <-mr.registerChannel:
		    wi := new(WorkerInfo)
		    wi.address = worker
		    mr.Workers[worker] = wi
		    mr.FreeWorkers[worker] = struct{}{}
		    fmt.Println("new worker", len(mr.Workers), worker)
        case <- time.After(time.Millisecond * 100):
            fmt.Println("free workers", len(mr.FreeWorkers))
            mr.SendJobsToFreeWorkers(doJobDone, mapJobsLeft, reduceJobsLeft)
		case _ = <-doJobDone:
            mapJobsLeft -= 1
            DPrintf("Job done\n")
        }
	}

	// Send nMap map jobs to nMap workers
	//for i := 0; i < nMap; i++ {
	//}
	fmt.Println("")
	return mr.KillWorkers()
}
