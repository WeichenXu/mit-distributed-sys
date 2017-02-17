package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// 1. choose a worker to do the job
	// 2. set up the input arguments
	// 3. synchronously RPC call DoTask()
	//

	var wg sync.WaitGroup
	wg.Add(ntasks)
	// initiate ntasks doWork RPC call
	for i := 0; i < ntasks; i++{
		go func(i int){
			// set up the RPC argument
			var worker string
			var ok bool
			args := new(DoTaskArgs)
			args.JobName = mr.jobName
			args.Phase = phase
			args.NumOtherPhase = nios
			args.TaskNumber = i
			args.File = mr.files[i]

			// assign the current task to a worker until it succeed
			for ok == false{
				worker = <-mr.registerChannel
				ok = call(worker, "Worker.DoTask", args, new(struct{}))
			}
			// will get blocked if we enqueue the message first
			wg.Done()
			mr.registerChannel <- worker
		}(i)
	}
	// wait untill all tasks are finished
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
