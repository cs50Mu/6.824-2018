package mapreduce

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		ntasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var tasks []DoTaskArgs
	taskQ := make(chan DoTaskArgs)
	switch phase {
	case mapPhase:
		for i, file := range mapFiles {
			taskArgs := DoTaskArgs{
				JobName:       jobName,
				File:          file,
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: nOther,
			}
			tasks = append(tasks, taskArgs)
		}
	case reducePhase:
		for i := 0; i < ntasks; i++ {
			taskArgs := DoTaskArgs{
				JobName:       jobName,
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: nOther,
			}
			tasks = append(tasks, taskArgs)
		}
	}

	go func() {
		for _, task := range tasks {
			taskQ <- task
		}
	}()

	successChan := make(chan struct{})
	var successCnt int

loop:
	for {
		select {
		case task := <-taskQ:
			go func() {
				worker := <-registerChan
				isSuccess := call(worker, "Worker.DoTask", &task, &struct{}{})
				if !isSuccess {
					taskQ <- task
				} else {
					successChan <- struct{}{}
					go func() {
						// 此行需要放在单独的goroutine里执行
						// 因为当最后已经无任务可做时，也不再需要worker
						// 此行会阻塞住
						registerChan <- worker
					}()
				}
			}()
		case <-successChan:
			successCnt++
		default:
			if successCnt == ntasks {
				break loop
			}
		}
	}

	fmt.Printf("Schedule: %v done\n", phase)
}
