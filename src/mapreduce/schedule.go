package mapreduce

import (
	"fmt"
	"time"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

const (
	UNASSIGNED = 0
	ASSIGNED   = 2
	FINISHED   = 3
)

type taskRet struct {
	taskNum int
	ret     bool
}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	var finishedMap = make(map[int]int)
	var taskChan = make(chan int, ntasks)
	var resultChan = make(chan taskRet, ntasks)
	var done = make(chan bool)
	wait := sync.WaitGroup{}
	wait.Add(2)
	go func() {
	Done:
		for {
			select {
			case r := <-resultChan:
				if r.ret {
					finishedMap[r.taskNum] = FINISHED
				} else {
					finishedMap[r.taskNum] = UNASSIGNED
				}
			case <-time.After(time.Second * 1):
				var b = true // if task done
				for i := 0; i < ntasks; i++ {
					if finishedMap[i] != FINISHED {
						b = false
						if finishedMap[i] != ASSIGNED {
							taskChan <- i
							finishedMap[i] = ASSIGNED
						}
					}
				}
				if b {
					done <- true
					break Done
				}
			}
			//fmt.Println("Fuck doing here")
		}
		wait.Done()

	}()

	// emit task forever
	go func() {

	Done:
		for {
			fmt.Println("Gonna start go routine")
			select {
			case address := <-registerChan:
				fmt.Println("Get worker here")
				taskNum := <-taskChan
				fmt.Println("Get task num here")

				go func() {
					taskArgs := DoTaskArgs{JobName: jobName,
						File:                   mapFiles[taskNum], // in case of index 0
						Phase:                  phase,
						TaskNumber:             taskNum,
						NumOtherPhase:          n_other }
					fmt.Printf("Task %d has been send to worker rpc, phase: %s\n ", taskNum, phase)

					resultChan <- taskRet{taskNum,
							      call(address, "Worker.DoTask", taskArgs, nil)}
				}()
				fmt.Println("Fuck here")
			case <-done:
				break Done
			}

		}
		wait.Done()
	}()
	wait.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
