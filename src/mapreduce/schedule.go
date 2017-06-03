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
	var taskDone = make(chan bool)
	var stopRegister = make(chan bool)
	var workerChan = make(chan string, 100)

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(3)
	go func() {
	Done:
		for {
			select {
			case address := <-registerChan:
				workerChan <- address
			case <-stopRegister:
				break Done
			}
		}
		waitGroup.Done()
	}()

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
			case <-time.After(time.Millisecond * 100):
				var b = true // if task taskDone
				for i := 0; i < ntasks; i++ {
					if finishedMap[i] != FINISHED {
						b = false
						if finishedMap[i] == UNASSIGNED {
							fmt.Println("Gonna emit task to channel")
							taskChan <- i
							finishedMap[i] = ASSIGNED
						}
					}
				}
				if b {
					fmt.Println("Stop control channel")
					taskDone <- true
					stopRegister <- true
					fmt.Println("Waiting for someone")
					break Done

				}
			}
		}
		fmt.Println("Gonna exit control there")
		waitGroup.Done()

	}()

	// emit task forever
	go func() {
	Done:
		for {
			select {
			case taskNum := <-taskChan:
				fmt.Println("Get worker here maybe stuck here")
				address := <-workerChan
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
					workerChan <- address

				}()
			case <-time.After(time.Second * 1):
				fmt.Println("Waiting for some thing")
			case <-taskDone:
				break Done
			}

		}
		waitGroup.Done()
	}()
	waitGroup.Wait()
	fmt.Printf("Schedule: %v phase taskDone\n", phase)
}
