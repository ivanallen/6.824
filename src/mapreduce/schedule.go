package mapreduce

import (
	"fmt"
	"sync"
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

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	/*
		var mu sync.Mutex
		var cond = sync.NewCond(&mu)
		var workAddrs []string
		go func() {
			for workAddr := range registerChan {
				mu.Lock()
				workAddrs = append(workAddrs, workAddr)
				fmt.Printf("1:workAddrs:%v\n", workAddrs)
				cond.Broadcast()
				mu.Unlock()
			}
		}()
		var pick = func() func() string {
			index := 0
			return func() string {
				mu.Lock()
				defer mu.Unlock()

				for len(workAddrs) == 0 {
					fmt.Printf("2:workAddrs:%v\n", workAddrs)
					cond.Wait()
				}

				i := index % len(workAddrs)
				index = i + 1
				return workAddrs[i]
			}
		}()
	*/

	var wg sync.WaitGroup
	if phase == mapPhase {
		for index, mapFile := range mapFiles {
			wg.Add(1)
			go func(index int, mapFile string) {
				retry := true
				for retry {
					retry = false
					doTaskArgs := &DoTaskArgs{
						JobName:       jobName,
						File:          mapFile,
						Phase:         mapPhase,
						TaskNumber:    index,
						NumOtherPhase: n_other,
					}
					workAddr := <-registerChan
					err := call(workAddr, "Worker.DoTask", doTaskArgs, nil)
					go func() { registerChan <- workAddr }()
					if !err {
						fmt.Printf("call error:%v", err)
						retry = true
					}
				}
				wg.Done()
			}(index, mapFile)
		}
		wg.Wait()
	} else {
		for i := 0; i < nReduce; i++ {
			wg.Add(1)
			go func(index int) {
				retry := true
				for retry {
					retry = false
					doTaskArgs := &DoTaskArgs{
						JobName:       jobName,
						Phase:         reducePhase,
						TaskNumber:    index,
						NumOtherPhase: n_other,
					}
					workAddr := <-registerChan
					err := call(workAddr, "Worker.DoTask", doTaskArgs, nil)
					go func() { registerChan <- workAddr }()
					if !err {
						fmt.Printf("call error:%v", err)
						retry = true
					}
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	}

	fmt.Printf("Schedule: %v done\n", phase)
}
