package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type State int

const (
	idle State = iota
	inProgress
	completed
)

type Coordinator struct {
	mu          sync.Mutex
	nReduce     int
	nMap        int
	mapTasks    map[string]MapTask
	reduceTasks map[int]ReduceTask
	finished    bool
}

type MapTask struct {
	State      State
	TaskNumber int
	StartTime  time.Time
}

type ReduceTask struct {
	State     State
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) FetchTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mapDone := true
	for k, v := range c.mapTasks {
		if v.State == idle {
			m := c.mapTasks[k]
			m.State = inProgress
			m.StartTime = time.Now()
			c.mapTasks[k] = m

			reply.FileName = k
			reply.TaskType = Map
			reply.TaskNumber = v.TaskNumber
			reply.NReduce = c.nReduce
			return nil
		}

		if v.State != completed {
			mapDone = false
		}
	}
	if mapDone == false {
		reply.TaskType = Wait
		return nil
	}

	reduceDone := true
	for k, v := range c.reduceTasks {
		if v.State == idle {
			// log.Printf("[%v] Giving reduce task %v", args.Pid, k)
			r := c.reduceTasks[k]
			r.State = inProgress
			r.StartTime = time.Now()
			c.reduceTasks[k] = r

			reply.TaskType = Reduce
			reply.TaskNumber = k
			reply.NMap = c.nMap
			return nil
		}
		if v.State != completed {
			reduceDone = false
		}
	}

	if mapDone && reduceDone {
		reply.TaskType = Finished
		c.finished = true
	} else {
		// log.Printf("Waiting mapDone: %v, reduceDone: %v", mapDone, reduceDone)
		reply.TaskType = Wait
	}
	return nil
}

const timeout = 10 * time.Second

func (c *Coordinator) monitor() {

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		done := c.finished
		for k, v := range c.mapTasks {
			if v.State == inProgress && time.Since(v.StartTime) > timeout {
				m := c.mapTasks[k]
				m.State = idle
				c.mapTasks[k] = m
			}
		}

		for k, v := range c.reduceTasks {
			if v.State == inProgress && time.Since(v.StartTime) > timeout {
				m := c.reduceTasks[k]
				m.State = idle
				c.reduceTasks[k] = m
			}
		}
		c.mu.Unlock()
		if done {
			return
		}
	}

}

func (c *Coordinator) OnMapTaskCompleted(args *MapTaskReportArgs, repl *TaskReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.mapTasks[args.FileName]
	m.State = completed
	c.mapTasks[args.FileName] = m
	repl.Ok = true
	return nil
}

func (c *Coordinator) OnReduceTaskCompleted(args *ReduceTaskReportArgs, repl *TaskReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.reduceTasks[args.TaskNumber]
	m.State = completed
	c.reduceTasks[args.TaskNumber] = m
	// log.Printf("[%v] Completed reduce task %v", args.Pid, args.TaskNumber)
	repl.Ok = true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.mapTasks {
		if v.State != completed {
			// log.Printf("Not completed map task %v", k)
			return false
		}
	}
	for _, v := range c.reduceTasks {
		if v.State != completed {
			// log.Printf("Not completed reduce task %v", k)
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	mapTasks := make(map[string]MapTask)
	nMap := len(files)
	for i, v := range files {
		mapTasks[v] = MapTask{State: idle, TaskNumber: i}
	}

	mapReduce := make(map[int]ReduceTask)
	for i := range nReduce {
		r := ReduceTask{State: idle}
		mapReduce[i] = r
	}

	c := Coordinator{sync.Mutex{}, nReduce, nMap, mapTasks, mapReduce, false}
	go c.monitor()
	c.server(sockname)
	return &c
}
