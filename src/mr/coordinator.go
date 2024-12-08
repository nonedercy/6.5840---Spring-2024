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

const (
	IDLE = iota
	WORKING
	DONE
)

type Coordinator struct {
	// Your definitions here.
	t           int
	nReduce     int
	nWork       int
	workTasks   []*task
	mapTasks    []*task
	reduceTasks []*task
	mu          sync.Mutex
}

type task struct {
	filename string
	status   int
	ch       chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Task(args *TaskArgs, reply *TaskReply) error {
	//fmt.Printf("t is %v, task %v Task call\n", args.T, args.Id)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.t == TASK_DONE {
		reply.T = TASK_DONE
		return nil
	}
	if args.T == c.t && args.Id >= 0 && c.workTasks[args.Id].status != DONE {
		c.workTasks[args.Id].ch <- struct{}{}
		c.workTasks[args.Id].status = DONE
		c.nWork -= 1
		if c.nWork == 0 {
			if c.t == TASK_MAP {
				c.t = TASK_REDUCE
				c.workTasks = c.reduceTasks
				c.nWork = c.nReduce
			} else if c.t == TASK_REDUCE {
				c.t = TASK_DONE
				reply.T = TASK_DONE
				return nil
			}
		}
	}
	for i, t := range c.workTasks {
		if t.status == IDLE {
			reply.T = c.t
			reply.Id = i
			reply.Filename = t.filename
			reply.NReduce = c.nReduce
			t.status = WORKING
			go c.startWatching(i)
			return nil
		}
	}
	reply.T = TASK_IDLE
	return nil
}

func (c *Coordinator) startWatching(i int) {
	//fmt.Printf("t is %v, task %v startWatching\n", c.t, i)
	select {
	case <-c.workTasks[i].ch:
		//fmt.Printf("t is %v, task %v done\n", c.t, i)
	case <-time.After(10 * time.Second):
		c.mu.Lock()
		//fmt.Printf("t is %v, task %v timelimit\n", c.t, i)
		if c.workTasks[i].status == WORKING {
			c.workTasks[i].status = IDLE
		}
		c.mu.Unlock()
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.t == TASK_DONE
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.t = TASK_MAP
	c.nReduce = nReduce
	for _, file := range files {
		c.mapTasks = append(c.mapTasks, &task{filename: file, ch: make(chan struct{})})
	}
	c.workTasks = c.mapTasks
	c.nWork = len(c.workTasks)
	for i := 0; i < nReduce; i += 1 {
		c.reduceTasks = append(c.reduceTasks, &task{ch: make(chan struct{})})
	}
	c.server()
	return &c
}
