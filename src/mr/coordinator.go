package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Phase int

const (
	PHASE_MAP    Phase = 1
	PHASE_REDUCE Phase = 2
	PHASE_DONE   Phase = 3
)

// Race detector will report warnings for ph, nReduce and count.
// It will not affect the correctness of the program
type Coordinator struct {
	// Your definitions here.
	tasks   []string
	tp      map[string]*time.Timer
	ph      Phase
	nReduce int
	count   int
	mutex   sync.Mutex
}

// Use PrepareForReduce to make reduce task list for coordinator.
// Please make sure c.mutex has been locked.
func (c *Coordinator) PrepareForReduce() {
	for i := 0; i < c.nReduce; i++ {
		c.tasks = append(c.tasks, strconv.Itoa(i))
	}
}

func (c *Coordinator) Init(files []string, nums int) {
	c.tp = make(map[string]*time.Timer)
	c.count = 0
	c.ph = PHASE_MAP
	c.tasks = files
	c.nReduce = nums
}

func (c *Coordinator) AddTimer(taskname string) {
	c.tp[taskname] = time.AfterFunc(10*time.Second, func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if _, ok := c.tp[taskname]; !ok {
			// Already finished, no need for processing
			log.Printf("task %v has finished, cannot timeout", taskname)
		} else {
			// Task timeout, remove from c.tp and append to c.tasks
			log.Printf("task %v time out", taskname)
			delete(c.tp, taskname)
			c.tasks = append(c.tasks, taskname)
		}
	})
}

func (c *Coordinator) RemoveTimer(taskname string) int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, ok := c.tp[taskname]; !ok {
		// Already timeout, remove fail, return -1
		log.Printf("task %v has time out, cannot remove timer", taskname)
		c.mutex.Unlock()
		return -1
	} else {
		// Remove successed, return 0
		c.tp[taskname].Stop()
		delete(c.tp, taskname)
		// In below situation, push forward phase
		if len(c.tasks) == 0 && len(c.tp) == 0 {
			c.ph++
			if c.ph == PHASE_REDUCE {
				c.PrepareForReduce()
			}
		}
		return 0
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Dispatch(args *DispatchArgs, reply *DispatchReply) error {
	reply.NReduce = c.nReduce
	reply.Number = c.count
	c.count++

	c.mutex.Lock()
	switch c.ph {
	case PHASE_MAP:
		if len(c.tasks) > 0 {
			reply.Type = MAP
			reply.Data = c.tasks[0]
			c.tasks = c.tasks[1:]
			c.AddTimer(reply.Data)
		} else {
			reply.Type = WAITAWHILE
		}
	case PHASE_REDUCE:
		if len(c.tasks) > 0 {
			reply.Type = REDUCE
			reply.Data = c.tasks[0]
			c.tasks = c.tasks[1:]
			c.AddTimer(reply.Data)
		} else {
			reply.Type = WAITAWHILE
		}
	case PHASE_DONE:
		reply.Type = ALLDONE
	}
	c.mutex.Unlock()

	return nil
}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	reply.Result = false

	if args.Type != WorkType(c.ph) {
		return nil
	}

	if c.RemoveTimer(args.Data) == 0 {
		reply.Result = true
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.ph == PHASE_DONE {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Init(files, nReduce)
	log.Printf("Coordinator running...")

	c.server()
	return &c
}
