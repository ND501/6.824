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

type TimerPool struct {
	tChan    chan string
	timerMap map[string]*time.Timer
}

func (tp *TimerPool) Init() {
	tp.tChan = make(chan string)
	tp.timerMap = make(map[string]*time.Timer)
}

func (tp *TimerPool) Add(taskname string) {
	tp.timerMap[taskname] = time.NewTimer(10 * time.Second)
	go func() {
		<-tp.timerMap[taskname].C
		tp.tChan <- taskname
	}()
}

func (tp *TimerPool) Remove(taskname string) int {
	if _, ok := tp.timerMap[taskname]; ok {
		tp.timerMap[taskname].Stop()
		delete(tp.timerMap, taskname)
		return 0
	}
	return -1
}

func (tp *TimerPool) Size() int {
	return len(tp.timerMap)
}

type RpcType int

const (
	DISPATCH RpcType = 1
	REPORT   RpcType = 2
	REDUCEGO RpcType = 3
)

type RpcEvent struct {
	Type RpcType
	Sync chan int
}

type Phase int

const (
	PHASE_MAP    Phase = 1
	PHASE_REDUCE Phase = 2
	PHASE_DONE   Phase = 3
)

type Coordinator struct {
	// Your definitions here.
	tasks   []string
	tp      TimerPool
	ph      Phase
	nReduce int
	count   int
	mutex   sync.Mutex
}

func (c *Coordinator) Init(files []string, nums int) {
	c.tp.Init()
	c.count = 0
	c.ph = PHASE_MAP
	c.tasks = files
	c.nReduce = nums
}

func (c *Coordinator) Run() {
	// log.Println("Coordinator running...")

	for {
		taskname := <-c.tp.tChan
		c.mutex.Lock()
		if c.tp.Remove(taskname) == 0 {
			c.tasks = append(c.tasks, taskname)
			// log.Printf("%v timeout, phase is %v", taskname, c.ph)
		}
		c.mutex.Unlock()
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
			c.tp.Add(reply.Data)
		} else {
			reply.Type = WAITAWHILE
		}
	case PHASE_REDUCE:
		if len(c.tasks) > 0 {
			reply.Type = REDUCE
			reply.Data = c.tasks[0]
			c.tasks = c.tasks[1:]
			c.tp.Add(reply.Data)
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

	c.mutex.Lock()
	if c.tp.Remove(args.Data) == 0 {
		reply.Result = true
		// log.Printf("%v report, phase is %v", args.Data, c.ph)
		if len(c.tasks) == 0 && c.tp.Size() == 0 {
			c.ph++
			// log.Printf("move phase to %v", c.ph)
			if c.ph == PHASE_REDUCE {
				for i := 0; i < c.nReduce; i++ {
					c.tasks = append(c.tasks, strconv.Itoa(i))
				}
				// log.Printf("init reduce tasks")
			}
		}
	}
	c.mutex.Unlock()

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
	go c.Run()

	c.server()
	return &c
}
