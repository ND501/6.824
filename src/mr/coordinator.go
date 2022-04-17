package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	UnProcessedFiles []string
	ProcessingFiles  []string
	NReduce          int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Dispatch(args *RequestArgs, reply *ResponseArgs) error {
	reply.NReduce = c.NReduce
	// for test
	if len(c.UnProcessedFiles) > 0 {
		var filename string = c.UnProcessedFiles[0]
		reply.Type = MAP
		reply.Data = filename

		c.UnProcessedFiles = c.UnProcessedFiles[1:]
		c.ProcessingFiles = append(c.ProcessingFiles, filename)
	} else {
		reply.Type = REDUCE
		reply.Data = "nil"
	}
	fmt.Printf("len of UnProcessedFiles: %v\nlen of ProcessingFiles: %v\n",
		len(c.UnProcessedFiles), len(c.ProcessingFiles))
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
	c.UnProcessedFiles = files
	c.NReduce = nReduce

	c.server()
	return &c
}
