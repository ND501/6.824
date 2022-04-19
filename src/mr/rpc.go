package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkType int

const (
	MAP        WorkType = 1
	REDUCE     WorkType = 2
	ALLDONE    WorkType = 3
	WAITAWHILE WorkType = 4
)

type DispatchArgs struct {
}

type DispatchReply struct {
	Type    WorkType
	Number  int
	NReduce int
	Data    string
}

type ReportArgs struct {
	Type WorkType
	Data string
}

type ReportReply struct {
	Result bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
