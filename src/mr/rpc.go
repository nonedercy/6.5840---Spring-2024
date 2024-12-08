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

// Add your RPC definitions here.
type TaskArgs struct {
	T  int
	Id int
}

type TaskReply struct {
	T        int
	Id       int
	Filename string
	NReduce  int
}

const (
	TASK_MAP = iota
	TASK_REDUCE
	TASK_IDLE
	TASK_DONE
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
