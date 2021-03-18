package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TType int

const (
	MAP    TType = 0
	REDUCE TType = 1
)

type MrTask struct {
	TaskType TType
	TaskNum  string
	File     string
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type MrArgs struct {
}

type MrReply struct {
	IsTaskAssigned bool
	IsAllWorkDone  bool
	Task           MrTask
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
