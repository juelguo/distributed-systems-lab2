package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
// TaskType represents the kind of work a worker should do.
type TaskType int

const (
	TaskTypeNone TaskType = iota
	TaskTypeMap
	TaskTypeReduce
	TaskTypeWait
	TaskTypeExit
)

// TaskRequestArgs is sent by workers asking for work.
type TaskRequestArgs struct{}

// TaskRequestReply describes the task assigned to a worker.
type TaskRequestReply struct {
	TaskType TaskType
	TaskID   int
	FileName string
	NReduce  int
	NMap     int
}

// TaskDoneArgs is sent by workers when they finish a task.
type TaskDoneArgs struct {
	TaskID   int
	TaskType TaskType
}

// TaskDoneReply acknowledges a task report.
type TaskDoneReply struct {
	OK bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
