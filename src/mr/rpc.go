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
type TaskRequestArgs struct{
	WorkerID int  // Used to identify the worker (for debugging/logging)
}

// TaskRequestReply describes the task assigned to a worker.
type TaskRequestReply struct {
	TaskType TaskType 
	TaskID   int
	FileName string
	NReduce  int
	NMap     int
}

// TODO: Maybe we can merge TaskDoneArgs and TaskRequestArgs
// and TaskDoneReply and TaskRequestReply to reduce the number of RPCs?

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

/**
 *	Author: Pengfei Li
 *	Add distributed mode to the basic MapReduce framework.
 *  By setting env varible "MR_COORDINATOR" to the coordinator's IP address and port,
 *  the worker can connect to the coordinator in distributed mode.
 *  If the env variable is not set, the worker will fall back to the original local mode."
**/

func coordinatorSock() string {
	// for distributed mode
	if addr := os.Getenv("MR_COORDINATOR"); addr != "" {
		return addr
	}

	// for local mode
	s := "/var/tmp/5840-mr-"
    s += strconv.Itoa(os.Getuid())
    return s
}

func IsDistributed() bool {
	return os.Getenv("MR_COORDINATOR") != ""
}
