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

/** 
 * Author: Jueliang Guo 
 * Description: Define the Coordinator structure and its methods
**/

type Coordinator struct {
	mu          sync.Mutex // protect shared access to this structure
	mapTasks    []Task     // map task list - each task corresponds to an input file
	reduceTasks []Task     // reduce task list - each task corresponds to a reduce bucket (intermediate files)
	nMap        int        // number of map tasks = number of input files
	nReduce     int        // number of reduce buckets, set in mrcoordinator.go
	done        bool       // true if all tasks are done.
	// NEW: mapTaskID -> workerAddress
	intermediateLocations map[int]string 
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

const taskTimeout = 10 * time.Second

type Task struct {
	ID     			int
	File     		string
	Status   		TaskStatus
	Start    		time.Time // Used to track timeouts
	TaskType 		TaskType
	WorkerAddress 	string // Address of the worker processing this task
}

/** 
 * Author: Jueliang Guo
 * Co-Author: Pengfei Li
**/

// AssignTask assigns work to a worker.
func (c *Coordinator) AssignTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requeueTimedOutTasks()

	// if there are map tasks remaining, assign a map task
	if !c.allTasksDone(c.mapTasks) {
		if task, ok := c.assignTask(c.mapTasks, TaskTypeMap); ok {
			reply.TaskType = TaskTypeMap
			reply.TaskID = task.ID
			reply.FileName = task.File
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			return nil
		}
		reply.TaskType = TaskTypeWait
		return nil
	}

	// if there are reduce tasks remaining, assign a reduce task
	if !c.allTasksDone(c.reduceTasks) {
		if task, ok := c.assignTask(c.reduceTasks, TaskTypeReduce); ok {
			reply.TaskType = TaskTypeReduce
			reply.TaskID = task.ID
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			reply.IntermediateLocations = make(map[int]string)
			// NEW: provide intermediate file locations for this reduce task
			for mapTaskID, location := range c.intermediateLocations {
				reply.IntermediateLocations[mapTaskID] = location
			}
			return nil
		}
		reply.TaskType = TaskTypeWait
		return nil
	}

	reply.TaskType = TaskTypeExit
	return nil
}

// TaskDone updates coordinator state after a worker finishes.
func (c *Coordinator) TaskDone(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var target *[]Task
	switch args.TaskType {
	case TaskTypeMap:
		target = &c.mapTasks
		// NEW: record intermediate file location
		c.intermediateLocations[args.TaskID] = args.WorkerAddress 
	case TaskTypeReduce:
		target = &c.reduceTasks
	default:
		reply.IsDone = false
		return nil
	}

	if args.TaskID < 0 || args.TaskID >= len(*target) {
		reply.IsDone = false
		return nil
	}

	task := &(*target)[args.TaskID]
	if task.Status == InProgress {
		task.Status = Completed
	}

	if c.allTasksDone(c.mapTasks) && c.allTasksDone(c.reduceTasks) {
		c.done = true
	}

	reply.IsDone = true
	return nil
}

func (c *Coordinator) assignTask(tasks []Task, t TaskType) (*Task, bool) {
	for i := range tasks {
		if tasks[i].Status == Idle {
			tasks[i].Status = InProgress
			tasks[i].Start = time.Now()
			return &tasks[i], true
		}
	}
	return nil, false
}

func (c *Coordinator) allTasksDone(tasks []Task) bool {
	for _, t := range tasks {
		if t.Status != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) requeueTimedOutTasks() {
	now := time.Now()

	for i, task := range c.mapTasks {
		if task.Status == InProgress && now.Sub(task.Start) > taskTimeout {
			c.mapTasks[i].Status = Idle
		}
	}

	for i, task := range c.reduceTasks {
		if task.Status == InProgress && now.Sub(task.Start) > taskTimeout {
			c.reduceTasks[i].Status = Idle
		}
	}
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

// Heartbeat allows workers to check if coordinator is alive
func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
// Distributed mode: set COORDINATOR_PORT (e.g., ":10086") to listen on TCP
// Local mode: uses Unix socket (no env needed)
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	// Check if coordinator should listen on TCP (distributed mode)
	port := os.Getenv("COORDINATOR_PORT")
	if port != "" {
		// Distributed mode: listen on TCP
		l, e := net.Listen("tcp", port)
		if e != nil {
			log.Fatal("listen error:", e)
		}
		log.Printf("Coordinator listening on TCP port %s\n", port)
		go http.Serve(l, nil)
	} else {
		// Local mode: Unix socket
		sockname := coordinatorSock()
		os.Remove(sockname)
		l, e := net.Listen("unix", sockname)
		if e != nil {
			log.Fatal("listen error:", e)
		}
		go http.Serve(l, nil)
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		nReduce:     nReduce,
		nMap:        len(files),
		intermediateLocations: make(map[int]string),
	}

	for i, f := range files {
		c.mapTasks[i] = Task{ID: i, File: f, Status: Idle, TaskType: TaskTypeMap}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{ID: i, Status: Idle, TaskType: TaskTypeReduce}
	}

	c.server()
	return &c
}
