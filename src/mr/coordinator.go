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

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	nMap        int
	done        bool
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

const taskTimeout = 10 * time.Second

type Task struct {
	ID       int
	File     string
	Status   TaskStatus
	Start    time.Time
	TaskType TaskType
}

// AssignTask assigns work to a worker.
func (c *Coordinator) AssignTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requeueStaleTasks()

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
			return nil
		}
		reply.TaskType = TaskTypeWait
		return nil
	}

	reply.TaskType = TaskTypeExit
	return nil
}

// TaskDone updates coordinator state after a worker finishes.
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var target *[]Task
	switch args.TaskType {
	case TaskTypeMap:
		target = &c.mapTasks
	case TaskTypeReduce:
		target = &c.reduceTasks
	default:
		reply.OK = false
		return nil
	}

	if args.TaskID < 0 || args.TaskID >= len(*target) {
		reply.OK = false
		return nil
	}

	task := &(*target)[args.TaskID]
	if task.Status == InProgress {
		task.Status = Completed
	}

	if c.allTasksDone(c.mapTasks) && c.allTasksDone(c.reduceTasks) {
		c.done = true
	}

	reply.OK = true
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

func (c *Coordinator) requeueStaleTasks() {
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
