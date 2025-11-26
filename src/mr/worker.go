package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := os.Getpid()
	log.Printf("Worker %d: started", workerID) // Use process ID as worker ID
	for {
		// Request a task from the coordinator.
		// function will be defined in rpc.go
		args := TaskRequestArgs{WorkerID: workerID}
		reply := TaskRequestReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			log.Fatalf("Worker: RPC call to AssignTask failed")
			return
		}
		// Process the assigned task.
		switch reply.TaskType {
		// TaskType will be defined in rpc.go
		case TaskTypeMap:
			doMapTask(&reply, mapf)
		case TaskTypeReduce:
			// TODO: implement reduce task
			// doReduceTask(&reply, reducef)
			return
		case TaskTypeWait:
			time.Sleep(time.Second)
		case TaskTypeExit:
			// All tasks are done, exit the worker.
			return
		default:
			log.Fatalf("Worker: Unknown task type %v", reply.TaskType)
		} // Execute the map function.
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapTask(reply *TaskRequestReply, mapf func(string, string) []KeyValue) {
	filename := reply.FileName
	// Read the input file.

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("doMapTask: cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("doMapTask: cannot read %v", filename)
	}
	file.Close()

	// Call the map funnction
	kva := mapf(filename, string(content))

	// Partition kva into nReduce intermediate files

	intermediateFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		// 路径不知道怎么写，暂时在当前目录下创建临时文件，最后再改名
		intermediateFileName := fmt.Sprintf("mr-temp-%d-%d", reply.TaskID, i)
		intermediateFiles[i], err = os.Create(intermediateFileName)
		if err != nil {
			log.Fatalf("doMapTask: cannot create intermediate file %v", intermediateFileName)
		}
		encoders[i] = json.NewEncoder(intermediateFiles[i])
	}

	// Distribute key-value pairs to intermediate files
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % reply.NReduce
		err := encoders[reduceTaskNum].Encode(&kv)
		if err != nil {
			log.Fatalf("doMapTask: cannot encode key-value pair %v", kv)
		}
	}

	// Close intermediate files
	for i := 0; i < reply.NReduce; i++ {
		intermediateFiles[i].Close()
	}
	// rename temporary files to final intermediate files
	for i := 0; i < reply.NReduce; i++ {
		tempFileName := fmt.Sprintf("mr-temp-%d-%d", reply.TaskID, i)
		finalFileName := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
		err := os.Rename(tempFileName, finalFileName)
		if err != nil {
			log.Fatalf("doMapTask: cannot rename file %v to %v", tempFileName, finalFileName)
		}
	}

	// Notify the coordinator that the map task is done
	taskDoneArgs := TaskDoneArgs{
		TaskID:   reply.TaskID,
		TaskType: TaskTypeMap,
	}
	taskDoneReply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &taskDoneArgs, &taskDoneReply)
	if !ok {
		log.Fatalf("doMapTask: RPC call to TaskDone failed")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
