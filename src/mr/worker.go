package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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
	log.Printf("Worker %d: started", workerID) // DEBUG ONLY: Use process ID as worker ID
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
		case TaskTypeMap:
			doMapTask(&reply, mapf)
		case TaskTypeReduce:
			doReduceTask(&reply, reducef)
		case TaskTypeWait:
			time.Sleep(time.Second)
		case TaskTypeExit:
			// All tasks are done, exit the worker.
			return
		default:
			log.Fatalf("Worker: Unknown task type %v", reply.TaskType)
			return
		} 
	}


}

/** 
 * Author: Shuo Zhang, 
 * Co-Author: Jueliang Guo, Pengfei Li
 * Description: Implement the map task function
**/

func doMapTask(reply *TaskRequestReply, mapf func(string, string) []KeyValue) {
	mapId := reply.TaskID
	filename := reply.FileName

	// 1) Open a file and read its contents
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("doMapTask: cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("doMapTask: cannot read %v", filename)
	}
	file.Close()

	// 2) Convert content to k-v arrays
	kva := mapf(filename, string(content))

	// 3) Partition kva into nReduce intermediate files
	intermediateFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-temp-%d-%d", mapId, i) // mr-temp-mapID-reduceID
		intermediateFiles[i], err = os.Create(intermediateFileName)
		if err != nil {
			log.Fatalf("doMapTask: cannot create intermediate file %v", intermediateFileName)
		}
		encoders[i] = json.NewEncoder(intermediateFiles[i])
	}

	// 4) Distribute key-value pairs to intermediate files
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % reply.NReduce
		err := encoders[reduceTaskNum].Encode(&kv)
		if err != nil {
			log.Fatalf("doMapTask: cannot encode key-value pair %v", kv)
		}
	}

	// Close all intermediate files
	for i := 0; i < reply.NReduce; i++ {
		intermediateFiles[i].Close()
	}

	// 5) IMPORTANT: Atomic rename to avoid partial files!!!
	// It eliminates the need for locking in the coordinator
	// And avoid the issue of incomplete files if a worker crashes
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

/** 
 * Author: Shuo Zhang
 * Co-Author: Pengfei Li
 * Description: Implement the reduce task function
**/

func doReduceTask(reply *TaskRequestReply, reducef func(string, []string) string) {
	reduceID := reply.TaskID
	nMap := reply.NMap

	// 1) Read all intermediate files: mr-mapID-reduceID
	var intermediate []KeyValue

	for m := 0; m < nMap; m++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", m, reduceID)

		file, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("doReduceTask: cannot open %v: %v", intermediateFileName, err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalf("doReduceTask: cannot decode from %v: %v", intermediateFileName, err)
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// 2) Sort by key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// 3) Create temp output files for the bucket
	oname := fmt.Sprintf("mr-out-%d", reduceID)
	tempFile, err := ioutil.TempFile(".", "mr-out-tmp-")
	if err != nil {
		log.Fatalf("doReduceTask: cannot create temp file for %v: %v", oname, err)
	}
	tempFileName := tempFile.Name()

	// 4) Group by key and call reducef(key, values)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()

	// Atomic rename temp file to final output file
	err = os.Rename(tempFileName, oname)
	if err != nil {
		os.Remove(tempFileName) // Clean up temp file on failure
		log.Fatalf("doReduceTask: cannot rename temp file to %v: %v", oname, err)
	}

	// 5) Notify the coordinator
	taskDoneArgs := TaskDoneArgs{
		TaskID:   reduceID,
		TaskType: TaskTypeReduce,
	}
	taskDoneReply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &taskDoneArgs, &taskDoneReply)
	if !ok {
		log.Fatalf("doReduceTask: RPC call to TaskDone failed")
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

/** 
 * Author: Shuo Zhang
 * Co-Author: Pengfei Li
 * Description: Polling function to communicate with coordinator.
 * 				Returns false if something goes wrong.
**/

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("dialing: %v", err) // Don't crash, just print the error
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
