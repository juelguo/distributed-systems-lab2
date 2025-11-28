package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
	"io/ioutil"
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
	for {
		// Request a task from the coordinator.
		// function will be defined in rpc.go
		args := TaskRequestArgs{}
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
		} 
	}


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
	// Rename temporary files to final intermediate files
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

	// 3) Create final output file mr-out-reduceID
	// We should write to a temp file and then rename it
	oname := fmt.Sprintf("mr-out-%d", reduceID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("doReduceTask: cannot create %v: %v", oname, err)
	}
	defer ofile.Close()

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

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// 5) Notify the coordinator
	taskDoneArgs := TaskDoneArgs{
		TaskID:   reply.TaskID,
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
