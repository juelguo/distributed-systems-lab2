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
	"bytes"
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

const coordinatorTimeout = 30 * time.Second // 30s
const maxFailedAttempts = 3

// Periodically check if the coordinator is alive

func checkCoordinatorAlive(done chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	failedAttempts := 0

	for {
		select {
		// Coordinator signals that all tasks are done
		case <-done:
			return
		
		// Timeout
		case <-ticker.C:
			args := HeartbeatArgs{}
			reply := HeartbeatReply{}
			ok := call("Coordinator.Heartbeat", &args, &reply)
			if !ok {
				failedAttempts++
				log.Printf("Worker: Coordinator heartbeat failed (%d/%d)", failedAttempts, maxFailedAttempts)
				if failedAttempts >= maxFailedAttempts {
					log.Printf("Worker: Coordinator is unresponsive. Exiting.")
					os.Exit(1)
				}
			} else {
				failedAttempts = 0 // Reset on success
			}
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := os.Getpid()
	log.Printf("Worker %d: started", workerID) // DEBUG ONLY: Use process ID as worker ID

	// NEW: Start WorkerServer and heartbeat goroutine if in distributed mode
	var workerServer *WorkerServer
	var workerAddress string
	if IsDistributed() {
		workerServer, workerAddress = StartWorkerServer()
		_ = workerServer // Suppress unused variable warning
		coordDone := make(chan struct{})
		go checkCoordinatorAlive(coordDone)
		defer close(coordDone)
	}

	for {
		// Request a task from the coordinator.
		// function will be defined in rpc.go
		args := TaskRequestArgs{WorkerID: workerID, WorkerAddress: workerAddress}
		reply := TaskRequestReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)

		// If the coordinator is not reachable, log the error and exit
		if !ok {
			// Coordinator unreachable, exit worker (change to Printf)
			log.Printf("Worker %d: RPC call to AssignTask failed", workerID)
			return
		}

		// Process the assigned task.
		switch reply.TaskType {
		case TaskTypeMap:
			doMapTask(&reply, mapf, workerServer)
		case TaskTypeReduce:
			doReduceTask(&reply, reducef, workerServer)
		case TaskTypeWait:
			time.Sleep(time.Second)
		case TaskTypeExit:
			log.Printf("Worker %d: received exit signal", workerID)
			return
		default:
			log.Printf("Worker: Unknown task type %v", reply.TaskType) // Don't crash, just log
			return
		} 
	}
}

/** 
 * Author: Shuo Zhang, 
 * Co-Author: Jueliang Guo, Pengfei Li
 * Description: Implement the map task function
**/

func doMapTask(reply *TaskRequestReply, mapf func(string, string) []KeyValue, ws *WorkerServer) {
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
	tempFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		tempFiles[i], err = ioutil.TempFile(".", "mr-temptemp-")
		if err != nil {
			log.Fatalf("doMapTask: cannot create intermediate file for bucket %d", i)
		}
		encoders[i] = json.NewEncoder(tempFiles[i])
	}

	// 4) Distribute key-value pairs to intermediate files
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % reply.NReduce
		err := encoders[reduceTaskNum].Encode(&kv)
		if err != nil {
			log.Fatalf("doMapTask: cannot encode key-value pair %v", kv)
		}
	}

	// 5) IMPORTANT: Atomic rename to avoid partial files!!!
	// It eliminates the need for locking in the coordinator
	// And avoid the issue of incomplete files if a worker crashes
	for i := 0; i < reply.NReduce; i++ {
		tempFiles[i].Close()
		finalFileName := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
		err := os.Rename(tempFiles[i].Name(), finalFileName)
		if err != nil {
			log.Fatalf("doMapTask: cannot rename file %v to %v", tempFiles[i].Name(), finalFileName)
		}
	}

	// NEW: Register intermediate files with WorkerServer
	if ws != nil {
		for i := 0; i < reply.NReduce; i++ {
			intermediateFileName := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
			ws.RegisterFile(intermediateFileName)
		}
	}

	// 6) Notify the coordinator that the map task is done
	taskDoneArgs := TaskRequestArgs{
		TaskID:        mapId,
		TaskType:      TaskTypeMap,
	}
	// If WorkerServer is running, use its address for intermediate file location
	if ws != nil {
		taskDoneArgs.WorkerAddress = ws.Address()
	}
	taskDoneReply := TaskRequestReply{}
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

func doReduceTask(reply *TaskRequestReply, reducef func(string, []string) string, ws *WorkerServer) {
	reduceID := reply.TaskID
	nMap := reply.NMap
	intermediate := []KeyValue{}

	// NEW: 1) Use WorkerServer to fetch intermediate files if in distributed mode

	for mapId := 0; mapId < nMap; mapId++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", mapId, reduceID)
		var content []byte

		if IsDistributed() && ws != nil {
			workerAddr, ok := reply.IntermediateLocations[mapId]
			if !ok {
				log.Printf("doReduceTask: no location for map task %d", mapId)
				continue
			}
			
			if workerAddr == ws.Address() {
				data, err := ioutil.ReadFile(intermediateFileName)
				if err != nil {
					log.Fatalf("doReduceTask: cannot read intermediate file %v from local storage: %v", intermediateFileName, err)
				}
				content = data
			} else {
			res, err := fetchFileHelper(workerAddr, intermediateFileName)
			if err != nil {
				log.Printf("doReduceTask: cannot fetch intermediate file %v from worker %v: %v", intermediateFileName, workerAddr, err)
				continue
			}
			content = res
			}

		} else {
			data, err := ioutil.ReadFile(intermediateFileName)
			if err != nil {
				log.Fatalf("doReduceTask: cannot read intermediate file %v: %v", intermediateFileName, err)
			}
			content = data
		}
		// Decode JSON-encoded KeyValue pairs
		dec := json.NewDecoder(bytes.NewReader(content))
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
	taskDoneArgs := TaskRequestArgs{
		TaskID:   reduceID,
		TaskType: TaskTypeReduce,
	}
	taskDoneReply := TaskRequestReply{}
	
	ok := call("Coordinator.TaskDone", &taskDoneArgs, &taskDoneReply)
	// If the coordinator is not reachable, log the error and exit
	if !ok {
		log.Fatalf("doReduceTask: RPC call to TaskDone failed")
	}
}

// NEW: Helper function to fetch file content from a worker via RPC

func fetchFileHelper(workerAddr string, filename string) ([]byte, error) {
	client, err := rpc.DialHTTP("tcp", workerAddr)
	if err != nil {
		return nil, fmt.Errorf("fetchFileHelper: dialing error: %v", err)
	}
	defer client.Close()

	args := FetchFileArgs{FileName: filename}
	reply := FetchFileReply{}

	err = client.Call("WorkerServer.FetchFile", &args, &reply)
	if err != nil {
		return nil, fmt.Errorf("fetchFileHelper: cannot get %s from %s: %v", filename, workerAddr, err)
	}

	return reply.Content, nil
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

func call(rpcname string, args interface{}, reply interface{}) bool {
	var c *rpc.Client
	var err error

	if IsDistributed() {
		// TCP mode for distributed
		addr := coordinatorSock()
		c, err = rpc.DialHTTP("tcp", addr)
	} else {
		// Unix socket for local mode
		sockname := coordinatorSock()
		c, err = rpc.DialHTTP("unix", sockname)
	}

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
