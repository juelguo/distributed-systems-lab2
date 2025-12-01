package mr

import (
    "io/ioutil"
    "log"
    "net"
    "net/http"
    "net/rpc"
    "os"
    "sync"
)

/**
  * Worker RPC Server
  * Author: Pengfei Li
  * Description:
  * Serves intermediate files to other workers upon request
  * and allows coordinator to check worker liveness.
  * Notice: 
  *	I am using Pascal Case for struct and method names
  * cuz they need to be exported for RPC.
**/

type WorkerServer struct {
    mu       sync.Mutex 		// mutex that protects files map
    address  string	   			// address of this worker server (lowercase for internal use)
    files    map[string]bool 	// tracks which intermediate files this worker has
    shutdown chan struct{}		// channel to signal server shutdown
}

// Address returns the worker server's address
func (ws *WorkerServer) Address() string {
    return os.Getenv("MR_WORKER")
}

// Sent by other workers to request an intermediate file
type FetchFileArgs struct {
    FileName string
}

// Reply containing the requested file content
type FetchFileReply struct {
    Content []byte
    Exists  bool
}

// Serve intermediate file content via RPC (using Lab1 logic)
func (ws *WorkerServer) FetchFile(args *FetchFileArgs, reply *FetchFileReply) error {
    ws.mu.Lock()
    defer ws.mu.Unlock()

    content, err := ioutil.ReadFile(args.FileName)
    if err != nil {
        reply.Exists = false
        return nil
    }

    reply.Content = content
    reply.Exists = true
    return nil
}

// Heartbeat allows coordinator to check if worker is alive
func (ws *WorkerServer) Heartbeat(args *struct{}, reply *struct{}) error {
    return nil
}

// Start an RPC server
// Set MR_WORKER_PORT (e.g., ":8080") to listen on a specific port,
// otherwise a random available port is used.
func StartWorkerServer() (*WorkerServer, string) {
    ws := &WorkerServer{
        files:    make(map[string]bool),
        shutdown: make(chan struct{}),
    }

    rpc.Register(ws)
    rpc.HandleHTTP()

    // Use designated address from env, or random port if not set
    // This is to settle address inconsistancy insideout the container
    host := os.Getenv("MR_WORKER")

    _, port, err := net.SplitHostPort(host)
    if err != nil {
        log.Fatalf("WorkerServer: invalid MR_WORKER address: %v", err)
    }
    
    l, err := net.Listen("tcp", ":"+port)
    if err != nil {
        log.Fatalf("WorkerServer: cannot listen on %s: %v", port, err)
    }
	
    log.Printf("WorkerServer: listening on %s", host)

    go http.Serve(l, nil)

    return ws, host
}

// RegisterFile marks a file as available on this worker
func (ws *WorkerServer) RegisterFile(filename string) {
    ws.mu.Lock()
    defer ws.mu.Unlock()
    ws.files[filename] = true
}