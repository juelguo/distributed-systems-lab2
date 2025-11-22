# Lab 2: MapReduce

Build a MapReduce coordinator and worker set that can execute user-provided Map/Reduce plugins, tolerate worker failures, and produce the same output as the sequential reference. This README condenses the assignment requirements and how to run the code locally or in containers.

## Prerequisites
- Go 1.15 (matches `go.mod`)
- Optional: Docker or VS Code Dev Containers

## Repo layout
- `src/go.mod` – module name (`6.5840`), Go 1.15 requirement.
- `src/main/` – coordinator/worker entrypoints, sequential reference, test scripts, example inputs.
  - `mrcoordinator.go` – coordinator main (starts RPC server, waits for `Done()`).
  - `mrworker.go` – worker main (loads plugin, loops asking for tasks via RPC).
  - `mrsequential.go` – single-process reference MapReduce (no RPC).
  - `pg-*.txt` – sample inputs.
  - `test-mr.sh`, `test-mr-many.sh` – test scripts.
- `src/mrapps/` – MapReduce plugins: `wc.go` (word count), `indexer.go`, `crash.go`.
- `src/mr/` – implement coordinator/worker/RPC logic: `coordinator.go`, `worker.go`, `rpc.go`.
- `Makefile` – handin packaging helper (not needed for local testing).

## Getting started
1) Build a plugin (example: word count):
```
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
```
2) Run sequential reference:
```
rm -f mr-out*
go run mrsequential.go wc.so pg-*.txt
```
Output goes to `mr-out-0`.

## Basic part
- Write code in `src/mr/coordinator.go`, `src/mr/worker.go`, and `src/mr/rpc.go`. Do **not** change `main/mrcoordinator.go` or `main/mrworker.go`.
- Coordinator:
  - Hand out Map tasks (one per input file) and Reduce tasks (0..nReduce-1) via RPC.
  - Detect slow/failed workers: if a task is not done within ~10s, reassign it.
  - Report `Done() == true` when all tasks complete so `mrcoordinator.go` can exit.
- Worker:
  - Repeatedly RPC for a task, execute it, write outputs, and ask again.
  - Map: bucket intermediate pairs into `mr-X-Y` files (X=map task id, Y=reduce id) using the provided `ihash` to pick Y.
  - Reduce: read all `mr-*-Y` files, apply Reduce, write `mr-out-Y` with lines formatted `"%v %v"` (key value).
  - Exit when coordinator is gone (RPC fails) or when you design a “please exit” task.
- File rules:
  - Intermediate files live in the worker’s current directory.
  - Final reduce outputs must be named `mr-out-X` (X is reduce index).
- Concurrency: coordinator RPC handlers run concurrently; protect shared state with locks.

## Running the distributed version
In one terminal (coordinator):
```
cd src/main
rm -f mr-out*
go run -race mrcoordinator.go pg-*.txt
```
In one or more terminals (workers):
```
cd src/main
go run -race mrworker.go wc.so
```
Compare results to reference:
```
cat mr-out-* | sort | more
```

## Tests
Automated script (uses race detector by default):
```
cd src/main
bash test-mr.sh
```
It exercises word count, indexer, parallelism, job counting, early exit, and crash recovery. `test-mr-many.sh` can loop runs with a timeout.

## Advanced part
- Run across multiple machines (no shared filesystem like EFS).
- Store intermediate data on the worker that produced it.
- Use RPC (or similar) between processes.
- Handle worker failures/delays reasonably (avoid excessive duplicate work).
- At least detect coordinator failure.
- Demo with ≥2 workers, 1 coordinator; be ready to kill processes and keep running.

## Tips and hints
- Use `go run -race` while developing.
- Rebuild plugins after changing `mr/` code:
  - `go build -race -buildmode=plugin ../mrapps/wc.go`
- JSON (`encoding/json`) is a simple way to encode intermediate key/value pairs.
- Use `ioutil.TempFile` + `os.Rename` to atomically write outputs.
- Workers may need to poll/sleep while waiting for work.
- For crash testing, use `../mrapps/crash.go`.
- RPC only sends exported (capitalized) struct fields.
- When calling RPCs:
```
reply := SomeType{}
call(..., &reply)
```