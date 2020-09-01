package core

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type worker chan struct{} // Worker channel, for broadcast

type Master struct {
	// Your definitions here.
	mapTaskQueue   chan int              // Map task queue, consumed by workers
	mapSuccessSign map[int]chan struct{} // Receive sign when a map task successfully finished
	mapTaskDone    map[int]chan struct{} // Used to notify the map task WaitGroup
	srcFileDict    map[int]string        // Get source file name by map task number

	intermediateFiles chan []string // intermeiate file queue

	reduceTaskQueue   chan int              // Reduce task queue, consumed by workers
	reduceSuccessSign map[int]chan struct{} // Receive sign when a reduce task successfully finished
	reduceTaskDone    map[int]chan struct{} // Used to notify the reduce WaitGroup
	tmpFilesDict      map[int][]string      // Get intermediate files by reduce task number

	workers map[worker]struct{} // Used to broadcast workers

	nReduce int

	isMapTaskOver    bool
	isReduceTaskOver bool

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// When all tasks finished, remind all workers start running reduces
func (m *Master) broadcast() {
	// Wait for all map task finished
	var wgMap sync.WaitGroup
	for taskNumber := range m.mapTaskDone {
		wgMap.Add(1)
		go func(taskNumber int) {
			defer wgMap.Done()
			<-m.mapTaskDone[taskNumber]
		}(taskNumber)
	}
	wgMap.Wait()

	close(m.mapTaskQueue)

	close(m.intermediateFiles)
	for files := range m.intermediateFiles {
		for _, file := range files {
			strs := strings.Split(file, "-")
			reduceNumber, _ := strconv.Atoi(strs[2])
			if _, ok := m.tmpFilesDict[reduceNumber]; !ok {
				m.reduceTaskQueue <- reduceNumber
				m.reduceSuccessSign[reduceNumber] = make(chan struct{})
				m.reduceTaskDone[reduceNumber] = make(chan struct{})
			}
			m.tmpFilesDict[reduceNumber] = append(m.tmpFilesDict[reduceNumber], file)
		}
	}

	m.mu.Lock()
	// Remind all workers to start reduce
	for worker := range m.workers {
		worker <- struct{}{}
		delete(m.workers, worker)
	}

	m.isMapTaskOver = true
	m.mu.Unlock()

	// Wait for all reduce task finished
	var wgReduce sync.WaitGroup
	for taskNumber := range m.reduceTaskDone {
		wgReduce.Add(1)
		go func(taskNumber int) {
			defer wgReduce.Done()
			<-m.reduceTaskDone[taskNumber]
		}(taskNumber)
	}
	wgReduce.Wait()

	close(m.reduceTaskQueue)

	m.mu.Lock()
	// Remind all workers to quit
	for worker := range m.workers {
		worker <- struct{}{}
		delete(m.workers, worker)
	}

	// Completion of work
	m.isReduceTaskOver = true
	m.mu.Unlock()
}

// Distribute a map task to a worker node
func (m *Master) DistributeMapTask(args *MapArgs, reply *MapReply) error {

	taskNumber, ok := <-m.mapTaskQueue
	if !ok { // All map tasks has been consumed.
		reply.MapTaskNumber = 0
		reply.FileName = ""
		return nil
	}

	reply.MapTaskNumber = taskNumber
	reply.FileName = m.srcFileDict[taskNumber]
	reply.NReduce = m.nReduce

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		select {
		case <-m.mapSuccessSign[taskNumber]:
			m.mapTaskDone[taskNumber] <- struct{}{}
			// DEBUG
			// log.Printf("map task %d success\n", taskNumber)
		case <-t.C:
			// If a task hasn't completed its task after 10 second,
			// putting this task back in the queue
			m.mapTaskQueue <- taskNumber
			// DEBUG
			// log.Printf("map task %d failed\n", taskNumber)
		}
	}()

	return nil
}

// Complete a map task
func (m *Master) CompleteMapTask(args *MapArgs, reply *MapReply) error {
	m.intermediateFiles <- args.IntermediateFileNames

	m.mapSuccessSign[args.MapTaskNumber] <- struct{}{}

	return nil
}

// Register a worker channel, and wait for receiving a reduce signal
func (m *Master) WaitForReduce(args *MapArgs, reply *MapReply) error {

	m.mu.Lock()

	// If map tasks is over, skip wait
	if m.isMapTaskOver {
		m.mu.Unlock()
		return nil
	}

	ch := make(chan struct{})
	m.workers[ch] = struct{}{}
	m.mu.Unlock()

	<-ch

	return nil
}

// Distribute a reduce task to a worker node
func (m *Master) DistributeReduceTask(args *ReduceArgs, reply *ReduceReply) error {
	taskNumber, ok := <-m.reduceTaskQueue
	if !ok { // All reduce tasks has been consumed.
		reply.ReduceTaskNumber = 0
		reply.FileNames = nil
		return nil
	}

	reply.ReduceTaskNumber = taskNumber
	reply.FileNames = m.tmpFilesDict[taskNumber]

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		select {
		case <-m.reduceSuccessSign[taskNumber]:
			m.reduceTaskDone[taskNumber] <- struct{}{}
			// DEBUG
			// log.Printf("reduce task %d success\n", taskNumber)
		case <-t.C:
			// If a task hasn't completed its task after 10 second,
			// putting this task back in the queue
			m.reduceTaskQueue <- taskNumber
			// DEBUG
			// log.Printf("reduce task %d failed\n", taskNumber)
		}
	}()

	return nil
}

// Complete a reduce task
func (m *Master) CompleteReduceTask(args *ReduceArgs, reply *ReduceReply) error {

	m.reduceSuccessSign[args.ReduceTaskNumber] <- struct{}{}

	return nil
}

// Register a worker channerl, and wait for quit
func (m *Master) WaitForFinish(args *ReduceArgs, reply *ReduceReply) error {

	m.mu.Lock()

	// If reduce tasks is over, skip wait
	if m.isReduceTaskOver {
		m.mu.Unlock()
		return nil
	}

	ch := make(chan struct{})
	m.workers[ch] = struct{}{}
	m.mu.Unlock()

	<-ch

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	_ = rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		_ = http.Serve(l, nil)
	}()
}

//
// cmd/master/master.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mu.Lock()
	ret = m.isReduceTaskOver
	m.mu.Unlock()

	return ret
}

//
// create a Master.
// cmd/master/master.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	taskNums := len(files)
	m := Master{
		mapTaskQueue:   make(chan int, taskNums),
		mapSuccessSign: make(map[int]chan struct{}),
		mapTaskDone:    make(map[int]chan struct{}),
		srcFileDict:    make(map[int]string),

		intermediateFiles: make(chan []string, 1000),

		reduceTaskQueue:   make(chan int, nReduce),
		reduceSuccessSign: make(map[int]chan struct{}),
		reduceTaskDone:    make(map[int]chan struct{}),
		tmpFilesDict:      make(map[int][]string),

		workers: make(map[worker]struct{}),

		nReduce: nReduce,
	}

	// Your code here.

	// Puts all files into the task queue
	index := 1
	for _, file := range files {
		m.mapTaskQueue <- index
		m.mapSuccessSign[index] = make(chan struct{})
		m.mapTaskDone[index] = make(chan struct{})
		m.srcFileDict[index] = file
		index++
	}

	go m.broadcast()

	m.server()
	return &m
}
