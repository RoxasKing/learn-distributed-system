package core

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// cmd/worker/worker.go calls this function.
//
func Worker(mapFunc func(string, string) []KeyValue, reduceFunc func(string, []string) string) {

	// Your worker implementation here.

	go func() {
		for {
			if !CallDistributeMapTask(mapFunc) {
				break
			}
		}
	}()

	// Wait for all map tasks to complete and then start the Reduce task.
	CallWaitForReduce()

	go func() {
		for {
			if !CallDistributeReduceTask(reduceFunc) {
				break
			}
		}
	}()

	// Wait for all reduce tasks to complete and then start the Reduce task.
	CallWaitForFinish()

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func CallDistributeMapTask(mapFunc func(string, string) []KeyValue) bool {
	args := MapArgs{}
	reply := MapReply{}

	if !call("Master.DistributeMapTask", &args, &reply) {
		log.Printf("Call %v failed\n", "Master.DistributeMapTask")
		time.Sleep(1 * time.Second)
		return true
	}

	if reply.MapTaskNumber == 0 { // There has no map task to consume, so return false
		return false
	}

	mapNumber := reply.MapTaskNumber
	fileName := reply.FileName
	nReduce := reply.NReduce

	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("cannot open %v", fileName)
		time.Sleep(1 * time.Second)
		return true
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", fileName)
		time.Sleep(1 * time.Second)
		return true
	}
	file.Close()

	intermediate := mapFunc(fileName, string(content))
	sort.Sort(ByKey(intermediate))

	dict := make(map[int][]KeyValue)
	for _, kv := range intermediate {
		reducerNumber := ihash(kv.Key)%nReduce + 1
		dict[reducerNumber] = append(dict[reducerNumber], kv)
	}

	var intermediateFiles []string
	for reduceNumber, kvs := range dict {
		if len(kvs) == 0 {
			continue
		}
		intermediateFileName := fmt.Sprintf("mr-%d-%d", mapNumber, reduceNumber)
		intermediateFiles = append(intermediateFiles, intermediateFileName)
		ifile, _ := os.Create(intermediateFileName)
		enc := json.NewEncoder(ifile)
		for _, kv := range kvs {
			_ = enc.Encode(kv)
		}
		ifile.Close()
	}

	go CallCompleteMapTask(mapNumber, intermediateFiles)
	return true
}

func CallCompleteMapTask(mapTasNumber int, intermediateFiles []string) {
	args := MapArgs{
		MapTaskNumber:         mapTasNumber,
		IntermediateFileNames: intermediateFiles,
	}
	reply := MapReply{}

	call("Master.CompleteMapTask", &args, &reply)
}

func CallWaitForReduce() {
	args := MapArgs{}
	reply := MapReply{}

	// Wait for reply, then start reduce work
	call("Master.WaitForReduce", &args, &reply)
}

func CallDistributeReduceTask(reduceFunc func(string, []string) string) bool {
	args := ReduceArgs{}
	reply := ReduceReply{}

	if !call("Master.DistributeReduceTask", &args, &reply) {
		log.Printf("Call %v failed\n", "Master.DistributeReduceTask")
		time.Sleep(1 * time.Second)
		return true
	}

	reduceNumber := reply.ReduceTaskNumber
	fileNames := reply.FileNames

	if reduceNumber == 0 { // There has no reduce task to consume, so return false
		return false
	}

	kvs := make(map[string][]string)
	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			log.Printf("cannot open %v", fileName)
			time.Sleep(1 * time.Second)
			return true
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
		file.Close()
	}

	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	oname := fmt.Sprintf("mr-out-%d", reduceNumber)
	ofile, _ := os.Create(oname)
	for _, key := range keys {
		output := reduceFunc(key, kvs[key])
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
	ofile.Close()

	go CallCompleteReduceTask(reduceNumber)
	return true
}

func CallCompleteReduceTask(reduceTaskNumber int) {
	args := ReduceArgs{
		ReduceTaskNumber: reduceTaskNumber,
	}
	reply := ReduceReply{}

	call("Master.CompleteReduceTask", &args, &reply)
}

func CallWaitForFinish() {
	args := ReduceArgs{}
	reply := ReduceReply{}

	// Wait for reply, then start reduce work
	call("Master.WaitForFinish", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
