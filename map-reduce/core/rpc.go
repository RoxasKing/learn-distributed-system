package core

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type MapArgs struct {
	MapTaskNumber         int
	IntermediateFileNames []string
}

type MapReply struct {
	MapTaskNumber int
	FileName      string
	NReduce       int
}

type ReduceArgs struct {
	ReduceTaskNumber int
}

type ReduceReply struct {
	ReduceTaskNumber int
	FileNames        []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
