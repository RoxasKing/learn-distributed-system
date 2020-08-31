package main

//
// start the master process, which is implemented
// in map-reduce/core/master.go
//
// go run master.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"github.com/RoxasKing/learn-distributed-system/map-reduce/core"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: master inputfiles...\n")
		os.Exit(1)
	}

	m := core.MakeMaster(os.Args[1:], 10)
	for !m.Done() {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
