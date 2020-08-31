package main

//
// start a worker process, which is implemented
// in map-reduce/core/worker.go. typically there will be
// multiple worker processes, talking to one master.
//
// go run worker.go wc.so
//
// Please do not change this file.
//

import (
	"fmt"
	"log"
	"os"
	"plugin"

	"github.com/RoxasKing/learn-distributed-system/map-reduce/core"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: worker xxx.so\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	core.Worker(mapf, reducef)
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. map-reduce/plugins/wc/wc.so
//
func loadPlugin(filename string) (
	func(string, string) []core.KeyValue,
	func(string, []string) string,
) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []core.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
