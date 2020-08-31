package main

//
// same as crash.go but doesn't actually crash.
//
// go build -buildmode=plugin nocrash.go
//

import (
	crand "crypto/rand"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/RoxasKing/learn-distributed-system/map-reduce/core"
)

func maybeCrash() {
	max := big.NewInt(1000)
	rr, _ := crand.Int(crand.Reader, max)
	if false && rr.Int64() < 500 {
		// crash!
		os.Exit(1)
	}
}

func Map(filename string, contents string) []core.KeyValue {
	maybeCrash()

	kva := []core.KeyValue{
		{Key: "a", Value: filename},
		{Key: "b", Value: strconv.Itoa(len(filename))},
		{Key: "c", Value: strconv.Itoa(len(contents))},
		{Key: "d", Value: "xyzzy"},
	}
	return kva
}

func Reduce(key string, values []string) string {
	maybeCrash()

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}

var (
	_ = maybeCrash
	_ = Map
	_ = Reduce
)
