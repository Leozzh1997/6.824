package mr

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

/*type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}*/

// Add your RPC definitions here.
type MapArgs struct {
	WorkId   int
	FileName string
}

type MapReply struct {
	FileName     string
	WorkId       int
	FileAllocate bool
}

type ReduceArgs struct {
	FileName string
}

type ReduceReply struct {
	FileName  []string
	WorkId    int
	MapFinish bool
	NReduce   int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
