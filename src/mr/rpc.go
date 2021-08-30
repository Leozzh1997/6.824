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
type FileWithId struct {
	FileName string
	Id int
}

type MapArgs struct {
	Files []FileWithId
	MapFileName string //coordinator give a file to worker to do mapTask
}

type MapReply struct {
	MapFile     string
	WorkId       int
	MapFileAllocate bool
	MapFinish bool
	NReduce int
}

type ReduceArgs struct {
	FileName FileWithId //mr-out-X
	ReduceTaskId int
}

type ReduceReply struct {
	ReduceFile  []string
	ReduceTaskId int
	ReduceFileAllocate    bool
	ReduceFinish bool
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
