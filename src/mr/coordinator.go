package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	Files            []string
	IsRead           map[string]bool
	NReduce          int
	MapWorkId        int
	IntermediateFile []string
	MapFinishNum     int
	ReduceWorkId     int
	ReduceFinishNum  int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
/*func (c *Coordinator) ExampleTask(args *ExampleArgs, reply *ExampleReply) error {
	fmt.Println("Called")
	reply.Y = args.X + 1
	return nil
}*/

func (c *Coordinator) MapTask(args *MapArgs, reply *MapReply) error {
	for k, v := range c.IsRead {
		if !v {
			if args.WorkId == -1 {
				reply.WorkId = c.MapWorkId
				c.MapWorkId++
			}
			c.IsRead[k] = true
			reply.FileName = k
			reply.FileAllocate = true
			return nil
		}
	}

	reply.FileAllocate = false
	return nil
}

func (c *Coordinator) MapFinish(args *MapArgs, reply *MapReply) error {
	if args.FileName == "" {
		return errors.New("No file given")
	}
	c.IntermediateFile = append(c.IntermediateFile, args.FileName)
	c.MapFinishNum++
	return nil
}

func (c *Coordinator) ReduceTask(args *ReduceArgs, reply *ReduceReply) error {
	if c.MapFinishNum < c.MapWorkId {
		reply.MapFinish = false
		return nil
	}
	reply.MapFinish = true
	reply.WorkId = c.ReduceWorkId
	reply.FileName = c.IntermediateFile
	reply.NReduce = c.NReduce
	c.ReduceWorkId++
	return nil
}

func (c *Coordinator) ReduceFinish(args *ReduceArgs, reply *ReduceReply) error {
	c.ReduceFinishNum++
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	//Return true when all of reduce tasks finish.
	if c.ReduceFinishNum == c.NReduce {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//
	//Init a coordinator.
	//response a filename that unused to workers to do map task
	//if the map process is finished,inform worker to do reduce task
	//
	mapWordId := 0
	isRead := map[string]bool{}
	for _, file := range files {
		isRead[file] = false
	}
	c := Coordinator{files, isRead, nReduce - 8, mapWordId, nil, 0, 0, 0}
	// Your code here.

	c.server()
	return &c
}
