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
	files            []string
	isRead           map[string]bool
	nReduce          int
	mapWorkId        int
	intermediateFile []string
	mapFinishNum     int
	reduceWorkId     int
	reduceFinishNum  int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) MapTask(args *MapArgs, reply *MapReply) error {

	for k, v := range c.isRead {
		if !v {
			if reply.workId == -1 {
				reply.workId = c.mapWorkId
				c.mapWorkId++
			}
			c.isRead[k] = true
			reply.fileName = k
			return nil
		}
	}
	return nil
}

func (c *Coordinator) MapFinish(args *MapArgs, reply *MapReply) error {
	if args.fileName == "" {
		return errors.New("No file given")
	}
	c.intermediateFile = append(c.intermediateFile, args.fileName)
	c.mapFinishNum++
	return nil
}

func (c *Coordinator) ReduceTask(args *ReduceArgs, reply *ReduceReply) error {
	if c.mapFinishNum < c.mapWorkId {
		reply.mapFinish = false
		return nil
	}
	reply.workId = c.reduceWorkId
	reply.fileName = c.intermediateFile
	reply.nReduce = c.nReduce
	c.reduceWorkId++
	return nil
}

func (c *Coordinator) ReduceFinish(args *ReduceArgs, reply *ReduceArgs) error {
	c.reduceFinishNum++
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
	if c.reduceFinishNum == c.nReduce {
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
