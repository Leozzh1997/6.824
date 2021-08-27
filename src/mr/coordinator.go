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
	workId           int
	intermediateFile []string
	mapFinishNum     int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) MapTask(args *MapArgs, reply *MapReply) error {
	if reply.workId == -1 {
		reply.workId = c.workId
		c.workId++
	}

	for k, v := range c.isRead {
		if !v {
			c.isRead[k] = true
			reply.fileName = k
			return nil
		}
	}

	return errors.New("Map Task Finish")
}

func (c *Coordinator) MapInform(args *MapArgs, reply *MapReply) error {
	if args.fileName == "" {
		return errors.New("No file given")
	}
	c.intermediateFile = append(c.intermediateFile, args.fileName)
	c.mapFinishNum++
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
	wordId := 0
	isRead := map[string]bool{}
	for _, file := range files {
		isRead[file] = false
	}
	c := Coordinator{files, isRead, nReduce - 8, wordId}

	// Your code here.

	c.server()
	return &c
}
