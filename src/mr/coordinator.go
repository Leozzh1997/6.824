package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type FileDescribetor struct {
	IsAllocate   bool
	IsFinish     bool
	AllocateTime int64
	ReduceFile   []string
}

type Coordinator struct {
	// Your definitions here.
	mu                 sync.Mutex
	MapFiles           []string
	MapFilesNum        int
	MapTaskAllocate    map[string]*FileDescribetor
	ReduceTaskAllocate []FileDescribetor
	NReduce            int
	MapWorkId          int
	MapFinishNum       int
	ReduceFinishNum    int
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

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.MapFinishNum == len(c.MapFiles) {
		reply.MapFinish = true
		return nil
	}
	for _, file := range c.MapFiles {
		if c.MapTaskAllocate[file].IsFinish {
			continue
		}
		if !c.MapTaskAllocate[file].IsAllocate {
			c.MapTaskAllocate[file].IsAllocate = true
			c.MapTaskAllocate[file].AllocateTime = time.Now().Unix()
			reply.MapFileAllocate = true
			reply.MapFile = file
			reply.NReduce = c.NReduce
			reply.WorkId = c.MapWorkId
			c.MapWorkId++
			return nil
		}
		var T int64 = 10
		t := time.Now().Unix() - c.MapTaskAllocate[file].AllocateTime // test worker crash
		if t > T {
			c.MapTaskAllocate[file].AllocateTime = time.Now().Unix()
			reply.MapFileAllocate = true
			reply.MapFile = file
			reply.NReduce = c.NReduce
			reply.WorkId = c.MapWorkId
			c.MapWorkId++
			return nil
		}
	}
	reply.MapFileAllocate = false
	return nil
}

func (c *Coordinator) MapFinish(args *MapArgs, reply *MapReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Files == nil {
		return errors.New("file not given")
	}
	for _, file := range args.Files {
		c.ReduceTaskAllocate[file.Id].ReduceFile = append(c.ReduceTaskAllocate[file.Id].ReduceFile, file.FileName)
	}
	c.MapTaskAllocate[args.MapFileName].IsFinish = true
	c.MapFinishNum++
	return nil
}

func (c *Coordinator) ReduceTask(args *ReduceArgs, reply *ReduceReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.NReduce == c.ReduceFinishNum {
		reply.ReduceFinish = true
		return nil
	}
	nReduce := c.NReduce
	for i := 0; i < nReduce; i++ {
		if c.ReduceTaskAllocate[i].IsFinish {
			continue
		}
		if !c.ReduceTaskAllocate[i].IsAllocate {
			c.ReduceTaskAllocate[i].IsAllocate = true
			c.ReduceTaskAllocate[i].AllocateTime = time.Now().Unix()
			reply.ReduceFile = append(reply.ReduceFile, c.ReduceTaskAllocate[i].ReduceFile...)
			reply.ReduceFileAllocate = true
			reply.ReduceTaskId = i
			return nil
		}
		var T int64 = 10
		t := time.Now().Unix() - c.ReduceTaskAllocate[i].AllocateTime
		if t > T {
			c.ReduceTaskAllocate[i].AllocateTime = time.Now().Unix()
			reply.ReduceFile = append(reply.ReduceFile, c.ReduceTaskAllocate[i].ReduceFile...)
			reply.ReduceFileAllocate = true
			reply.ReduceTaskId = i
			return nil
		}
	}
	reply.ReduceFileAllocate = false
	return nil
}

func (c *Coordinator) ReduceFinish(args *ReduceArgs, reply *ReduceReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	id := args.ReduceTaskId
	c.ReduceTaskAllocate[id].IsFinish = true
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ReduceFinishNum == c.NReduce {
		ret = true
		//os.Remove(coordinatorSock())
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
	c := Coordinator{}
	c.MapFiles = append(c.MapFiles, files...)
	c.MapFilesNum = len(files)
	c.MapTaskAllocate = make(map[string]*FileDescribetor)
	for _, file := range files {
		fileDescribetor := FileDescribetor{false, false, 0, nil}
		c.MapTaskAllocate[file] = &fileDescribetor
	}
	c.NReduce = nReduce
	c.MapWorkId = 0
	c.MapFinishNum = 0
	c.ReduceFinishNum = 0
	for i := 0; i < nReduce; i++ {
		s := []string{}
		fileDescribetor := FileDescribetor{false, false, 0, s}
		c.ReduceTaskAllocate = append(c.ReduceTaskAllocate, fileDescribetor)
	}

	c.server()
	return &c
}
