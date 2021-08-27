package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Work struct {
	mapFile      string
	reduceFile   []string
	mapWorkId    int
	reduceWorkId int
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//step1 Call MapTask and
	//inform coordinator intermediateFile
	work := Work{mapWorkId: -1, reduceWorkId: -1}
	intermediate := []KeyValue{}
	for {
		mapRet := CallMap(&work)
		if !mapRet {
			break
		}
		file, err := os.Open(work.mapFile)
		if err != nil {
			log.Fatalf("cannot open %v", work.mapFile)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", work.mapFile)
		}
		file.Close()
		kva := mapf(work.mapFile, string(content))
		intermediate = append(intermediate, kva...)
	}
	oname := "mr-inter-" + string(work.mapWorkId)
	ofile, _ := os.Create(oname)
	for _, kv := range intermediate {
		fmt.Fprintf(ofile, "%v\n", kv.Key)
	}
	CallMapFinish(oname)
	// uncomment to send the Example RPC to the coordinator.
	// step2 Call Reduce task
	for {
		ret := CallReduce(&work) //ret = false means map Step unfinished
		if !ret {
			time.Sleep(time.Second)
		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallMap(work *Work) bool {

	// declare an argument structure.
	args := MapArgs{}

	// fill in the argument(s).

	// declare a reply structure.
	reply := MapReply{workId: work.mapWorkId}

	// send the RPC request, wait for the reply.
	mapRet := call("Coordinator.MapTask", &args, &reply)
	if !mapRet {
		return mapRet
	}
	work.mapFile = reply.fileName
	work.mapWorkId = reply.workId
	return true
}

func CallMapFinish(fileName string) {
	args := MapArgs{fileName: fileName}
	reply := MapReply{}
	call("Coordinator.MapInform", &args, &reply)
}

func CallReduce(work *Work) bool {

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
