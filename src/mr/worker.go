package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Work struct {
	MapFile      string
	ReduceFile   []string
	MapWorkId    int
	ReduceWorkId int
	NReduce      int
}

type cmp []string

func (a cmp) Len() int           { return len(a) }
func (a cmp) Less(i, j int) bool { return a[i] < a[j] }
func (a cmp) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

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
	work := Work{MapWorkId: -1, ReduceWorkId: -1}
	intermediate := []KeyValue{}
	for {
		mapRet := CallMap(&work)
		if !mapRet {
			break
		}
		file, err := os.Open(work.MapFile)
		if err != nil {
			log.Fatalf("cannot open %v", work.MapFile)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", work.MapFile)
		}
		file.Close()
		kva := mapf(work.MapFile, string(content))
		intermediate = append(intermediate, kva...)
	}
	fmt.Println(work.MapWorkId)
	if work.MapWorkId != -1 {
		oname := "mr-inter-" + strconv.Itoa(work.MapWorkId) + ".txt"
		ofile, err := os.Create(oname)
		if err != nil {
			fmt.Println(err)
		}
		for _, kv := range intermediate {
			fmt.Fprintf(ofile, "%v\n", kv.Key)
		}
		CallMapFinish(oname)
	}

	// uncomment to send the Example RPC to the coordinator.
	// Call Reduce task
	//ret = false means map Step unfinished
	for {
		if ret := CallReduce(&work); ret {
			break
		}
		time.Sleep(time.Second)
	}
	if work.ReduceWorkId == -1 {
		fmt.Println("Enough Worker")
		return
	}
	//execute reducef
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	reducePre := []string{}
	for _, filename := range work.ReduceFile {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("can't open file")
		}
		content, err := ioutil.ReadAll(file)
		words := strings.FieldsFunc(string(content), ff)
		for _, word := range words {
			if ihash(word)%work.NReduce == work.ReduceWorkId {
				reducePre = append(reducePre, word)
			}
		}
	}
	oname := "mr-out-" + strconv.Itoa(work.ReduceWorkId) + ".txt"
	ofile, _ := os.Create(oname)
	sort.Sort(cmp(reducePre))
	i := 0
	for i < len(reducePre) {
		j := i + 1
		for j < len(reducePre) && reducePre[j] == reducePre[i] {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, reducePre[i])
		}
		output := reducef(reducePre[i], values)
		fmt.Fprintf(ofile, "%v %v\n", reducePre[i], output)
		i = j
	}
	CallReduceFinish()
	fmt.Println("All Finish")
	//CallExample()
}

/*func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.ExampleTask", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}*/

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallMap(work *Work) bool {

	// declare an argument structure.
	args := MapArgs{WorkId: work.MapWorkId}
	// fill in the argument(s).

	// declare a reply structure.
	reply := MapReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.MapTask", &args, &reply)
	if !reply.FileAllocate {
		return false
	}
	//fmt.Printf("%v %v\n", reply.WorkId, reply.FileName)
	work.MapFile = reply.FileName
	work.MapWorkId = reply.WorkId
	return true
}

func CallMapFinish(fileName string) {
	args := MapArgs{FileName: fileName}
	reply := MapReply{}
	call("Coordinator.MapFinish", &args, &reply)

}

func CallReduce(work *Work) bool {
	args := ReduceArgs{}
	reply := ReduceReply{}
	call("Coordinator.ReduceTask", &args, &reply)
	if !reply.MapFinish {
		return false
	}
	work.ReduceWorkId = reply.WorkId
	work.ReduceFile = reply.FileName
	work.NReduce = reply.NReduce
	return true
}

func CallReduceFinish() {
	args := ReduceArgs{}
	reply := ReduceReply{}
	call("Coordinator.ReduceFinish", &args, &reply)
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
	//fmt.Println("call finish")
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
