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
	MapInterFile []FileWithId
	MapWorkId    int
	NReduce      int
	ReduceTaskId int
	IsAllocate   bool
	IsFinish     bool
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
	work := Work{}
	for {
		CallMap(&work)
		if work.IsFinish {
			break
		}
		if !work.IsAllocate {
			time.Sleep(time.Second)
			continue
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
		NReduce := work.NReduce
		intermediate := make([][]KeyValue, NReduce)
		for _, kv := range kva {
			reduceNum := ihash(kv.Key) % NReduce
			intermediate[reduceNum] = append(intermediate[reduceNum], kv)
		}
		for i := 0; i < NReduce; i++ {
			oname := "mr-" + strconv.Itoa(work.MapWorkId) + "-" + strconv.Itoa(i)
			ofile, err := os.Create(oname)
			if err != nil {
				fmt.Println(err)
			}
			for _, kv := range intermediate[i] {
				fmt.Fprintf(ofile, "%v\n", kv.Key)
			}
			file := FileWithId{oname, i}
			work.MapInterFile = append(work.MapInterFile, file)
		}
		CallMapFinish(&work)
	}
	// uncomment to send the Example RPC to the coordinator.
	// Call Reduce task
	//ret = false means map Step unfinished
	//execute reducef
	for {
		CallReduce(&work)
		if work.IsFinish {
			break
		}
		if !work.IsAllocate {
			time.Sleep(time.Second)
			continue
		}
		ff := func(r rune) bool { return !unicode.IsLetter(r) }
		reducePre := []string{}
		for _, filename := range work.ReduceFile {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatal("can't open file")
			}
			content, err := ioutil.ReadAll(file)
			words := strings.FieldsFunc(string(content), ff)
			//error ihash should be used by mapStep
			for _, word := range words {
				reducePre = append(reducePre, word)
			}
		}
		oname := "mr-out-" + strconv.Itoa(work.ReduceTaskId)
		os.Remove(oname)
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
		CallReduceFinish(work.ReduceTaskId)
	}
	//fmt.Println("All Finish")
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
func CallMap(work *Work) {

	// declare an argument structure.
	args := MapArgs{}
	// fill in the argument(s).

	// declare a reply structure.
	reply := MapReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.MapTask", &args, &reply)
	work.MapFile = reply.MapFile
	work.MapWorkId = reply.WorkId
	work.IsAllocate = reply.MapFileAllocate
	work.IsFinish = reply.MapFinish
}

func CallMapFinish(work *Work) {
	args := MapArgs{Files: work.MapInterFile, MapFileName: work.MapFile}
	reply := MapReply{}
	call("Coordinator.MapFinish", &args, &reply)
}

func CallReduce(work *Work) {
	args := ReduceArgs{}
	reply := ReduceReply{}
	call("Coordinator.ReduceTask", &args, &reply)

	work.IsAllocate = reply.ReduceFileAllocate
	work.IsFinish = reply.ReduceFinish
	work.ReduceTaskId = reply.ReduceTaskId
	work.ReduceFile = reply.ReduceFile
}

func CallReduceFinish(id int) {
	args := ReduceArgs{}
	args.ReduceTaskId = id
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
