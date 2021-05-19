package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var ok bool

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
	// Your worker implementation here.
	ok = false
	for !ok {
		// 1. 申请Task  mr-nmap-nreduce or mr-out-nreduce ，如果检测到coordinator没开启 break
		registerReply := RegisterReply{}
		call("Coordinator.Register", &RegisterArgs{}, &registerReply)  
		task:=registerReply.Task
	
		// 2. 对应Task调用对应函数
		if task.Phase == MAP_PHASE {
			doMap(mapf, &task)
			
		} else {
			doReduce(reducef, &task)
			
		}
		report(task.WorkerId, nil)
		
	}
}

func doMap(mapf func(string, string) []KeyValue, task *Task) {
	file, err := os.Open(task.FileName)
	if err != nil {
		report(task.WorkerId, err)
		return 
	}
	intermediate := make([][]KeyValue, task.NReduce)
	content, err := ioutil.ReadAll(file)
	if err != nil {
		report(task.WorkerId, err)
		return 
	}
	file.Close()
	kva := mapf(task.FileName, string(content))
	for _, kv := range kva {
		has := ihash(kv.Key) % task.NReduce
		intermediate[has] = append(intermediate[has], kv)
	} 
	for r := 0; r < task.NReduce; r++ {
		fname := "mr-" + strconv.Itoa(task.WorkerId) + "-" + strconv.Itoa(r)
		newFile, err := os.Create(fname)
		if err != nil {
			report(task.WorkerId, err)
			return 
		}
		enc := json.NewEncoder(newFile)
		for i := 0; i < len(intermediate[r]); i++ {
			err := enc.Encode(&intermediate[r][i])    //  将json保存在文件中
			if err != nil {
				report(task.WorkerId, err)
				return 	
			}
		} 
		newFile.Close()
	}
}	
func doReduce(reducef func(string, []string) string, task *Task) {

	kva := []KeyValue{}
	for m := 0; m < task.NMap ; m++ {
		iname := "mr-" + strconv.Itoa(m) + "-" + strconv.Itoa(task.WorkerId)
		ifile, err := os.Open(iname)
		if err != nil {
			report(task.WorkerId, err)
			return 
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
			  break
			}
			kva = append(kva, kv)
		}
	}
	oname := "mr-out-"+strconv.Itoa(task.WorkerId)
	ofile, err := os.Create(oname)
	if err != nil {
		report(task.WorkerId, err)
		return 
	}
	sort.Sort(ByKey(kva))	
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	ofile.Close()
}



func report(idx int, err error) {
	call("Coordinator.Report",&ReportArgs{idx, err}, &ReportReply{})
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "localhost"+":1234")	
	
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// 可能是完成任务从而关闭coor导致的断开，所以不报错
		ok = true
		return true
	}
	
	
	
	err = c.Call(rpcname, args, reply)
	
	c.Close()
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}



