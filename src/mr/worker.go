package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		req := &GetTaskRequest{}
		resp := &GetTaskResponse{}
		if call("Master.GetTask", req, resp) == true {
			switch resp.ErrCode {
			case ErrWait:
				// sleep waiting
				time.Sleep(1 * time.Second)
				continue
			case ErrAllDone:
				// All job done, this worker can be closed.
				break
			case ErrSuccess:
				// Do Map or reduce
				switch resp.Task.TaskType {
				case TypeMap:
					DoMap(resp.Task, mapf)
				case TypeReduce:
					DoReduce(resp.Task, reducef)
				}
			}
		} else {
			// rpc call failed, master is closed, meaning job finished.
			break
		}
	}

	// uncomment to send the Example RPC to the master.
	//CallExample()

}

// store Key-Value in "mr-map-*" files
func DoMap(task Task, mapf func(string, string) []KeyValue) {
	// 1. Read the target input-file
	filename := task.Content
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("[worker]cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[worker]cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	// 2. create temp file, and rename it atomically later.
	oname := fmt.Sprintf("mr-map-%v", task.TaskId)
	tmpfile, err := ioutil.TempFile(".", oname)
	if err != nil {
		log.Fatal(err)
	}

	// 3. write all KeyValue in json format.
	enc := json.NewEncoder(tmpfile)
	for _, kv := range kva {
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("[worker]encode save json err=%v\n", err)
		}
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	// 4. atomically rename tmpfile
	os.Rename(tmpfile.Name(), oname)

	// 5. notice master that this task is done.
	NoticeMaster(task.TaskId, task.TaskType)
}

// read all "mr-map-*" files, and do reduce of keys that ihash(key)==reduceNumber.
// Then sore them in "mr-out-*" file.
func DoReduce(task Task, reducef func(string, []string) string) {
	var kva []KeyValue
	// 1. Read all mr-map-* files.
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		matched, _ := regexp.Match(`mr-map-*`, []byte(file.Name()))
		if !matched {
			continue
		}
		filename := file.Name()
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		// 2. Get total reduce number from task.Content
		nReduce, _ := strconv.Atoi(task.Content)

		// 3. For every keyValue, if {ihash(key) % totalReduceNumber} == {this worker's reducer number}, then it means this key is for this worker to reduce it.
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if ihash(kv.Key)%nReduce == int(task.TaskId) {
				kva = append(kva, kv)
			}
		}
	}

	// 4. Sort all keys
	sort.Sort(ByKey(kva))

	// 5. create temp file, and rename it atomically later.
	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	tmpfile, _ := ioutil.TempFile(".", oname)
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
		// 6. Write reduce results to file.
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	// 7. atomically rename tmpfile
	os.Rename(tmpfile.Name(), oname)

	// 8. notice master that this task is done.
	NoticeMaster(task.TaskId, task.TaskType)
}

// send Notice to master, to tell master that this task is finished.
func NoticeMaster(taskId int32, taskType int32) {
	req := &NoticeRequest{
		TaskId:   taskId,
		TaskType: taskType,
	}
	resp := &NoticeResponse{}
	if call("Master.Notice", req, resp) == true {
		return
	}
	// if rpc call failed, meaning master is closed, meaning job is finished.
	return
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
