package mr

import (
	//"io/ioutil"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"os/exec"

	//"sync"
	"encoding/json"
	//"time"
	"sort"
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

type Workers struct {
	id      int
	mapf    func(filename string, content string) []KeyValue
	reducef func(key string, values []string) string
	NMap    int
	NReduce int
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

func (w *Workers) doMapTask(task TaskAssign) {
	filename := fmt.Sprintf("map-%v", task.MapNum)
	file := open(filename)
	content := readall(file)

	kva := w.mapf(filename, string(content))

	tempJsonfiles := make(map[string]*os.File)

	for i := 1; i <= w.NReduce; i++ {
		jsonfileName := fmt.Sprintf("mr-%v-%v.json", task.MapNum, i)

		tempJsonfile := tempfile(".", jsonfileName)
		defer tempJsonfile.Close()
		tempJsonfiles[jsonfileName] = tempJsonfile
	}

	encs := make(map[string]*json.Encoder)

	for i := 1; i <= w.NMap; i++ {
		for j := 1; j <= w.NReduce; j++ {
			tempjsonfile := tempJsonfiles[fmt.Sprintf("mr-%v-%v.json", i, j)]

			if tempjsonfile == nil {
				encs[fmt.Sprintf("mr-%v-%v.json", i, j)] = nil
				continue
			}

			encs[fmt.Sprintf("mr-%v-%v.json", i, j)] = json.NewEncoder(tempjsonfile)
		}
	}

	for _, kv := range kva {
		reduceNum := ihash(kv.Key) % w.NReduce + 1
		name := fmt.Sprintf("mr-%v-%v.json", task.MapNum, reduceNum)
		tempjsonfile := tempJsonfiles[name]

		if tempjsonfile == nil {
			continue
		}

		enc := encs[name]

		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("encode failed in %v", filename)
			defer exec.Command("rm", tempjsonfile.Name()).Run()
			defer tempjsonfile.Close()

			call("Master.TaskStatusUpdate", &TaskInfo{
				MapNum:    task.MapNum,
				ReduceNum: task.ReduceNum,
				Status:    TaskNotAssigned,
				WorkerId:  w.id,
			}, &ExampleReply{})

			return
		}
	}

	for filename, file := range tempJsonfiles {
		if file == nil {
			continue
		}
		os.Rename(file.Name(), filename)
	}

	call("Master.TaskStatusUpdate", &TaskInfo{
		MapNum:    task.MapNum,
		ReduceNum: task.ReduceNum,
		Status:    TaskCompleted,
		WorkerId:  w.id,
	}, &ExampleReply{})
}

func (w *Workers) doReduceTask(task TaskAssign) {
	kva := []KeyValue{}
	for i := 1; i < w.NMap; i++ {
		filename := fmt.Sprintf("mr-%v-%v.json", i, task.ReduceNum)

		file := open(filename)

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	//reduceName := //fmt.Sprintf("mr-out-%v" + task.Suffix, task.ReduceNum)
	reduceName := fmt.Sprintf("mr-out-%v", task.ReduceNum)
	reduceFile := create(reduceName)

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
		output := w.reducef(kva[i].Key, values)
		fmt.Fprintf(reduceFile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	call("Master.TaskStatusUpdate", &TaskInfo{
		MapNum:    task.MapNum,
		ReduceNum: task.ReduceNum,
		Status:    TaskCompleted,
		WorkerId:  w.id,
	}, &ExampleReply{})
}

func (w *Workers) doTask() error {
	task := TaskAssign{}
	if call("Master.AssignTask", ExampleArgs{w.id}, &task) == false {
		w.isFinished()
	} else {
		if task.MapNum != -1 {
			w.doMapTask(task)
		} else if task.ReduceNum != -1 {
			w.doReduceTask(task)
		} else {
			w.isFinished()
		}
	}
	return nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := Workers{
		mapf:    mapf,
		reducef: reducef,
	}

	call("Master.InitWorker", &w, &w)

	r := AliveReply{IsAlive: false}
	call("Master.IsAlive", &ExampleArgs{}, &r)

	for r.IsAlive {
		w.doTask()
		r.IsAlive = false
		call("Master.IsAlive", &ExampleArgs{}, &r)
	}
}

func (w *Workers) isFinished() {
	reply := AliveReply{IsAlive: false}

	if call("Master.IsAlive", &ExampleArgs{}, &reply) == false || reply.IsAlive == false {
		os.Exit(0)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, task interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, task)

	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
