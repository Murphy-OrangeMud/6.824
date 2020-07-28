package mr

import (
	"io/ioutil"
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"os"
	//"sync"
	"encoding/json"
	"time"
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

	info := Info{}

	err := call("Master.InitWorker", &ExampleArgs{}, &info)

	if err == false {
		log.Fatalf("initialization failed for worker")
		os.Exit(1)
	}

	/*
	cond.L.Lock()
	cond.Wait()
	cond.L.Unlock()
	*/

	for {
		fmt.Println("another loop...")

		reply := TaskAssign{-1, -1}

		err = call("Master.AssignTask", &ExampleArgs{0,}, &reply)

		if err == false {
			log.Fatalf("assign map task failed")
			os.Exit(1)
		} else {
			fmt.Println(reply.MapNum)
			if reply.MapNum >= 0 {
				filename := fmt.Sprintf("map-%v" + info.Suffix, reply.MapNum)
				file, err := os.Open(fmt.Sprintf("map-%v" + info.Suffix, reply.MapNum))

				if err != nil {
					log.Fatalf("cannot open %v", filename)
					call("Master.TaskStatusUpdate", &TaskInfo {
						reply.MapNum,
						reply.ReduceNum,
						0,
						0,
					}, &ExampleReply{})
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
					call("Master.TaskStatusUpdate", &TaskInfo {
						reply.MapNum,
						reply.ReduceNum,
						0,
						0,
					}, &ExampleReply{})
				}
				file.Close()
				kva := mapf(filename, string(content))

				tempJsonfiles := make(map[string]*os.File)

				for i := 1; i <= info.NReduce; i++ {
					jsonfileName := fmt.Sprintf("mr-%v-%v.json", reply.MapNum, i)
					
					if Exist(jsonfileName) {
						tempJsonfiles[jsonfileName] = nil
						continue
					} 

					tempJsonfile, err := ioutil.TempFile(".", fmt.Sprintf("mr-%v-%v*.json", reply.MapNum, i))
					
					if err != nil {
						log.Fatalf("cannot create tempfile %v", jsonfileName)
						call("Master.TaskStatusUpdate", &TaskInfo {
							reply.MapNum,
							reply.ReduceNum,
							0,
							0,
						}, &ExampleReply{})
					}

					tempJsonfiles[jsonfileName] = tempJsonfile
				}

				encs := make(map[string]*json.Encoder)

				for i := 1; i <= info.NMap; i++ {
					for j := 1; j <= info.NReduce; j++ {
						tempjsonfile := tempJsonfiles[fmt.Sprintf("mr-%v-%v.json", i, j)]

						if tempjsonfile == nil {
							encs[fmt.Sprintf("mr-%v-%v.json", i, j)] = nil
							continue
						}

						encs[fmt.Sprintf("mr-%v-%v.json", i, j)] = json.NewEncoder(tempjsonfile)
					}
				}

				for _, kv := range kva {
					reduceNum := ihash(kv.Key) % info.NReduce + 1
					name := fmt.Sprintf("mr-%v-%v.json", reply.MapNum, reduceNum)
					tempjsonfile := tempJsonfiles[name]

					if tempjsonfile == nil {
						continue
					}

					enc := encs[name]

					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("encode failed in %v", filename)
						tempjsonfile.Close()
						call("Master.TaskStatusUpdate", &TaskInfo {
							reply.MapNum,
							reply.ReduceNum,
							0,
							0,
						}, &ExampleReply{})
					}
				}

				for filename, file := range tempJsonfiles {
					if file == nil {
						continue
					}
					err := os.Rename(file.Name(), filename)
					if err != nil {
						log.Fatalf("rename failed in %v", filename)
						file.Close()
						// to be revised
						call("Master.TaskStatusUpdate", &TaskInfo {
							reply.MapNum,
							reply.ReduceNum,
							0,
							0,
						}, &ExampleReply{})
					}
				}

				call("Master.TaskStatusUpdate", &TaskInfo {
					reply.MapNum,
					reply.ReduceNum,
					2,
					0,
				}, &ExampleReply{})
			}
		}

		if reply.MapNum == -2 {
			break
		}
	
	}

	for {
		reply := TaskAssign{-1, -1}

		for {
			err := call("Master.AssignTask", &ExampleArgs{1}, &reply)
			
			if err == false {
				log.Fatalf("assign reduce task failed for worker")
				os.Exit(1)
			}
			if reply.ReduceNum != -1 {
				break
			}
			time.Sleep(100)
		}

		if reply.ReduceNum > 0 {
			for i := 1; i <= info.NMap; i++ {
				filename := fmt.Sprintf("mr-%v-%v.json", i, reply.ReduceNum)
				
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open file %v", filename)
					call("Master.TaskStatusUpdate", &TaskInfo {
						reply.MapNum,
						reply.ReduceNum,
						0,
						0,
					}, &ExampleReply{})
				}

				dec := json.NewDecoder(file)
				
				kva := []KeyValue{}

				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}

				sort.Sort(ByKey(kva))

				reduceName := fmt.Sprintf("mr-out-%v", reply.ReduceNum)
				reduceFile, err := os.Create(reduceName)

				if err != nil {
					log.Fatalf("cannot create %v", reduceName)
					call("Master.TaskStatusUpdate", &TaskInfo {
						reply.MapNum,
						reply.ReduceNum,
						0,
						0,
					}, &ExampleReply{})
				}

				for key, value := range kva {
					fmt.Fprintf(reduceFile, "%v %v\n", key, value)
				}
				
				call("Master.TaskStatusUpdate", &TaskInfo {
					reply.MapNum,
					reply.ReduceNum,
					2,
					0,
				}, &ExampleReply{})
			}
		}

		if reply.ReduceNum == -2 {
			break
		}
	}
	
	// uncomment to send the Example RPC to the master.
	// CallExample()

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

func isFinished() {
	reply := AliveReply{false}

	call("Master.IsAlive", &ExampleArgs{}, &reply)

	if reply.IsAlive == false {
		os.Exit(0)
	}
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

func Exist(filename string) bool {
    _, err := os.Stat(filename)
    return err == nil || os.IsExist(err)
}
