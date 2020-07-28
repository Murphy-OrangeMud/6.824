package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"io/ioutil"
	"regexp"
	//"math"
	//"time"
)

type Master struct {
	// Your definitions here.
	mutex sync.Mutex
	completedMapTask    int
	completedReduceTask int
	nReduce int
	nMap int
	suffix string
	mapTaskInfo []TaskInfo
	reduceTaskInfo []TaskInfo
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// confirm that the master routine is still running
func (m *Master) IsAlive(args *ExampleArgs, reply *AliveReply) error {
	reply.IsAlive = true
	return nil
}

//AssignTask : assign a task to the worker
func (m *Master) AssignTask(args *ExampleArgs, reply *TaskAssign) error {
	fmt.Println("assigning task...")
	// map task
	if args.X == 0 {
		for _, task := range m.mapTaskInfo {
			m.mutex.Lock()
			if task.Status == 0 {
				reply.MapNum = task.MapNum
				task.Status = 1
				m.mutex.Unlock()
				return nil
			}
			m.mutex.Unlock()
		}
		if m.completedMapTask == m.nMap {
			reply.MapNum = -2
		}
		fmt.Printf("%v %v\n", reply.MapNum, reply.ReduceNum)
	} else { // args.X == 1, reduce task
		for _, task := range m.reduceTaskInfo {
			m.mutex.Lock()
			if task.Status == 0 {
				reply.ReduceNum = task.ReduceNum
				task.Status = 1
				m.mutex.Unlock()
				return nil
			}
			m.mutex.Unlock()
		}
		if m.completedMapTask == m.nMap {
			reply.MapNum = -2
		}
	}
	return nil
}

func (m *Master) TaskStatusUpdate(args *TaskInfo, reply *ExampleReply) error {
	fmt.Printf("update task for map: %v and reduce %v\n", args.MapNum, args.ReduceNum)
	if args.MapNum != -1 {
		for _, task := range m.mapTaskInfo {
			m.mutex.Lock()
			if task.MapNum == args.MapNum {
				task.Status = args.Status
				m.mutex.Unlock()
				break
			}
			m.mutex.Unlock()
		}
		if args.Status == 2 {
			m.mutex.Lock()
			m.completedMapTask++
			m.mutex.Unlock()
		}
	} else {
		for _, task := range m.reduceTaskInfo {
			m.mutex.Lock()
			if task.ReduceNum == args.ReduceNum {
				task.Status = args.Status
				m.mutex.Unlock()
				break
			}
			m.mutex.Unlock()
		}
		if args.Status == 2 {
			m.mutex.Lock()
			m.completedMapTask++
			m.mutex.Unlock()
		}
	}
	return nil
}

func (m *Master) InitWorker(args *ExampleArgs, reply *Info) error {
	reply.NReduce = m.nReduce
	reply.NMap = m.nMap
	reply.Suffix = m.suffix
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.completedMapTask == m.nMap && m.completedReduceTask == m.nReduce {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{mutex: sync.Mutex{}, completedMapTask: 0, completedReduceTask: 0, nReduce: nReduce, nMap: 1, suffix: getSuffix(files[0]), mapTaskInfo: []TaskInfo{}, reduceTaskInfo: []TaskInfo{}, }

	// Your code here.
	// read in data and split

	// deal with regular expression
	fs := []string{}
	
	for _, path := range files {
		dir, name := parsePath(path)

		rd, err := ioutil.ReadDir(dir)

		if err != nil {
			log.Fatalf("parse directory failed")
		}

		for _, f := range rd {
			match, _ := regexp.MatchString(name, f.Name())

			if match == true && f.IsDir() {
				log.Fatalf("incorrect paramter: %v is a directory", f.Name())
				os.Exit(1)
			}
			if match == true {
				fs = append(fs, dir + "/" + f.Name())
			}
		}
	}

	const dataSegment = (1 << 26) // 64kb due to the paper

	var sumSize int64
	var maxSize int64
	sumSize = 0
	maxSize = dataSegment
	for _, filename := range fs {
		fi, err := os.Stat(filename)
		
		if err != nil {
			log.Fatalf("cannot read metadata of %v", filename)
			os.Exit(1)
		}
		sumSize += fi.Size()
		maxSize = Max(maxSize, fi.Size())
	}

	if maxSize > dataSegment {
		buffer := make([]byte, dataSegment)

		wFile, err := os.Create(fmt.Sprintf("map-%v%v", m.nMap, m.suffix))
		if err != nil {
			log.Fatalf("cannot create %v", fmt.Sprintf("map-%v%v", m.nMap, m.suffix))
		}
		m.mapTaskInfo = append(m.mapTaskInfo, TaskInfo {
			-1, m.nMap, 0, 0,
		})
		m.nMap++
		var n int

		for i := 0; i < len(fs); {

			filename := fs[i]
			fmt.Println(filename)

			file, err := os.Open(filename)
			
			if err != nil {
				log.Fatalf("cannot open file %v", filename)
				os.Exit(1)
			}
			
			n, err = file.Read(buffer)
			wFile.Write(buffer)
			
			if n == len(buffer) {
				buffer = make([]byte, dataSegment)
				wFile.Close()
				wFile, err = os.Create(fmt.Sprintf("map-%v%v", m.nMap, m.suffix))
				if err != nil {
					log.Fatalf("cannot create file map-%v%v", m.nMap, m.suffix)
				}
				m.mapTaskInfo = append(m.mapTaskInfo, TaskInfo {
					MapNum: m.nMap, ReduceNum: -1, Status: 0, Pid: 0,
				})
				m.nMap++
			} else {
				buffer = buffer[n:]
				file.Close()
				i++
			}
		}
	} else {
		for _, filename := range fs {

			buffer, err := ioutil.ReadFile(filename)

			if err != nil {
				log.Fatalf("cannot read file %v", filename)
			}

			err = ioutil.WriteFile(fmt.Sprintf("map-%v" + m.suffix, m.nMap), buffer, 0777)

			if err != nil {
				log.Fatalf("cannot write file %v", fmt.Sprintf("map-%v" + m.suffix, m.nMap))
			}

			m.mapTaskInfo = append(m.mapTaskInfo, TaskInfo {
				MapNum: m.nMap, ReduceNum: -1, Status: 0, Pid: 0,
			})
			m.nMap++
		}
	}

	m.nMap--

	// init reduce tasks
	for i := 1; i <= nReduce; i++ {
		m.reduceTaskInfo = append(m.reduceTaskInfo, TaskInfo {
			MapNum: -1, ReduceNum: i, Status: 0, Pid: 0,
		})
	}
	
	m.completedMapTask = 0
	m.completedReduceTask = 0

	// fmt.Print(m.completedMapTask, m.nMap, m.completedReduceTask, m.nReduce)

	m.server()

	l := sync.Mutex{}
	c := sync.NewCond(&l)
	
	/*
	c.L.Lock()
	c.Broadcast()
	c.L.Unlock()
	*/

	for m.completedMapTask < m.nMap {}

	fmt.Println("begin reduce task...")

	c.L.Lock()
	c.Broadcast()
	c.L.Unlock()

	for m.completedReduceTask < m.nReduce {}

	return &m
}

func getSuffix(filename string) string {
	idx := -1
	for i := 0; i < len(filename); i++ {
		if filename[i] == '.' {
			idx = i
			break
		}
	}
	if idx != -1 {
		return filename[idx:]
	}
	return ""
}

func Max(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func parsePath(path string) (string, string){
	i := len(path) - 1
	for ; i >= 0; i-- {
		if path[i] == '/' {
			break
		}
	}
	
	var dir string
	var name string

	if i > 0 {
		dir = path[:i]
	} else if i == 0 {
		dir = "/"
	} else {
		dir = "../main"
	}

	name = path[i + 1:]

	return dir, name
	
}