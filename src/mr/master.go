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
	"os/exec"
	"regexp"
	"errors"
	"time"
)

type Master struct {
	mutex sync.Mutex
	completedTask int
	reduceInit bool
	nReduce int
	nMap int
	workerNum int
	done bool
	suffix string
	taskInfo []TaskInfo
	aliveworkers int
}

const (
	TaskFailed = -1
	TaskNotAssigned = 0
	TaskRunning = 1
	TaskCompleted = 2
)

// confirm that the master routine is still running
func (m *Master) IsAlive(args *ExampleArgs, reply *AliveReply) error {
	reply.IsAlive = true
	return nil
}

func (m *Master) schedule() {
	for !m.Done() {
		for i, task := range m.taskInfo {
			m.mutex.Lock()
			curTime := time.Now()
			switch task.Status {
			case TaskFailed: {
				m.taskInfo[i].Status = TaskNotAssigned
				m.mutex.Unlock()
				break
			}
			case TaskRunning: {
				if int(curTime.Sub(task.StartTime).Seconds()) > 30 {
					m.taskInfo[i].Status = TaskNotAssigned
					m.aliveworkers--
					m.mutex.Unlock()
					break
				}
				m.mutex.Unlock()
				break
			}
			case TaskNotAssigned: { 
				m.mutex.Unlock() 
				break
			}
			case TaskCompleted: {
				m.mutex.Unlock() 
				break
			}
			}
		}
		if m.completedTask == m.nMap && !m.reduceInit {
			m.mutex.Lock()
			m.reduceInit = true
			m.InitReduce()
			m.mutex.Unlock()
		} else if m.completedTask ==  m.nMap + m.nReduce {
			m.done = true
		}
		time.Sleep(1e10)
	}
}

func (m *Master) AssignTask(args ExampleArgs, reply *TaskAssign) error {
	for i, task := range m.taskInfo {
		m.mutex.Lock()
		if task.Status == TaskNotAssigned {
			reply.MapNum = task.MapNum
			reply.ReduceNum = task.ReduceNum
			reply.Suffix = task.Suffix
			m.taskInfo[i].Status = TaskRunning
			m.taskInfo[i].StartTime = time.Now()
			m.mutex.Unlock()
			return nil
		}
		m.mutex.Unlock()
	}
	reply.MapNum = -1
	reply.ReduceNum = -1
	return nil
}

func (m *Master) TaskStatusUpdate(args *TaskInfo, reply *ExampleReply) error {
	if args.Status == TaskCompleted {
		m.mutex.Lock()
		m.completedTask++
		m.mutex.Unlock()
	}
	for i, task := range m.taskInfo {
		m.mutex.Lock()
		if args.MapNum != -1 && task.MapNum == args.MapNum {
			m.taskInfo[i].Status = args.Status
			m.mutex.Unlock()
			return nil
		} else if args.ReduceNum != -1 && task.ReduceNum == args.ReduceNum {
			m.taskInfo[i].Status = args.Status
			m.mutex.Unlock()
			return nil
		}
		m.mutex.Unlock()
	}
	return errors.New("Inexist task!\n")
}

func (m *Master) InitWorker(args *Workers, reply *Workers) error {
	reply.NMap = m.nMap
	reply.NReduce = m.nReduce
	reply.id = m.workerNum
	m.workerNum++
	m.aliveworkers++
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
	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		reduceInit: false,
		mutex: sync.Mutex{}, 
		completedTask: 0, 
		nReduce: nReduce, 
		nMap: 1, 
		workerNum: 0,
		done: false, 
		suffix: getSuffix(files[0]), 
		taskInfo: []TaskInfo{}, 
	}

	m.InitMap(files)

	//fmt.Printf("nmap: %v, nreduce: %v\n", m.nMap, m.nReduce)

	go m.schedule()
	m.server()

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

func (m * Master) InitMap(files [] string) {
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
		cmd1 := exec.Command("cat", fs...)
		cmd2 := exec.Command("split", "-C", string(dataSegment), "tempfile" + m.suffix, "map-")

		tempfile := create("tempfile" + m.suffix)

		cmd3 := exec.Command("rm", "tempfile" + m.suffix)
		defer cmd3.Start()
		defer tempfile.Close()

		cmd1.Stdout = tempfile
		err := cmd1.Run()
		if err != nil {
			log.Fatalf("run command %v failed", cmd1.Path)
			os.Exit(1)
		}

		err = cmd2.Run()
		if err != nil {
			log.Fatalf("run command %v failed", cmd2.Path)
			os.Exit(1)
		}

		rd := readdir(".")

		for  _, f := range rd {
			match, _ := regexp.MatchString("map-", f.Name())

			if match == true {
				os.Rename(f.Name(), fmt.Sprintf("map-%v" + m.suffix, m.nMap))
				m.nMap++
			}
		}

	} else {
		for _, filename := range fs {

			buffer, err := ioutil.ReadFile(filename)

			if err != nil {
				log.Fatalf("cannot read file %v", filename)
			}

			// err = ioutil.WriteFile(fmt.Sprintf("map-%v" + m.suffix, m.nMap), buffer, 0777)
			err = ioutil.WriteFile(fmt.Sprintf("map-%v", m.nMap), buffer, 0777)

			if err != nil {
				log.Fatalf("cannot write file %v", fmt.Sprintf("map-%v", m.nMap))
			}

			m.taskInfo = append(m.taskInfo, TaskInfo {
				MapNum: m.nMap, ReduceNum: -1, Suffix: m.suffix, 
				Status: TaskNotAssigned,
			})
			m.nMap++
		}
	}

	m.nMap--
}

func (m * Master) InitReduce() {
	for i := 1; i <= m.nReduce; i++ {
		m.taskInfo = append(m.taskInfo, TaskInfo {
			MapNum: -1, ReduceNum: i, Suffix: m.suffix, 
			Status: TaskNotAssigned,
		})
	}
}