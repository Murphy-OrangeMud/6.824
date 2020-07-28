package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type AliveReply struct {
	IsAlive bool
}

type TaskAssign struct {
	MapNum int
	ReduceNum int
}

type Info struct {
	NMap int
	NReduce int
	Suffix string
}

type TaskInfo struct {
	MapNum int
	ReduceNum int
	Status int  // 0: unassigned, 1: in progress, 2: completed
	Pid int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
