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
type TaskApply struct{
	ServerState int
}

type TaskReply struct{
	TaskType string // value : mapper reducer exit
	TaskNumber int // task number assigned by master
	InputFileName string // input file name
	NReduce int // number of reducer
	NMap int // number of mapper
	Finished bool // if all task done
}

type TaskHandinApply struct {
	TaskType string // value : map or reducet
	TaskNumber int // task number 
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
