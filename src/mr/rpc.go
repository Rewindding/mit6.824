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
	serverState int
	
}

type TaskReply struct{
	taskType string // value : mapper reducer exit
	taskNumber int // task number assigned by master
	inputFileName string // input file name
	nReduce int // number of reducer
	nMap int // number of mapper
}

type TaskHandinApply struct {
	taskType string // value : map or reducet
	taskNumber int // task number 
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
