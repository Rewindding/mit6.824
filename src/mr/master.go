package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type TaskState struct {
	state int // task state 0:idel 1:in progress 2:done
	startTime time.Time // task started time 
}
// return true if the task timeout 
func (t *TaskState) isTimeOut() bool { 
	// represents the elapsed time between two instants as an int64 nanosecond count.
	elapsed := time.Now().Sub(t.startTime)
	return elapsed>(10*time.Second)
}
// set task state = 1 ,start time = now 
func (t *TaskState) setInProgress() {
	t.state = 1;
	t.startTime = time.Now()
}
func (t *TaskState) excutable() bool {
	return t.state==1||(t.state==2&&t.isTimeOut())
}
type Master struct {
	// Your definitions here.
	nMap int // number of map task
	nReduce int // number of reduce task
	inputFiles []string // input file names
	mapTaskState []TaskState 
	reduceTaskState []TaskState
	finishedMapTaskCnt int // number of finished map task
	finishedReduceTaskCnt int // number of finished reduce task
	isDone bool // is the task fully done
	mu sync.Mutex // mutex
}
// return true if all map tasks finished
func (m* Master) isMapTaskFinished() bool {
	return m.finishedMapTaskCnt == m.nMap
}
func (m* Master) setFinished(TaskType string, taskNumber int) bool { 
	if TaskType == "map" {
		if m.mapTaskState[taskNumber].state == 2 {
			//already finished
			return false
		}
		m.mapTaskState[taskNumber].state = 2
		m.finishedMapTaskCnt++
	} else if TaskType == "reduce" {
		if m.reduceTaskState[taskNumber].state == 2 {
			//already finished
			return false
		}
		m.reduceTaskState[taskNumber].state = 2
		m.finishedReduceTaskCnt++
	} else {
		return false
	}
	return true
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args *TaskApply, reply *TaskReply) error {

	// this function will be called concurrently 
	m.mu.Lock()
	defer m.mu.Unlock()
	// assemble map task first
	mapTaskNumber := -1
	for i:=0;i<m.nMap;i++{
		if m.mapTaskState[i].excutable() {// task not started or not finished and timeout
			mapTaskNumber = i	
			break;
		}
	}
	if mapTaskNumber!=-1 { // assign a map task and return
		reply.TaskType = "map"
		reply.TaskNumber = mapTaskNumber
		reply.InputFileName = m.inputFiles[mapTaskNumber]
		reply.NMap = m.nMap
		reply.NReduce = m.nReduce
		//set start time and state
		m.mapTaskState[mapTaskNumber].setInProgress()
		return nil
	}
	if(!m.isMapTaskFinished()) {
		reply.TaskType="wait"
		return nil
	}
	//if map task fully done , assemble reduce task 
	reduceTaskNumber := -1
	for i:=0; i<m.nReduce; i++ {
		if m.reduceTaskState[i].excutable() {// task not started or not finished and timeout
			reduceTaskNumber = i
			break;
		}
	}
	//no reduce task
	if reduceTaskNumber==-1 {
		// log.error()
		return nil
	}
	// assign a reduce task
	reply.TaskType = "reduce"
	reply.TaskNumber = reduceTaskNumber
	reply.NMap = m.nMap
	reply.NReduce = m.nReduce
	m.reduceTaskState[reduceTaskNumber].setInProgress()
	return nil
}

// RPC : work call this when a task is done
func (m* Master) HandinTask(args * TaskHandinApply,reply* string) error {
	// this function will be called concurrently 
	m.mu.Lock()
	defer m.mu.Unlock()
	res := m.setFinished(args.TaskType,args.TaskNumber)
	if(!res){
		// return error
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	// Your code here.
	return m.finishedMapTaskCnt==m.nMap&&m.finishedReduceTaskCnt==m.nReduce
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nMap=len(files)
	m.nReduce=nReduce
	m.inputFiles=files // cppy array
	m.mapTaskState = make([]TaskState,m.nMap,m.nMap)
	m.reduceTaskState = make([]TaskState,nReduce,nReduce)
	m.isDone = false

	m.server()
	return &m
}
