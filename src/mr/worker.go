package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "sort"
import "encoding/json"
import "strconv"


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	applyArgs := TaskApply{}
	applyResult :=TaskReply{}
	for { // try to get a task
		res := call("Master.GetTask",&applyArgs,&applyResult)
		if(!res) { //something wrong in rpc  
			log.Fatalf("Rpc call failed")
			break
		}
		if applyResult.TaskType == "wait" { //wait until map tasks finished
			time.Sleep(time.Second)
		} else {
			break
		}

	}
	if applyResult.TaskType == "map" {
		HandleMap(applyResult,mapf)
	} else if applyResult.TaskType == "reduce" {
		HandleReduce(applyResult,reducef)
	}
}

func HandleMap(taskReply TaskReply,mapf func(string, string) []KeyValue) {
	filename := taskReply.InputFileName
	taskNumber := taskReply.TaskNumber
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	// write every kv to target intermediate files
	// create temarary files array ,index is the number of reducer
	var tmpFileArr []*os.File // 数组里面存的是struct pointer
	for i:=0; i<taskReply.NReduce; i++{
		//mr-mapTaskNum-reduceTaskNum
		tmpfileName:="mr-"+strconv.Itoa(taskNumber)+"-"+strconv.Itoa(i)
		tmpfile , err := ioutil.TempFile("",tmpfileName)
		if(err!=nil) {
			log.Fatalf("cannot create tempfile %v", tmpfileName)
			return
		}
		tmpFileArr=append(tmpFileArr,tmpfile)
	}
	for _ , kv :=  range kva {
		p := ihash(kv.Key)%taskReply.NReduce
		enc:=json.NewEncoder(tmpFileArr[p])
		err:=enc.Encode(kv)
		if err!=nil {
			log.Fatalf("encode kv %v failed",kv)
		}
	}
	// rename temparary files atomicly
	for i:=0;i<taskReply.NReduce;i++ {
		// file name:mr-mapTaskNum-reduceTaskNum
		os.Rename("mr-"+strconv.Itoa(taskNumber)+"-"+strconv.Itoa(i),"mr-"+strconv.Itoa(taskNumber)+"-"+strconv.Itoa(i))
		//file.close() ?	
	}
	// send done messages to master
	args := TaskHandinApply{}
	reply := ""
	args.TaskType = "map"
	args.TaskNumber = taskNumber
	call("Master.HandinTask",&args,&reply)
}

func HandleReduce(taskReply TaskReply,reducef func(string, []string) string) {
	nReduce := taskReply.NReduce
	reduceNumber := taskReply.TaskNumber
	intermediateKV := []KeyValue{}
	//read nMap input files and append to intermediateKV array, how to handle memory overflow?
	for i:=0; i<nReduce; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceNumber)
		file, err := os.Open(filename)
		if err!=nil{
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediateKV = append(intermediateKV, kv)
		}
		file.Close();
	}
	// sort by key
	sort.Sort(ByKey(intermediateKV))
	// out put file
	ofilename := "mr-out-"+strconv.Itoa(reduceNumber)
	// create a temp file
	tofile,err := ioutil.TempFile("",ofilename)
	if err != nil {
		log.Fatalf("reducer :%v ,output temp file create failed!",reduceNumber)
	}
	values := []string{} // how to declare an empty string array? 
	for i := 0; i<len(intermediateKV); i++ {
		if i>0&&intermediateKV[i-1].Key != intermediateKV[i].Key {
			//call reducer
			output := reducef(intermediateKV[i-1].Key,values)
			//write to file
			fmt.Fprintf(tofile,"%v %v\n",intermediateKV[i-1].Key,output)
			values = []string{} // make values array empty
		}
		values=append(values,intermediateKV[i].Value)
	}
	if len(values)>0 {
		//call reducer
		lastKey := intermediateKV[len(intermediateKV)-1].Key 
		output := reducef(lastKey,values)
		//write to file
		fmt.Fprintf(tofile,"%v %v\n",lastKey,output)
		values = []string{} // make values array empty
	}
	// atomatic rename file
	os.Rename(ofilename,ofilename)
	// submit result to master
	args := TaskHandinApply{}
	reply := ""
	args.TaskType = "reduce"
	args.TaskNumber = reduceNumber
	call("Master.HandinTask",&args,&reply)
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
