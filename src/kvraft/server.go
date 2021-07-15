package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Debug = 0
	// raft timeout 的原因可能是网络失败，可能是当前服务器已经不再是leader了,原因不确定
	RaftTimeOut = time.Millisecond * 300
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpName string
	Key    string
	Value  string
	// request id used to identify duplicate ,format : clientId:reqNum
	RequestId string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// in-memory map to store k-v data?
	kvStore map[string]string
	// store longIndex:ch,use to communication between raft and application
	channelMap sync.Map
	excutedReq sync.Map
}

// TO DO
// 0.唯一标识每一个request,防止无效的retry，也防止返回，先要唯一标识每一个client，然后给每一个request一个唯一的request id
// 1.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// receive a client get request
	// reads are handled by leader only
	reqId := fmt.Sprintf("%v:%v", args.ClientId, args.RequestId)
	DPrintf("Get method called")
	op := Op{
		OpName:    "Get",
		Key:       args.Key,
		RequestId: reqId,
	}
	logIndex, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		DPrintf("server:%v not leader\n", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	// wait this log get replicated to majoraty server and committed
	ch := make(chan Op)

	_, loaded := kv.channelMap.LoadOrStore(logIndex, ch)
	if loaded {
		log.Printf("unhandled problem: duplicate log index:%v", logIndex)
		// panic("unhandled problem: duplicate log index")
		// TODO it's said maybe same log index appear more than once!?
	}
	defer kv.channelMap.Delete(logIndex)
	select {
	case currentOp := <-ch:
		{
			// judge if the server is still leader
			if currentOp.RequestId != reqId {
				// excution failed
				DPrintf("get error:%v", ErrWrongLeader)
				reply.Err = ErrWrongLeader
				return
			}
			reply.Err = OK
			reply.Value = currentOp.Value
		}
	case <-time.After(RaftTimeOut):
		{
			// log.Printf("log idx:%v timeout", logIndex)
			reply.Err = ErrTimeout
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reqId := fmt.Sprintf("%v:%v", args.ClientId, args.RequestId)
	if _, ok := kv.excutedReq.Load(reqId); ok {
		// duplicate return directly
		reply.Err = OK
		return
	}
	op := Op{
		OpName:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		RequestId: reqId,
	}
	logIndex, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("server:%v not leader\n", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("put append called,handle by leader%v at term %v", kv.me, term)
	// wait this log get replicated to majority server and committed
	ch := make(chan Op)
	_, loaded := kv.channelMap.LoadOrStore(logIndex, ch)
	if loaded {
		log.Printf("unhandled problem: duplicate log index:%v", logIndex)
		// panic("unhandled problem: duplicate log index")
		// TODO it's said maybe same log index appear more than once!?
	}
	defer kv.channelMap.Delete(logIndex)
	// 搞一个select，然后设置超时channel
	select {
	case cmd := <-ch:
		{
			// 取出对应的log reqId，看一下是不是相等.
			if cmd.RequestId != reqId {
				// excution failed
				DPrintf("put append error:%v", ErrWrongLeader)
				reply.Err = ErrWrongLeader
				return
			}
			// return result
			reply.Err = OK
		}
	case <-time.After(RaftTimeOut):
		{
			// log.Printf("log idx:%v timeout", logIndex)
			reply.Err = ErrTimeout
			return
		}

	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	// create condi variable,used for handle request
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	// 从persister恢复
	kv.readPersist(persister.ReadSnapshot())
	// You may need initialization code here.
	// should each server need to start a go routine and listen to the applyCh??...
	go func(kv *KVServer) {
		for !kv.killed() {
			// read from apply msg channel
			// log.Printf("server:%v, read from ch blocked",me)
			logEntry := <-kv.applyCh
			// log.Printf("server:%v, unblocked",me)
			if !logEntry.CommandValid { // snapshot command
				//log.Printf("kv app receive snapshot")
				snapshotData, ok := logEntry.Command.([]byte)
				if !ok {
					panic("err")
				}
				kv.readPersist(snapshotData)
				continue
			}
			command, ok := logEntry.Command.(Op)
			if !ok { // type assertion failed
				DPrintf("read a dummy log:%v", logEntry.Command.(string))
				continue
			}
			// Get无论如何都直接执行
			if command.OpName == "Get" {
				command.Value = kv.kvStore[command.Key]
			}
			// 存在这样的情况，已经被执行了但是因为reponse丢了，重试请求，重新被append到log里面，这里也要判断如果已经被执行了就不再执行
			_, loaded := kv.excutedReq.LoadOrStore(command.RequestId, true)
			if !loaded {
				// apply this command to kv store
				if command.OpName == "Put" {
					kv.kvStore[command.Key] = command.Value
				} else if command.OpName == "Append" {
					kv.kvStore[command.Key] += command.Value
				}
			}
			// 把op放到对应channel里面去
			ch, ok := kv.channelMap.Load(logEntry.CommandIndex)
			if ok { // only leader need to do this
				// log.Printf("notify idx:%v", logEntry.CommandIndex)
				ch.(chan Op) <- command
			}
			// 在这里make snapshot
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				// 获取kv存储的数据，
				appData := kv.getSnapshot(logEntry.CommandIndex - 1)
				// command index start from 1
				log.Printf("make app snapshot,lastApplied:%v", logEntry.CommandIndex-1)
				go kv.rf.MakeSnapshot(appData, logEntry.CommandIndex-1, logEntry.LogTerm)
				// log.Printf("server:%v,before size:%v,after size:%v,max:%v",me,beforeSize,kv.persister.RaftStateSize(),kv.maxraftstate)
			}
		}
	}(kv)
	time.Sleep(100 * time.Millisecond)
	return kv
}

func (kv *KVServer) readPersist(data []byte) {
	kv.kvStore = make(map[string]string)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.kvStore) != nil {
		kv.kvStore = make(map[string]string)
	}
	if kv.kvStore == nil {
		panic("nil map error")
	}
	executedReqArr := []string{}
	d.Decode(&executedReqArr)
	kv.excutedReq.Range(func(key, value interface{}) bool {
		kv.excutedReq.Delete(key)
		return true
	})
	for _, reqId := range executedReqArr {
		kv.excutedReq.Store(reqId, true)
	}
}

func (kv *KVServer) getSnapshot(lastApplied int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	executedReq := []string{}
	kv.excutedReq.Range(func(key interface{}, value interface{}) bool {
		executedReq = append(executedReq, key.(string))
		return true
	})
	e.Encode(executedReq)
	return w.Bytes()
}
