package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Debug = 0
	// raft timeout 的原因可能是网络失败，可能是当前服务器已经不再是leader了,原因不确定
	RaftTimeOut = time.Millisecond * 200
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
		panic("unhandled problem: duplicate log index")
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
		panic("unhandled problem: duplicate log index")
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
	kv.kvStore = make(map[string]string)
	// You may need initialization code here.
	// should each server need to start a go routine and listen to the applyCh??...
	go func(kv *KVServer) {
		for !kv.killed() {
			// read from apply msg channel
			// log.Printf("server:%v, read from ch blocked",me)
			logEntry := <-kv.applyCh
			// log.Printf("server:%v, unblocked",me)
			command, ok := logEntry.Command.(Op)
			if !ok { // type assertion failed
				DPrintf("read a dummy log:%v", logEntry.Command.(string))
				continue
			}
			// apply this command to kv store
			if command.OpName == "Put" {
				kv.kvStore[command.Key] = command.Value
			} else if command.OpName == "Append" {
				// 存在这样的情况，已经被执行了但是还是再次被append到log里面，这里也要判断如果已经被执行了就不再执行
				_, loaded := kv.excutedReq.LoadOrStore(command.RequestId, true)
				if !loaded {
					kv.kvStore[command.Key] += command.Value
				}
			} else if command.OpName == "Get" {
				command.Value = kv.kvStore[command.Key]
			}
			// 把op放到对应channel里面去
			ch, ok := kv.channelMap.Load(logEntry.CommandIndex)
			if ok { // only leader need to do this
				DPrintf("notify idx:%v", logEntry.CommandIndex)
				ch.(chan Op) <- command
			}
		}
	}(kv)
	time.Sleep(100 * time.Millisecond)
	return kv
}
