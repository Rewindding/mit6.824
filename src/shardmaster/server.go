package shardmaster

import (
	"../raft"
	"fmt"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

const (
	TimeOut = time.Millisecond * 300
	// op names
	JOIN  = "JOIN"
	LEAVE = "LEAVE"
	MOVE  = "MOVE"
	QUERY = "QUERY"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs      []Config // indexed by config num
	channelMap   sync.Map
	executedReqs sync.Map
}

type Op struct {
	// Your data here.
	OpName     string
	JoinArgs   JoinArgs
	LeaveArgs  LeaveArgs
	MoveArgs   MoveArgs
	QueryArgs  QueryArgs
	QueryReply QueryReply
	ClientId   int32
	RequestNum int32
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	op := &Op{
		OpName:     JOIN,
		JoinArgs:   *args, // 这里会把数据copy一份吗？猜测会
		ClientId:   args.ClientId,
		RequestNum: args.RequestNum,
	}
	_, err := sm.handleRequest(op)
	reply.Err = err
	reply.WrongLeader = err == ERR_NOT_LEADER
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := &Op{
		OpName:     LEAVE,
		LeaveArgs:  *args,
		ClientId:   args.ClientId,
		RequestNum: args.RequestNum,
	}
	_, err := sm.handleRequest(op)
	reply.Err = err
	reply.WrongLeader = err == ERR_NOT_LEADER
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	op := &Op{
		OpName:     MOVE,
		MoveArgs:   *args,
		ClientId:   args.ClientId,
		RequestNum: args.RequestNum,
	}
	_, err := sm.handleRequest(op)
	reply.Err = err
	reply.WrongLeader = err == ERR_NOT_LEADER
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	op := &Op{
		OpName:     QUERY,
		QueryArgs:  *args,
		ClientId:   args.ClientId,
		RequestNum: args.RequestNum,
	}
	respOp, err := sm.handleRequest(op)
	if respOp != nil {
		*reply = respOp.QueryReply
	}
	reply.Err = err
	reply.WrongLeader = err == ERR_NOT_LEADER
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) Killed() {

}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	go func(sm *ShardMaster) {
		for {
			// listen from the raft channel
			logEntry := <-sm.applyCh
			op, ok := logEntry.Command.(Op)
			if !ok {
				panic("err")
			}
			sm.handleOp(&op)
			// notify the request goroutine to respond
			value, loaded := sm.channelMap.Load(logEntry.CommandIndex)
			if !loaded {
				continue
			}
			ch := value.(chan Op)
			ch <- op
		}
	}(sm)
	return sm
}

func (sm *ShardMaster) handleRequest(op *Op) (*Op, Err) {
	reqId := getRequestId(op.ClientId, op.RequestNum)
	if _, ok := sm.executedReqs.Load(reqId); ok && op.OpName != QUERY {
		return op, OK
	}
	logIdx, _, isLeader := sm.rf.Start(*op)
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}
	ch := make(chan Op)
	_, loaded := sm.channelMap.LoadOrStore(logIdx, ch)
	defer sm.channelMap.Delete(logIdx)
	if loaded {
		panic("duplicate log index err")
	}
	select {
	case currentOp := <-ch:
		{
			// 检查reqId是否相同
			if getRequestId(currentOp.ClientId, currentOp.RequestNum) != reqId {
				return nil, ERR_NOT_CURRENT_REQ
			}
			return &currentOp, OK
		}
	case <-time.After(TimeOut):
		{
			return nil, ERR_TIMEOUT
		}
	}

}

func (sm *ShardMaster) handleOp(op *Op) {
	// detect duplicate request
	reqId := getRequestId(op.ClientId, op.RequestNum)
	_, loaded := sm.executedReqs.LoadOrStore(reqId, true)
	switch op.OpName {
	case JOIN:
		{
			if !loaded {
				sm.handleJoin(&op.JoinArgs)
			}
			break
		}
	case LEAVE:
		{
			if !loaded {
				sm.handleLeave(&op.LeaveArgs)
			}
			break
		}
	case MOVE:
		{
			if !loaded {
				sm.handleMove(&op.MoveArgs)
			}
			break
		}
	case QUERY:
		{
			conf := sm.handleQuery(&op.QueryArgs)
			op.QueryReply = QueryReply{
				WrongLeader: false,
				Err:         OK,
				Config:      *conf,
			}
			break
		}
	}
}

func getRequestId(clientId int32, requestNum int32) string {
	return fmt.Sprintf("%v:%v", clientId, requestNum)
}
