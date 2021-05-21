package kvraft

import (
	"../labrpc"
)
import "crypto/rand"
import "math/big"
import "sync"

var ClientId int = 0
var idLock sync.Mutex

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	requestId  int
	clientId   int // how to generate unique client id??
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	idLock.Lock()
	defer idLock.Unlock()
	ck.clientId = ClientId
	ClientId++
	ck.lastLeader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// log.Printf("get request,key:%v\n",key)
	// You will have to modify this function.
	reply := GetReply{}
	ck.mu.Lock()
	reqId := ck.requestId
	ck.requestId++
	ck.mu.Unlock()
	// 先请求last Leader
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: reqId,
	}
	leaderServer := ck.servers[ck.lastLeader]

	for reply.Err != OK {
		res := leaderServer.Call("KVServer.Get", &args, &reply)
		if res && reply.Err == OK {
			return reply.Value
		}
		for idx, server := range ck.servers {
			res = server.Call("KVServer.Get", &args, &reply)
			DPrintf("get,res:%v,reply.Err:%v", res, reply.Err)
			if !res {
				continue
			}
			if reply.Err == OK {
				ck.lastLeader = idx
				break
			}
			// log.Printf("retry get")
		}
	}
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// log.Printf("%v request,key:%v,value:%v",op,key,value)

	reply := PutAppendReply{}
	ck.mu.Lock()
	reqId := ck.requestId
	ck.requestId++
	ck.mu.Unlock()
	args := PutAppendArgs{
		Op:        op,
		Key:       key,
		Value:     value,
		ClientId:  ck.clientId,
		RequestId: reqId,
	}
	lastLeader := ck.servers[ck.lastLeader]
	for reply.Err != OK {
		res := lastLeader.Call("KVServer.PutAppend", &args, &reply)
		if res && reply.Err == OK {
			return
		}
		for idx, server := range ck.servers {
			res = server.Call("KVServer.PutAppend", &args, &reply)
			if !res {
				continue
			}
			if reply.Err == OK {
				ck.lastLeader = idx
				return
			}
			// log.Printf("retry put append")
		}
	}
	// how to return response to client ?
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) getRequestId() {

}
