package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "log"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu sync.Mutex
	requestId int
	clientId int // how to generate unique client id??
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
	log.Printf("get request,key:%v\n",key)
	// You will have to modify this function.
	reply := GetReply {}
	for reply.Err != OK {
		for _, server := range(ck.servers) {
			args := GetArgs {
				Key : key,
			}
			res := server.Call("KVServer.Get",&args,&reply)
			if !res {
				continue
			}
			if reply.Err == OK {
				break
			}
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
	log.Printf("%v request,key:%v,value:%v",op,key,value)
	reply := PutAppendReply {}
	for reply.Err != OK {
		for _, server := range(ck.servers) {
			args := PutAppendArgs {
				Op : op,
				Key : key,
				Value : value,
			}
			res := server.Call("KVServer.PutAppend",&args,&reply)
			if !res {
				continue
			}
			if reply.Err == OK {
				return
			}
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