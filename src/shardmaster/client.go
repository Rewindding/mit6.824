package shardmaster

//
// Shardmaster clerk.
//

import (
	"../labrpc"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

var ClientId int32 = 0

type Clerk struct {
	servers    []*labrpc.ClientEnd
	clientId   int32
	requestNum int32
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
	// Your code here.
	ck.clientId = atomic.AddInt32(&ClientId, 1)
	ck.requestNum = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		ClientId:   atomic.LoadInt32(&ck.clientId),
		RequestNum: atomic.AddInt32(&ck.requestNum, 1),
		Num:        num,
	}
	// Your code here.
	args.Num = num
	args.ClientId, args.RequestNum = ck.getClientIdAndReqNum()
	for {
		var reply QueryReply
		// try last leader first
		ok := ck.servers[ck.lastLeader].Call("ShardMaster.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			return reply.Config
		}
		// try each known server.
		for i, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId, args.RequestNum = ck.getClientIdAndReqNum()
	for {
		var reply JoinReply
		ok := ck.servers[ck.lastLeader].Call("ShardMaster.Join", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		// try each known server.
		for i, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId, args.RequestNum = ck.getClientIdAndReqNum()
	for {
		var reply LeaveReply
		ok := ck.servers[ck.lastLeader].Call("ShardMaster.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		// try each known server.
		for i, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId, args.RequestNum = ck.getClientIdAndReqNum()
	for {
		var reply MoveReply
		ok := ck.servers[ck.lastLeader].Call("ShardMaster.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		// try each known server.
		for i, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) getClientIdAndReqNum() (int32, int32) {
	return ck.clientId, atomic.AddInt32(&ck.requestNum, 1)
}
