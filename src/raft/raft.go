package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"
import "log"
import "bytes"
import "../labgob"

type State string
const (
	Follower State = "follower"
	Candidate State = "candidate"
	Leader State = "leader"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// 
// log entry
type LogEntry struct {
	Term int // term
	Index int // index
	Command interface{} //command
}

func GetRandTimeOut() int {
	return rand.Intn(200) + 110
}
// function that reset the election timeout 
func (rf *Raft) SetElectionTO(newTimeout int) {
	// election timeout milli second range from 10 to 500
	// lock ? TODO: concurrency control 
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimeout = newTimeout
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	cemu      sync.Mutex          // commit entry mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg		  // commited command should be send to applyCh
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state State // candidate follower leader
	term int // term number
	votedFor int // 
	logs []LogEntry // logs
	commitIndex int // index of highest log entry known to be commited
	lastApplied int // index of highest log entry known to be applied
	electionTimeout int // election timeout milli second range from 10 to 500
	//
	// volatile state on leaders
	//
	nextIndex []int // indeces of next log entry to be send to the follower, initialized to leader lastLogIndex +1
	matchIndex []int // indeces of the highest log entry known to be replicated on followers, initially handle
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.term
	isleader := (rf.state == Leader)
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs) // this cost too much if it's not append to a file...
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	
	if d.Decode(&rf.term) != nil ||
	   d.Decode(&rf.votedFor) != nil ||
	   d.Decode(&rf.logs) != nil {
		rf.term = 0
		rf.votedFor = -1
		rf.logs = []LogEntry{}
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int // 
	CandidateId int //
	LastLogIndex int //
	LastLogTerm int //
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int // current term, for candidate to update itself
	VoteGranted bool // true means receive a vote
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PreLogIndex int
	PreLogTerm int // -1 if not prev log
	Entries []LogEntry
	LeaderCommit int
}

// is the two args too few? how the leader know which rpc success ?
type AppendEntriesReply struct {
	Term int
	Success bool
	
	// parame used for fast backup
	XTerm int  // term of the conflict entry
	XIndex int // index of the first entry of the xTerm
	XLen int // length of the follower's log
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntryHandler", args, reply)
	return ok
}


// send append entries to all other servers
func (rf* Raft) AppendEntries() {
	rf.mu.Lock()
	// should it hold the lock during the rpc calls ? no !
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	//log.Printf("leader:%v,send append entries,leadercommit:%v,last apply:%v,loglen:%v",rf.me,rf.commitIndex,rf.lastApplied,len(rf.logs))
	for i , _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, rf *Raft) {
			rf.mu.Lock()
			entries := []LogEntry{}
			lastLogIndex := rf.nextIndex[server] - 1
			lastLogTerm := -1
			if lastLogIndex >= 0 {
				lastLogTerm = rf.logs[lastLogIndex].Term
			}
			for i:=rf.nextIndex[server]; i<len(rf.logs); i++ {
				entries = append(entries,rf.logs[i])
			}
			args := AppendEntriesArgs {
				Term : rf.term,
				LeaderId : rf.me,
				PreLogIndex : lastLogIndex,
				PreLogTerm : lastLogTerm,
				Entries : entries,
				LeaderCommit : rf.commitIndex,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntry(server,&args,&reply)
			if !ok { // invalid response
				return
			}
			// handle response, should handle concurrency control
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.term != args.Term { // stale response
				return
			}
			// if a bigger term received
			if rf.updateTerm(reply.Term,server) {
				return
			}
			if !reply.Success {
				// not success
				// log.Printf("server:%v,AE failed",server)
				rf.nextIndex[server] = rf.getNextIndex(reply.XTerm, reply.XIndex, reply.XLen)
				// if out of order response(stale response) arrive, this could be wrong?
				// rf.nextIndex shoud at leadst more than matchIndex
				rf.nextIndex[server] = max(rf.nextIndex[server],rf.matchIndex[server]+1)
			} else {
				// update match index
				// log.Printf("server:%v,AE succeed",server)
				if args.PreLogIndex + len(entries) > rf.matchIndex[server] {
					rf.matchIndex[server] = args.PreLogIndex + len(entries)
				}
				// update nextIndex 
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				// update leader's commit index
				if rf.matchIndex[server] > rf.commitIndex {
					newCommitIndex := rf.getLeaderCommit()
					if newCommitIndex > rf.commitIndex && rf.logs[newCommitIndex].Term == rf.term { // leader only update commited entry in his term
						rf.commitIndex = newCommitIndex
					}
				}
				// commit entries
				rf.commitEntries()
			}
			// log.Printf("Leader commit:%v,log len:%v",rf.commitIndex,len(rf.logs))
			//log.Printf("[%v] leader%v, server %v,nextIdx:%v",rf.term,rf.me,server,rf.nextIndex[server])
		}(i, rf)	
	}
}

// append entry rpc handler , hold the raft lock until return  
func (rf *Raft) AppendEntryHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {	
	// get the preLogindex and pre log term 
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("0.s%v,receive append entries",rf.me)
	// prepare param
	reply.Term = rf.term
	reply.Success = false
	
	// if bigger term received, handle first
	rf.updateTerm(args.Term,args.LeaderId)
	
	if args.Term < rf.term {
		return
	}
	// log.Printf("1.s%v,receive append entries",rf.me)
	// reset the election timeout, thread safe, already aquire the lock
	rf.electionTimeout = GetRandTimeOut()

	prelogterm := -1
	if args.PreLogIndex >= 0 && args.PreLogIndex < len(rf.logs) { 
		prelogterm = rf.logs[args.PreLogIndex].Term
	}

	rf.state = Follower // turn to a follower if it's a candidate
	// log.Printf("2.s%v,receive append entries",rf.me)
	// log consistency check
	if len(rf.logs)-1 < args.PreLogIndex || prelogterm != args.PreLogTerm { 
		log.Printf("[%v]leader%v server%v,log consistency check failed,a.preLogidx:%v,a.preterm:%v",rf.term,args.LeaderId,rf.me,args.PreLogIndex,args.PreLogTerm)
		reply.XTerm, reply.XIndex, reply.XLen = rf.getBackUpPara(args.PreLogIndex)
		return
	}
	// log.Printf("3.s%v,receive append entries",rf.me)
	// only if log entry confilicts can we delete the logs in the follower
	pos := args.PreLogIndex + 1
	hasconflict := false
	for _ , entry := range args.Entries {
		if pos < len(rf.logs) {
			if rf.logs[pos].Term!=entry.Term {
				hasconflict = true
			}
			rf.logs[pos] = entry
		} else {
			rf.logs = append(rf.logs,entry)
		}
		pos++
	}
	if hasconflict {
		rf.logs = rf.logs[:args.PreLogIndex+1+len(args.Entries)]
	}
	// log.Printf("4.s%v,receive append entries",rf.me)
	rf.persist()
	// set commitIndex carefully or will risk excuting wrong logs
	if args.LeaderCommit > rf.commitIndex {
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = min(args.LeaderCommit,args.PreLogIndex+len(args.Entries))
		rf.commitEntries()
	}
	// reply
	// log.Printf("5.s%v,receive append entries",rf.me)
	// log.Printf("[%v] server[%v],accept AE from %v,PrelogIdx:%v,PrelogTerm:%v,entries:%v",rf.term,rf.me,args.LeaderId,args.PreLogIndex,args.PreLogTerm,rf.logs)
	reply.Success = true
	return
}

//start a new election
func (rf* Raft) KickOffElection() {
	rf.mu.Lock()
	// should lock the whole raft when election ?? no ,candidate still need to handle append entry rpc
	// prepare args
	// reset election timeout and start a election
	// log.Printf("term[%v] server[%v],start election",rf.term,rf.me)
	rf.term++
	rf.state = Candidate
	rf.votedFor = -1 //
	rf.persist()
	serverCnt := len(rf.peers)
	lastLogTerm := 0 
	if len(rf.logs)-1 >= 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	args := RequestVoteArgs {
		Term :  rf.term,
		CandidateId : rf.me,
		LastLogIndex : len(rf.logs)-1,
		LastLogTerm : lastLogTerm,
	}
	rf.mu.Unlock()
	var voteLock sync.Mutex //protect voteCnt, received, max_term
	cond := sync.NewCond(&voteLock)
	voteCnt := 1 //a candidate vote for itself
	received := 1
	maxTerm := -1 // max term number received during election
	// concurrently send vote request
	for i := 0; i < serverCnt; i++ {
		if i == rf.me {continue} //this server itself
		reply := RequestVoteReply{}
		go func(server int) {
			res := rf.sendRequestVote(server,&args,&reply)
			voteLock.Lock()
			defer voteLock.Unlock()
			if res {
				// store the vote result
				if reply.VoteGranted {
					// log.Printf("term[%v] server[%v],get a vote from %v",rf.term,rf.me,server)
					voteCnt++
				}
			}
			received++
			if reply.Term > maxTerm {
				maxTerm = reply.Term
			}
			cond.Broadcast()
		}(i)
	}
	minority := serverCnt/2
	voteLock.Lock()
	defer voteLock.Unlock()
	for voteCnt <= minority && received != serverCnt && maxTerm <= rf.term {
		cond.Wait() // wait until get enough votes or election finished or a higher term received
		// whenever receives a response with higher term, should update term and turn to follower
		// will cond.wait() release the lock?
	}
	rf.mu.Lock() // need to read the term,modify state so lock the rf
	defer rf.mu.Unlock()
	if maxTerm > rf.term { //turn to follower
		rf.term = maxTerm
		rf.persist()
		rf.state = Follower
	} else if voteCnt > minority && args.Term == rf.term && rf.state == Candidate { // make sure now it's still a candidate
		// become leader
		rf.state = Leader
		// log.Printf("term[%v], server[%v] become the leader,servercnt:%v,leaderlog:%v",rf.term,rf.me,serverCnt,rf.logs)
		// initial nextIndex and matchIndex array
		rf.nextIndex = []int{}
		rf.matchIndex = []int{}
		logLen := len(rf.logs)
		for i:=0; i<serverCnt; i++ {
			rf.nextIndex = append(rf.nextIndex,logLen)
			rf.matchIndex = append(rf.matchIndex,-1)
		}
	} else {
		// should turn to follower state ?
		// rf.term -= 1
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply )  {
	// Your code here (2A, 2B).
	// handle requst vote here
	rf.mu.Lock()
	defer rf.mu.Unlock()


	reply.Term = rf.term
	reply.VoteGranted = false

	if args.Term < rf.term {
		// log.Printf("term[%v],server[%d]: smaller term,reject to vote server %v",rf.term,rf.me,args.CandidateId)
		return
	}
	rf.updateTerm(args.Term,-1)
	// only follower can vote
	if rf.state != Follower {
		// log.Printf("term[%v],server[%d]: server not in follower state,reject to vote server %v",rf.term,rf.me,args.CandidateId)
		return
	}
	if rf.votedFor >= 0 && rf.term == args.Term { // already voted in this term 
		// log.Printf("term[%v],server[%d]: server already voted for %v,reject to vote server %v",rf.term,rf.me,rf.votedFor,args.CandidateId)
		return
	}
	// should reset timeout here
	rf.electionTimeout = GetRandTimeOut()
	// vote restriction 
	lastLogIndex := len(rf.logs)-1
	lastLogTerm := -1
	if lastLogIndex >= 0 {
		lastLogTerm = rf.logs[lastLogIndex].Term
	}
	// vote restriction
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		return 
	}

	// vote for the candidate
	rf.votedFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// what should the start function do with the command ???
	index := -1
	term := rf.term
	isLeader := (rf.state == Leader)
	if !isLeader {
		return index, term, isLeader
	}
	// Your code here (2B).
	index = len(rf.logs)
	rf.logs = append(rf.logs,LogEntry {
		Index : index,
		Term : term,
		Command :command,
	})
	rf.persist()
	// log.Printf("[%v] server%v,leader append a command :%v",rf.term,rf.me,command)
	return index + 1, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.votedFor = -1
	rf.me = me
	rf.applyCh = applyCh
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = Follower
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	// start a background go routine to kick off election peridically
	rf.SetElectionTO(GetRandTimeOut())
	go func(rf* Raft){ 
		// how to gracefully implements the election timeout??
		for !rf.killed() { // leader dont't need election timeout
			time.Sleep(10*time.Millisecond)
			rf.electionTimeout -= 10
			if rf.electionTimeout <= 0 { // start a election
				rf.SetElectionTO(GetRandTimeOut())
				if rf.state != Leader {
					// log.Printf("term[%v] server %v start election",rf.term,rf.me)
					go rf.KickOffElection()
				}
			}
		}
	}(rf)

	// peridically leader should send append entry rpc... but how to implement this ?
	// at most ten heartbeats per second
	go func(rf *Raft){
		for !rf.killed() {
			time.Sleep(100*time.Millisecond)
			rf.AppendEntries()
		}
	}(rf)
	return rf
}
//
// helper functions
func (rf *Raft) getLeaderCommit() int {
	// using binary search,is there any another graceful solution?
	left,right,res := max(0,rf.commitIndex),len(rf.logs)-1,-1
	majority := len(rf.peers)/2
	for left <= right {
		mid := (left+right)/2
		cnt := 0
		for i:=0;i<len(rf.matchIndex);i++{
			if rf.matchIndex[i]>=mid{
				cnt += 1
			}
		}
		if cnt>=majority {
			res = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return res
}

// apply commited entries need higher level locks
func (rf *Raft) commitEntries(){
	// rf.cemu.Lock()
	// defer rf.cemu.Unlock()
	// apply command if commit index > apply index
	// log.Printf("[%v],server[%v],commit entries,lastApply:%v,commitIdx:%v",rf.term,rf.me,rf.lastApplied,rf.commitIndex)
	for rf.commitIndex > rf.lastApplied {
		// log.Printf("server:%v,send to channel",rf.me)
		rf.lastApplied += 1
		cmdIdx := rf.lastApplied
		rf.applyCh <- ApplyMsg{
			CommandValid : true,
			Command : rf.logs[cmdIdx].Command,
			CommandIndex : cmdIdx + 1, // the tester index start from 1!!!???
		}
		// log.Printf("[%v],server[%v],apply command:%v,idx:%v",rf.term,rf.me,rf.logs[cmdIdx].Command,cmdIdx)
		
	}
}

// whenever receive a term , this should be called
// return true if recTerm > rf.term
func (rf *Raft) updateTerm(rcvTerm,serverId int) bool {
	if(rcvTerm<=rf.term) {return false}
	rf.term = rcvTerm
	rf.state = Follower
	rf.votedFor = serverId
	rf.persist()
	return true
}

//
// functions used to fast backup
//

// follower call this to get the xTerm,xIndex,xLen
// conflictIdx is the prevLog index given by leader
func (rf *Raft) getBackUpPara(conflictIdx int) (int, int, int) {
	xLen := len(rf.logs)
	if xLen == 0 {
		return -1, -1, 0
	}
	// incase conflict idx >= len(rf.logs)
	conflictIdx = min(conflictIdx, len(rf.logs)-1)
	xTerm := rf.logs[conflictIdx].Term
	// use binary search to find to first confict Idx
	xIndex, l, r := -1, 0, conflictIdx
	for l <= r {
		mid := (l+r)/2
		if rf.logs[mid].Term == xTerm {
			xIndex = mid
			r = mid - 1
		} else if rf.logs[mid].Term < xTerm {
			l = mid + 1
		}
	}
	return xTerm, xIndex, xLen
}

// leader use this to detect the nextIdx quickly
func (rf *Raft) getNextIndex(xTerm int, xIndex int, xLen int) int {
	if xIndex < 0 {
		return 0
	}
	nextIndex := xIndex
	rightBound := min(xLen-1,len(rf.logs)-1)
	for nextIndex <= rightBound && rf.logs[nextIndex].Term == xTerm {
		nextIndex += 1
	}
	return nextIndex
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func max(a,b int) int {
	if a<b {
		return b
	}
	return a
}