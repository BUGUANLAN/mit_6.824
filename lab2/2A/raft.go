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

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

//import "fmt"

// import "bytes"
// import "../labgob"

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

type Log struct {
	Index   int    //在日志中的索引
	Term    int    //任期号
	Command string //命令
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state              int32         //0：follwer 1:candidate 2:leader
	currentTerm        int           // 当前最新任期号
	voteFor            int           //收到的投票的候选人id，没投就为空
	log                []Log         //log条目
	commitIndex        int           //已提交的最大的log条目索引
	lastApplied        int           //应用于状态机的最大索引条目
	nextIndex          int           //leader要发给server的下一个条目索引
	matchIndex         int           //该服务器上最大的条目索引
	electionTimeout    time.Duration //选举超时时间，选举时随机初始化
	getVotes           int           //候选人获得票数
	startTime          time.Time     //开始时间
	voteFinish         chan bool     //投票已完成
	rpcRequest         chan bool     //收到rpc请求
	isSendVoteFinished bool          //是否发送了选举完成channel
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = false
	if rf.state == 2 {
		isleader = true
	}
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期号
	CandidateId  int //候选人Id
	LastLogIndex int //候选人最后条目的索引
	LastLogTerm  int //候选人最后条目的任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //更新候选人的任期号
	VoteGranted bool //是否投票 true：投票 false：不投
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//对raft加锁
	rf.mu.Lock()
	fmt.Println(rf.me, "receive vote request from", args.CandidateId, "at", rf.startTime)
	if args.Term > rf.currentTerm {
		rf.voteFor = -1 //有更新的term初始化为未投
	}
	if args.Term < rf.currentTerm {
		//任期比我小不给投
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		if args.Term > rf.currentTerm {
			rf.state = 0
			rf.currentTerm = args.Term
		}

		if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
			//条件满足可以投票
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.state = 0
			rf.voteFor = args.CandidateId
			rf.rpcRequest <- true
			fmt.Println(rf.me, "vote for ", args.CandidateId, "term is", args.Term, "at", time.Now())
		}
	}
	rf.mu.Unlock()
}

type AppendEntriesArgs struct {
	Term         int   //当前leader任期号
	LeaderId     int   //
	PrevLogIndex int   //
	PrevLogTerm  int   //
	Entries      []Log //
	LeaderCommit int   //
}

type AppendEntriesReply struct {
	Term    int  //当前最新任期号
	Success bool //follower与prevLogIndex，prevLogTerm匹配成功时返回true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.currentTerm <= args.Term {
		rf.rpcRequest <- true
	}
	fmt.Println(rf.me, "receive heartbeat from", args.LeaderId, "at", rf.startTime)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		//检查条目是否匹配
		if rf.state != 0 {
			rf.state = 0 //有更新的term，自己要成为follwer
		}
		rf.currentTerm = args.Term
		if len(rf.log) <= args.PrevLogIndex {
			reply.Success = false
		} else {
			if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
				reply.Success = true
				rf.currentTerm = args.Term
			} else {
				//如果索引一样，term不一样删除当前索引之后的条目
				rf.log = rf.log[:args.PrevLogIndex]
				reply.Success = false
			}
		}
	}
	rf.mu.Unlock()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, votes *int) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if rf.isSendVoteFinished {
		return
	}
	if ok {
		if reply.VoteGranted {
			*votes++
			if (*votes) > len(rf.peers)/2 {
				rf.mu.Lock()
				rf.state = 2
				rf.isSendVoteFinished = true
				rf.mu.Unlock()
				rf.voteFinish <- true
				fmt.Println(rf.me, "is leader term is", rf.currentTerm, "at", time.Now())
				//成为leader立马发送heartbeat
				go rf.SendHeartbeat()
			}
		} else if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.state = 0
			rf.currentTerm = reply.Term
			rf.isSendVoteFinished = true
			rf.mu.Unlock()
			rf.voteFinish <- true
		}
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

//根据不同的状态执行不同的任务
func (rf *Raft) ExeTask() {
	for {

		state := atomic.LoadInt32(&rf.state)

		switch state {
		case 0:
			//等待rpc请求
			//设置超时时间
			select {
			case <-rf.rpcRequest:
			case <-time.After(time.Duration(rand.Intn(200+1)+200) * time.Millisecond):
				rf.mu.Lock()
				rf.state = 1
				fmt.Println(rf.me, "become candidate at", time.Now())
				rf.mu.Unlock()
			}

		case 1:
			mystate := atomic.LoadInt32(&rf.state)
			for mystate == 1 {
				rf.SendElection()
				time.Sleep(time.Duration(rand.Intn(30+1)+20) * time.Millisecond)
				mystate = atomic.LoadInt32(&rf.state)
			}

		case 2:
			//发送appendEntriy和heartbeat请求
			//fmt.Println(rf.me, "send heartbeat request")
			state := atomic.LoadInt32(&rf.state)
			for state == 2 {
				if rf.SendHeartbeat() == false {
					break
				}
				time.Sleep(100 * time.Millisecond)
				state = atomic.LoadInt32(&rf.state)
			}
		}
	}
}

func (rf *Raft) SendElection() {
	//发送选举请求
	//设置选举超时时间5s
	rf.mu.Lock()
	rf.currentTerm++
	rf.voteFor = rf.me
	//限制选举超时时间为5秒
	rf.electionTimeout = time.Duration(1) * time.Second
	rf.startTime = time.Now()
	rf.mu.Unlock()
	//发送选举请求
	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	args.LastLogIndex = 0
	args.LastLogTerm = 0
	args.Term = rf.currentTerm
	votes := 0
	votes++
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	me := rf.me
	rf.isSendVoteFinished = false
	rf.mu.Unlock()
	fmt.Println(me, "send elect request term is", currentTerm, "at", time.Now())
	for i, _ := range rf.peers {
		if i != me {
			go rf.sendRequestVote(i, &args, &votes)
		}
	}

	select {
	case <-rf.voteFinish:
	case <-time.After(time.Duration(rand.Intn(500+1)+500) * time.Millisecond):
		rf.mu.Lock()
		rf.state = 1
		rf.mu.Unlock()
	}
	//time.Sleep(time.Duration(rand.Intn(50+1)+40) * time.Millisecond)
}

func (rf *Raft) SendHeartbeat() bool {
	args := AppendEntriesArgs{}
	args.LeaderId = rf.me
	rf.mu.Lock()
	args.Term = rf.currentTerm
	myterm := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()
	replyNum := 0
	reply := AppendEntriesReply{}
	fmt.Println(rf.me, "send heartbeat request at", time.Now())
	for i, _ := range rf.peers {
		if i != me {
			if rf.sendAppendEntries(i, &args, &reply) {
				replyNum++
				if reply.Term > myterm {
					rf.mu.Lock()
					rf.state = 0
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
					return false
				}
			} else {
				//fmt.Println(me, "can not send heartbeat to", i)
			}
		}
	}
	if replyNum < len(rf.peers)/2 {
		rf.mu.Lock()
		rf.state = 0
		rf.mu.Unlock()
	}
	return true
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
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.state = 0
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.rpcRequest = make(chan bool, 10)
	rf.voteFinish = make(chan bool, 10)
	fmt.Println(len(peers), "peers")
	go rf.ExeTask()
	fmt.Println(me, "begin work")
	return rf
}
