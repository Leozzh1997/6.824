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
	//	"bytes"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type Entry struct {
	Command interface{}
	Term    int //term recived by leader
}

//
// A Go object implementing a single Raft peer.
//
const TICKER int64 = 100

type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	applyChan   chan ApplyMsg
	persister   *Persister // Object to hold this peer's persisted state
	me          int        // this peer's index into peers[]
	voteFor     int
	voteFinish  int
	voteMeNum   int
	dead        int32 // set by Kill()
	status      int   //0,1,2:follow,candidate,leader
	currentTerm int
	//isLeader             bool
	logEty      []Entry
	commitIndex int
	lastApplied int
	lastTicker  int64
	//for leader
	appendReply   int
	appendSuccess int
	nextIndex     []int
	matchIndex    []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == 2
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	CandidateTerm int
	CandidateId   int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	NodeTerm int
	VotedFor int
	// Your data here (2A).
}

type AppendEntriesArgs struct {
	LeaderTerm   int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Log          []Entry
}

type AppendEntriesReply struct {
	NodeTerm int
	Success  bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.CandidateTerm >= rf.currentTerm && rf.voteFor == -1 || args.CandidateTerm > rf.currentTerm {
		rf.voteFor = args.CandidateId
		rf.currentTerm = args.CandidateTerm
		rf.status = 0
	}
	rf.lastTicker = time.Now().Unix()
	reply.NodeTerm = rf.currentTerm
	reply.VotedFor = rf.voteFor
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.NodeTerm = rf.currentTerm
	if args.Log == nil {
		reply.Success = true
	}
	if args.LeaderTerm >= rf.currentTerm {
		rf.currentTerm = args.LeaderTerm
		rf.status = 0
		rf.voteFor = -1
		rf.lastTicker = time.Now().UnixMilli()
		if len(rf.logEty) == args.PrevLogIndex {
			rf.logEty = append(rf.logEty, args.Log...)
		}
		//log.Println("flw", len(rf.logEty))
		if rf.commitIndex < args.LeaderCommit {
			if args.LeaderCommit < len(rf.logEty) {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.logEty)
			}
			//log.Println("flw", rf.me, rf.commitIndex)
			go rf.sendMsg()
		}
		reply.Success = true
	}

	// Your code here (2A, 2B).
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
	//log.Println("caller called", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.voteFinish++
	//log.Println("call suceed", rf.me, server)
	if !ok {
		//log.Println("RPC error callerId,callId", rf.me, server)
		return ok
	} else {
		//log.Println("call suceed", rf.me, server)
	}
	if reply.NodeTerm > rf.currentTerm {
		rf.status = 0
		rf.currentTerm = reply.NodeTerm
	}
	if reply.VotedFor == rf.me {
		rf.voteMeNum++
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success {
		rf.appendSuccess++
	}
	rf.appendReply++
	if !ok {
		//log.Println("RPC error AppendEn lederId,callId", rf.me, server)
		return ok
	} else {
		//log.Println("Append suceed", rf.me, server)
	}
	if reply.NodeTerm > rf.currentTerm {
		rf.status = 0
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := 0
	if rf.status == 2 {
		ety := Entry{Command: command, Term: rf.currentTerm}
		rf.logEty = append(rf.logEty, ety)
		index = len(rf.logEty)
		log.Println("isStart!", index)
		go rf.sendHeartbeatOrEty(rf.logEty[index-1:], index)
	}
	// Your code here (2B).

	return index, rf.currentTerm, rf.status == 2
}

func (rf *Raft) sendMsg() {
	log.Println("id,index,commad", rf.me, rf.commitIndex, rf.logEty[rf.commitIndex-1].Command)
	msg := ApplyMsg{
		CommandValid: true,
		Command:      rf.logEty[rf.commitIndex-1].Command,
		CommandIndex: rf.commitIndex,
	}
	rf.applyChan <- msg
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		//log.Println("liveId", rf.me)
		rand.Seed(time.Now().UnixNano())
		selectTimeOut := rand.Intn(500) + 500
		time.Sleep(time.Duration(TICKER) * time.Millisecond)
		rf.mu.Lock()
		if rf.status != 2 {
			rf.mu.Unlock()
			time.Sleep(time.Duration(selectTimeOut) * time.Millisecond)
			rf.mu.Lock()
			if time.Now().UnixMilli()-rf.lastTicker > int64(selectTimeOut) {
				rf.status = 1
				rf.currentTerm++
				rf.voteFor = rf.me
				rf.voteMeNum = 1
				rf.voteFinish = 0
				rf.mu.Unlock()
				//log.Println("currentId,currentTerm", rf.me, rf.currentTerm)
				rf.startNewElection()
			} else {
				rf.mu.Unlock()
			}
		} else {
			//log.Println("sendHeartbeat,status", rf.me, rf.status)
			if time.Now().UnixMilli()-rf.lastTicker > TICKER {
				rf.mu.Unlock()
				rf.sendHeartbeatOrEty(nil, 0)
			} else {
				rf.mu.Unlock()
			}

		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

//
func (rf *Raft) startNewElection() {
	args := RequestVoteArgs{rf.currentTerm, rf.me}
	n := len(rf.peers)
	cond := sync.NewCond(&rf.mu)
	for i := 0; i < n; i++ {
		if i == args.CandidateId {
			continue
		}
		reply := RequestVoteReply{}
		go func(x int) {
			rf.sendRequestVote(x, &args, &reply)
			cond.Broadcast()
		}(i)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.voteMeNum <= n/2 && rf.voteFinish < n-1 {
		//log.Println("wait", rf.me)
		cond.Wait()
	}
	if rf.voteMeNum > n/2 {
		rf.status = 2
		//log.Println("leaderId,term", rf.me, rf.currentTerm)
		//go rf.sendHeartbeatOrEty(nil, 0)
	}
}

func (rf *Raft) sendHeartbeatOrEty(logEty []Entry, index int) {
	rf.mu.Lock()
	//log.Println(index)
	args := AppendEntriesArgs{
		LeaderTerm:   rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: index - 1,
		Log:          logEty,
		LeaderCommit: rf.commitIndex,
	}
	if index > 1 {
		args.PrevLogTerm = rf.logEty[index-2].Term
	}
	rf.appendReply = 0
	rf.appendSuccess = 1
	rf.lastTicker = time.Now().UnixMilli()
	rf.mu.Unlock()
	var cond = sync.NewCond(&rf.mu)
	n := len(rf.peers)
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		reply := AppendEntriesReply{}
		go func(x int) {
			rf.sendAppendEntries(x, &args, &reply)
			cond.Broadcast()
		}(i)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.appendSuccess <= n/2 && rf.appendReply <= n-1 {
		cond.Wait()
		//log.Println("detect", index, rf.appendSuccess)
	}
	//log.Println("loglenth", index, rf.appendSuccess > n/2, rf.commitIndex < len(rf.logEty))
	/*if logEty == nil {
		log.Println("log nil")
		return
	}*/
	if rf.appendSuccess > n/2 && rf.commitIndex < len(rf.logEty) && index != 0 {
		rf.commitIndex = args.PrevLogIndex + len(logEty)
		//log.Println("leader", rf.commitIndex)
		go rf.sendMsg()
	}
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
	rf.status = 0
	rf.lastTicker = time.Now().UnixMilli()
	rf.voteFor = -1
	rf.currentTerm = 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyChan = applyCh
	rf.logEty = make([]Entry, 0)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
