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

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	Index   int
}

//
// A Go object implementing a single Raft peer.
//
const (
	FOLLOER  = 0
	CADIDATE = 1
	LEADER   = 2
)
const TICKER int64 = 100

type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	applyChan      chan ApplyMsg
	installSuccess chan bool
	leaderId       int
	//heartbeatTimer chan bool
	//electTimer     chan bool
	persister   *Persister // Object to hold this peer's persisted state
	me          int        // this peer's index into peers[]
	voteFor     int
	dead        int32 // set by Kill()
	status      int   //0,1,2:follow,candidate,leader
	currentTerm int
	logEty      []Entry
	commitIndex int
	lastApplied int
	lastTicker  int64
	//selectTimeOut int64
	//for leader
	nextIndex  []int
	matchIndex []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//2D
	snapShot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == LEADER
}

//get lastIndex and lastTerm
func (rf *Raft) getLast() (int, int) {
	index := len(rf.logEty) - 1
	if index >= 0 {
		return rf.logEty[index].Index, rf.logEty[index].Term
	} else {
		return rf.lastIncludedIndex, rf.lastIncludedTerm
	}
}

//return the index position of the logEty
func (rf *Raft) getPos(index int) int {
	i := 0
	for i < len(rf.logEty) && rf.logEty[i].Index != index {
		i++
	}
	if len(rf.logEty) == i {
		return -1
	}
	return i
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logEty)
	state := w.Bytes()
	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)
	e2.Encode(rf.lastIncludedIndex)
	e2.Encode(rf.lastIncludedTerm)
	e2.Encode(rf.snapShot)
	snapShot := w2.Bytes()
	snapShot = nil //for lab3A
	rf.persister.SaveStateAndSnapshot(state, snapShot)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, flag int) {
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if flag == 0 {
		var currentTerm int
		var voteFor int
		var logEty []Entry
		if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil ||
			d.Decode(&logEty) != nil {
			DPrintf("decode error")
		} else {
			rf.currentTerm = currentTerm
			rf.voteFor = voteFor
			rf.logEty = logEty
		}
	} else if flag == 1 {
		var lastIncludedIndex int
		var lastIncludedTerm int
		var snapShot []byte
		if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil ||
			d.Decode(&snapShot) != nil {
			DPrintf("decode error")
		} else {
			rf.lastIncludedIndex = lastIncludedIndex
			rf.lastIncludedTerm = lastIncludedTerm
			rf.snapShot = snapShot
		}

	}

}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index, term := rf.getLast()
	if term > lastIncludedTerm || term == lastIncludedTerm && index > lastIncludedIndex {
		rf.installSuccess <- false
		return false
	}
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.snapShot = snapshot
	rf.logEty = rf.logEty[:0]
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.persist()
	rf.installSuccess <- true
	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapShot = snapshot
	rf.lastIncludedIndex = index
	i := rf.getPos(index)
	rf.lastIncludedTerm = rf.logEty[i].Term
	rf.logEty = rf.logEty[i+1:]
	rf.persist()
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	CandidateTerm int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int

	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	NodeTerm    int
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntriesArgs struct {
	LeaderTerm   int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Log          []Entry
	//2D
}

type AppendEntriesReply struct {
	NodeTerm        int
	Success         bool
	ConflixIndex    int
	ConflixTerm     int
	Installsnapshot bool
}

//2D
type InstallsnapshotArgs struct {
	LeaderTerm        int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallsnapshotReply struct {
	NodeTerm int
	Success  bool
}

func (rf *Raft) convertTo(status int) {
	switch status {
	case FOLLOER:
		rf.voteFor = -1
	case CADIDATE:
		rf.voteFor = rf.me
		rf.currentTerm++
	case LEADER:
		//rf.lastTicker = time.Now().UnixMilli()
	}
	rf.status = status
}

func resetElectTimeOut() int64 {
	rand.Seed(time.Now().UnixNano())
	return int64(rand.Intn(150) + 200)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.NodeTerm = rf.currentTerm
	reply.VoteGranted = false
	if args.CandidateTerm < rf.currentTerm {
		return
	}
	if args.CandidateTerm > rf.currentTerm {
		rf.currentTerm = args.CandidateTerm
		rf.convertTo(FOLLOER)
		rf.persist()
	}
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		lastIndex, lastTerm := rf.getLast()
		if args.LastLogTerm > lastTerm || args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex {
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			rf.lastTicker = time.Now().UnixMilli()
		} else {
			return
		}
	}
	rf.persist()
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.NodeTerm = rf.currentTerm
	reply.ConflixIndex = 0
	if rf.currentTerm > args.LeaderTerm {
		return
	}
	if rf.currentTerm < args.LeaderTerm {
		rf.currentTerm = args.LeaderTerm
		rf.convertTo(FOLLOER)
		rf.leaderId = args.LeaderId
		rf.persist()
	}
	rf.lastTicker = time.Now().UnixMilli()
	lastIndex, _ := rf.getLast()
	if lastIndex+1 <= args.PrevLogIndex {
		reply.ConflixIndex = lastIndex + 1
		return
	}
	prePos := rf.getPos(args.PrevLogIndex)
	if prePos != -1 && rf.logEty[prePos].Term != args.PrevLogTerm {
		term := rf.logEty[prePos].Term
		i := 0
		for i <= args.PrevLogIndex {
			if rf.logEty[i].Term == term {
				reply.ConflixIndex = rf.logEty[i].Index
				return
			}
			i++
		}
	}

	rf.logEty = append(rf.logEty[:prePos+1], args.Log...)
	rf.persist()

	if rf.commitIndex < args.LeaderCommit {
		index, _ := rf.getLast()
		rf.commitIndex = min(args.LeaderCommit, index)
	}
	if rf.commitIndex > rf.lastApplied {
		go rf.sendMsg(nil)
	}
	reply.ConflixIndex = 1
	reply.Success = true
	// Your code here (2A, 2B).
}

func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func (rf *Raft) InstallSnapshot(args *InstallsnapshotArgs, reply *InstallsnapshotReply) {
	rf.mu.Lock()
	reply.NodeTerm = rf.currentTerm
	if rf.currentTerm > args.LeaderTerm {
		rf.mu.Unlock()
		return
	}
	if rf.currentTerm < args.LeaderTerm {
		rf.convertTo(FOLLOER)
		rf.currentTerm = args.LeaderTerm
		rf.persist()
	}
	rf.lastTicker = time.Now().UnixMilli()
	msg := ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.Snapshot,
	}
	rf.mu.Unlock()
	rf.sendMsg(&msg)
	reply.Success = <-rf.installSuccess
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
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallsnapshotArgs, reply *InstallsnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.NodeTerm > rf.currentTerm {
			rf.convertTo(FOLLOER)
			rf.currentTerm = reply.NodeTerm
			rf.persist()
		} else if reply.Success {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
	} else {
		//DPrintf("Installsnapshot call fail")
	}
	return ok
}
func (rf *Raft) startNewElection() {
	rf.mu.Lock()
	rf.convertTo(CADIDATE)
	rf.lastTicker = time.Now().UnixMilli()
	rf.persist()
	voteMe := 1
	voteFinish := 0
	lastIndex, lastTerm := rf.getLast()
	args := RequestVoteArgs{
		CandidateTerm: rf.currentTerm,
		CandidateId:   rf.me,
		LastLogIndex:  lastIndex,
		LastLogTerm:   lastTerm,
	}
	rf.mu.Unlock()
	n := len(rf.peers)
	cond := sync.NewCond(&rf.mu)
	for i := 0; i < n; i++ {
		if i == args.CandidateId {
			continue
		}
		reply := RequestVoteReply{}
		go func(x int) {
			ok := rf.sendRequestVote(x, &args, &reply)
			rf.mu.Lock()
			if ok {
				//DPrintf("server %d sendrequest to %d voteinfo:%v", rf.me, x, reply.VoteGranted)

				if reply.VoteGranted {
					voteMe++
				}
				if reply.NodeTerm > rf.currentTerm {
					rf.currentTerm = reply.NodeTerm
					rf.convertTo(FOLLOER)
					rf.persist()
				}
			} else {
				//DPrintf("requestRPC error %d -> %d", rf.me, x)
			}
			voteFinish++
			rf.mu.Unlock()
			cond.Broadcast()
		}(i)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	for voteMe <= n/2 && voteFinish < n-1 && rf.status == CADIDATE {
		//DPrintf("server %d is wait", rf.me)
		cond.Wait()
	}
	if voteMe > n/2 && rf.status == CADIDATE {
		rf.convertTo(LEADER)
		rf.persist()
		index, _ := rf.getLast()
		for i := 0; i < n; i++ {
			rf.nextIndex[i] = index + 1
			rf.matchIndex[i] = 0
		}
		//DPrintf("server %d is now leader ,term is %d lastLog is %v index is %d",
		//rf.me, rf.currentTerm, rf.logEty[len(rf.logEty)-1], len(rf.logEty))
		rf.mu.Unlock()
		rf.sendHeartbeatOrEty()
		rf.mu.Lock()
	}
}

func (rf *Raft) sendHeartbeatOrEty() {
	rf.mu.Lock()
	appendReply := 0
	appendSuccess := 1
	rf.lastTicker = time.Now().UnixMilli()
	rf.mu.Unlock()
	var cond = sync.NewCond(&rf.mu)
	n := len(rf.peers)
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		if rf.nextIndex[i] < rf.lastIncludedIndex {
			installargs := InstallsnapshotArgs{
				LeaderTerm:        rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Snapshot:          rf.snapShot,
			}
			rf.mu.Unlock()
			installreply := InstallsnapshotReply{}
			go func(x int) {
				rf.sendInstallSnapshot(x, &installargs, &installreply)
			}(i)
			appendReply++
			continue
		} else {
			rf.mu.Unlock()
			go func(x int) {
				rf.mu.Lock()
				pos := rf.getPos(rf.nextIndex[x] - 1)
				args := AppendEntriesArgs{
					LeaderTerm:   rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[x] - 1,
					LeaderCommit: rf.commitIndex,
					Log:          rf.logEty[pos+1:],
				}
				if pos >= 0 {
					args.PrevLogTerm = rf.logEty[pos].Term
				} else {
					args.PrevLogTerm = rf.lastIncludedTerm
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(x, &args, &reply)
				rf.mu.Lock()
				appendReply++
				if ok {
					if reply.NodeTerm > rf.currentTerm || reply.ConflixIndex == 0 {
						DPrintf("term %d -> term %d id %d leader %d", rf.currentTerm, reply.NodeTerm, x, rf.me)
						rf.currentTerm = reply.NodeTerm
						rf.convertTo(FOLLOER)
						rf.persist()
					} else {
						if reply.Success {
							appendSuccess++
							rf.nextIndex[x] = args.PrevLogIndex + len(args.Log) + 1
							rf.matchIndex[x] = rf.nextIndex[x] - 1
						} else {
							rf.nextIndex[x] = reply.ConflixIndex
						}
					}
					//DPrintf("append ok %d -> %d", rf.me, x)
				} else {
					//DPrintf("appendRPC call failed %d -> %d", rf.me, x)
				}
				rf.mu.Unlock()
				cond.Broadcast()
			}(i)
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for appendSuccess <= n/2 && appendReply != n-1 && rf.status == LEADER {
		cond.Wait()
	}
	//DPrintf("server %d append ok,success num %d", rf.me, appendSuccess)
	if appendSuccess > n/2 && rf.status == LEADER {
		go rf.checkCommit()
	}
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
	if rf.status == LEADER {
		index, _ = rf.getLast()
		index++
		ety := Entry{Command: command, Term: rf.currentTerm, Index: index}
		rf.logEty = append(rf.logEty, ety)
		rf.matchIndex[rf.me] = index
		rf.persist()
		//go rf.sendHeartbeatOrEty()
		//log.Println("isStart!", rf.me)
	}
	// Your code here (2B).

	return index, rf.currentTerm, rf.status == LEADER
}
func (rf *Raft) checkCommit() {
	rf.mu.Lock()
	n := len(rf.peers)
	for N, _ := rf.getLast(); N > rf.commitIndex; N-- {
		k := 0
		for i := 0; i < n; i++ {
			if rf.matchIndex[i] >= N {
				k++
			}
		}
		j := rf.getPos(N)
		if k > n/2 && rf.logEty[j].Term == rf.currentTerm {
			rf.commitIndex = N
			rf.mu.Unlock()
			rf.sendMsg(nil)
			return
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendMsg(msg *ApplyMsg) {
	//DPrintf("server %d,log %v commit %d", rf.me, rf.logEty, rf.commitIndex)

	if msg != nil {
		rf.applyChan <- *msg
		return
	}
	rf.mu.Lock()
	applied := rf.lastApplied + 1
	commit := rf.commitIndex
	DPrintf("server %d,commit %d,applied %v", rf.me, rf.commitIndex, applied)
	rf.mu.Unlock()
	for ; applied <= commit; applied++ {
		rf.mu.Lock()
		i := rf.getPos(applied)
		if i < 0 {
			rf.mu.Unlock()
			break
		}
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logEty[i].Command,
			CommandIndex: applied,
		}
		rf.mu.Unlock()
		rf.applyChan <- msg
		rf.mu.Lock()
		rf.lastApplied++
		rf.mu.Unlock()
	}
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
/*func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.heartbeatTimer:
			rf.sendHeartbeatOrEty()
		case <-rf.electTimer:
			rf.startNewElection()
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}*/
//一定要用协程发起elect和heartbeat
func (rf *Raft) leaderTimer() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.status == LEADER {
			internal := time.Now().UnixMilli() - rf.lastTicker
			if internal > TICKER {
				//DPrintf("server %d send heartbeat", rf.me)
				rf.lastTicker = time.Now().UnixMilli()
				//rf.mu.Unlock()
				go rf.sendHeartbeatOrEty()
				//rf.mu.Lock()
				//rf.heartbeatTimer <- true
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 20)
	}
}

func (rf *Raft) flwOrCandidateTimer() {
	for !rf.killed() {
		timeout := resetElectTimeOut()
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		rf.mu.Lock()
		if rf.status != LEADER {
			internal := time.Now().UnixMilli() - rf.lastTicker
			if internal > timeout {
				//DPrintf("server %d start election term:%d", rf.me, rf.currentTerm)
				//rf.mu.Unlock()
				//rf.electTimer <- true
				go rf.startNewElection()
				//rf.mu.Lock()
			}
		}
		rf.mu.Unlock()
	}
}

//

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
	rf.status = FOLLOER
	rf.lastTicker = time.Now().UnixMilli()
	rf.voteFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyChan = applyCh
	rf.logEty = make([]Entry, 0)
	rf.logEty = append(rf.logEty, Entry{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.installSuccess = make(chan bool)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	//rf.electTimer = make(chan bool)
	//rf.heartbeatTimer = make(chan bool)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), 0)
	rf.readPersist(persister.ReadSnapshot(), 1)
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex

	// start ticker goroutine to start elections
	//go rf.ticker()
	go rf.leaderTimer()
	go rf.flwOrCandidateTimer()

	return rf
}
