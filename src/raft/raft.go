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

const ELECTION_TIMEOUT time.Duration = 200 * time.Millisecond

type State int

const (
	ST_FOLLOWER  State = 1
	ST_CANDIDATE State = 2
	ST_LEADER    State = 3
)

type LogEntry struct {
	term    int
	index   int
	command interface{}
}

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

type ElectionTimer struct {
	mu        sync.Mutex
	beginTime time.Time
}

func (et *ElectionTimer) reset(nowTime time.Time) {
	et.mu.Lock()
	defer et.mu.Unlock()
	et.beginTime = nowTime
}

func (et *ElectionTimer) timeout() bool {
	et.mu.Lock()
	defer et.mu.Unlock()

	nowTime := time.Now()
	if nowTime.Sub(et.beginTime) >= ELECTION_TIMEOUT {
		return true
	} else {
		return false
	}
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

	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []interface{}
	// Volatile state on all servers:
	commitIndex int
	lastApplied int
	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	et    ElectionTimer
	state State

	leaderRunning   int32
	electionRunning int32
	voteCount       int32
}

func (rf *Raft) init() {
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.state = ST_FOLLOWER
	rf.et.beginTime = time.Now()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == ST_LEADER {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

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
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// log.Printf("[%v] handle RequestVote from %v", rf.me, args)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	// Update currentTerm and vote
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state == ST_LEADER || rf.state == ST_CANDIDATE {
			log.Printf("[%v] degenerate from %v to follower", rf.me, rf.state)
			rf.state = ST_FOLLOWER
		}
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		logSize := len(rf.log)
		if logSize == 0 {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.et.reset(time.Now())
			log.Printf("[%v] vote for %v, reset timer", rf.me, args.CandidateId)
		} else {
			lastLog := rf.log[logSize-1].(LogEntry)
			if lastLog.term < args.LastLogTerm ||
				(lastLog.term == args.LastLogTerm && lastLog.index < args.LastLogIndex) {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.et.reset(time.Now())
				log.Printf("[%v] vote for %v, reset timer", rf.me, args.CandidateId)
			}
		}
	}
	// Otherwise, reply.VoteGranted default to be false
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
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if len(args.Entries) == 0 &&
		rf.state == ST_CANDIDATE &&
		rf.currentTerm <= reply.Term {
		// Reset timer when received valid AppendEntries or heartbeat
		rf.et.reset(time.Now())
		// Set electionRunning to 0
		atomic.StoreInt32(&rf.electionRunning, 0)
		// Convert to follower
		rf.state = ST_FOLLOWER
		log.Printf("[%v] received AppendEntries when electing, convert to follower", rf.me)
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartBeat(server int) {
	// Send heartbeat to server
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	// Update currentTerm and leaderRunning when needed
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		atomic.StoreInt32(&rf.leaderRunning, 0)
		rf.state = ST_FOLLOWER
		log.Printf("[%v] received newer term from [%v] with %v, degenerate to follower",
			rf.me, server, reply.Term)
	}
	rf.mu.Unlock()
}

func (rf *Raft) StartLeaderProcess() {
	// Set leaderRunning to 1 in atomic way
	atomic.StoreInt32(&rf.leaderRunning, 1)
	log.Printf("[%v] start leader process", rf.me)

	for {
		// Judge if leader should continue by leaderRunning atomic int
		if atomic.LoadInt32(&rf.leaderRunning) == 0 {
			break
		}
		// Send heartbeats to each peer
		for server := range rf.peers {
			if server != rf.me {
				go rf.sendHeartBeat(server)
			}
		}
		// Sleep for 100 milliseconds
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("[%v] quit leaderProcess", rf.me)
}

func (rf *Raft) sendRequestVote(server int) {
	// Call RequestVote to server
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	args.CandidateId = rf.me
	rf.mu.Lock()
	args.Term = rf.currentTerm
	rf.mu.Unlock()
	rf.peers[server].Call("Raft.RequestVote", &args, &reply)

	// Check vote result
	rf.mu.Lock()
	if reply.VoteGranted {
		// Atomic add voteCount
		atomic.AddInt32(&rf.voteCount, 1)
		if atomic.LoadInt32(&rf.voteCount) > int32((len(rf.peers)>>1)) &&
			rf.state != ST_LEADER {
			// Convert to leader and start leaderProcess
			rf.state = ST_LEADER
			go rf.StartLeaderProcess()
			// Stop election because already win
			atomic.StoreInt32(&rf.electionRunning, 0)
			log.Printf("[%v] win election with term %v", rf.me, rf.currentTerm)
		}
	} else if reply.Term >= rf.currentTerm {
		// Convert to follower and stop election
		rf.state = ST_FOLLOWER
		atomic.StoreInt32(&rf.electionRunning, 0)
		log.Printf("[%v] lose election because newer term %v", rf.me, reply.Term)
	}
	rf.mu.Unlock()
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	// Set electionRunning to 1 and vote to self
	atomic.StoreInt32(&rf.electionRunning, 1)
	rf.votedFor = rf.me
	atomic.StoreInt32(&rf.voteCount, 1)
	// Convert to candidate and increase currentTerm
	rf.state = ST_CANDIDATE
	rf.currentTerm++
	log.Printf("[%v] start new election with term %v, reset timer", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	// Call RequestVote to each peers
	for server := range rf.peers {
		if server != rf.me && atomic.LoadInt32(&rf.electionRunning) == 1 {
			go rf.sendRequestVote(server)
		}
	}

	// Wait for election result or timeout
	// TODO: set timeout to be 400 ms, maybe incorrect
	beginTime := time.Now()
	nowTime := time.Now()
	for nowTime.Sub(beginTime) < 400*time.Microsecond {
		rf.mu.Lock()
		if rf.state == ST_LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Microsecond)
		nowTime = time.Now()
	}
	// Convert to follower and set electionRunning to 0
	rf.mu.Lock()
	rf.state = ST_FOLLOWER
	atomic.StoreInt32(&rf.electionRunning, 0)
	rf.mu.Unlock()
	log.Printf("[%v] lose election because timeout", rf.me)
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
	atomic.StoreInt32(&rf.leaderRunning, 0)
	atomic.StoreInt32(&rf.leaderRunning, 0)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		nowState := rf.state
		rf.mu.Unlock()
		if nowState == ST_FOLLOWER && rf.et.timeout() {
			log.Printf("[%v] election timeout", rf.me)
			rf.et.reset(time.Now())
			go rf.StartElection()
		} else {
			// TODO: set to 200-400 ms, maybe not correct
			sleepFor := 200 + rand.Intn(200)
			time.Sleep(time.Duration(sleepFor) * time.Millisecond)
		}
	}
	log.Printf("[%v] is killed\n", rf.me)
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
	rf.init()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func init() {
	log.SetFlags(log.Ldate | log.Lmicroseconds)
}
