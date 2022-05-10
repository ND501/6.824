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

const ELECTION_TIMEOUT time.Duration = 300 * time.Millisecond

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

type ElectionTools struct {
	mu        sync.Mutex
	beginTime time.Time
	agreed    int32
	disagreed int32
}

func (et *ElectionTools) reset() {
	et.mu.Lock()
	defer et.mu.Unlock()

	et.beginTime = time.Now()
}

func (et *ElectionTools) timeout() bool {
	et.mu.Lock()
	defer et.mu.Unlock()

	nowTime := time.Now()
	// Random timeout range at [300, 600]
	randTimeOut := time.Duration(rand.Intn(300)+300) * time.Millisecond
	if nowTime.Sub(et.beginTime) >= randTimeOut {
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

	et    ElectionTools
	state State

	leaderRunning int32
	// TODO: maybe add a LeaderID variable, in case the client request send to follower
	//       and follower don't know where to redirect the request.
}

func (rf *Raft) init() {
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.state = ST_FOLLOWER
	rf.et.beginTime = time.Now()

	// Set sentinel as first log at index 0
	emptyLog := LogEntry{}
	emptyLog.index = 0
	emptyLog.term = 0
	rf.log = append(rf.log, interface{}(emptyLog))
}

func (rf *Raft) initLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	atomic.StoreInt32(&rf.leaderRunning, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.commitIndex
	}
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
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	// Update currentTerm and vote
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state == ST_LEADER || rf.state == ST_CANDIDATE {
			log.Printf(
				"[%v] handle RequestVote, newer term %v from [%v], convert to follower from %v",
				rf.me, args.Term, args.CandidateId, rf.state,
			)
			rf.state = ST_FOLLOWER
			atomic.StoreInt32(&rf.leaderRunning, 0)
		}
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLog := rf.log[len(rf.log)-1].(LogEntry)
		if lastLog.term < args.LastLogTerm ||
			(lastLog.term == args.LastLogTerm && lastLog.index <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.et.reset()
			log.Printf("[%v] vote for %v, reset timer, log not empty", rf.me, args.CandidateId)
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
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > reply.Term {
		// Out-of-date request, do noting and return
		return
	} else if rf.currentTerm < reply.Term {
		// Update currentTerm, set votedFor to -1
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	}

	// Received request from current leader, reset timer
	rf.et.reset()

	// When candidate received valid AppendEntries, stop election
	if rf.state == ST_CANDIDATE {
		rf.state = ST_FOLLOWER
		log.Printf(
			"[%v] handle AppendEntries, from [%v], electing now, convert to follower",
			rf.me, args.LeaderId,
		)
	}

	// Handle AppendEntries
	if len(args.Entries) == 1 {
		// If log doesn't contain an entry at PrevLogIndex with PrevLogTerm, append failed
		if len(rf.log)-1 < args.PrevLogIndex ||
			rf.log[args.PrevLogIndex].(LogEntry).term != args.PrevLogTerm {
			log.Printf(
				"[%v] handle AppendEntries, append failed, not contain prev log "+
					"at index %v with term %v",
				rf.me, args.PrevLogIndex, args.PrevLogTerm,
			)
			return
		}
		newEntry := args.Entries[0].(LogEntry)
		// If log contain entry at new entry's index with different term, delete them all
		if len(rf.log)-1 >= newEntry.index &&
			rf.log[newEntry.index].(LogEntry).term != args.Term {
			rf.log = rf.log[:args.PrevLogIndex+1]
			log.Printf(
				"[%v] handle AppendEntries, delete %v conflict logs",
				rf.me, len(rf.log)-newEntry.index,
			)
		}
		// Append new entry and set reply.Success to true
		rf.log = append(rf.log, newEntry)
		reply.Success = true
		log.Printf(
			"[%v] handle AppendEntries, append successed with %+v",
			rf.me, newEntry,
		)
	} else if len(args.Entries) > 1 {
		log.Printf(
			"[%v] handle AppendEntries, from [%v], many entries, not supported",
			rf.me, args.LeaderId,
		)
	}
	// Otherwise, reply.Success is false
}

func (rf *Raft) sendAppendEntries(server int, isHeartbeat bool) {
	// TODO: to be continue
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	// Fill in arguments
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	if !isHeartbeat {
		toAppend := rf.nextIndex[server]
		args.PrevLogIndex = toAppend - 1
		args.PrevLogTerm = rf.log[toAppend-1].(LogEntry).term
		// Only append one entry, not support many entries in one RPC for now
		// TODO: support many entries
		args.LeaderCommit = rf.commitIndex
		args.Entries = append(args.Entries, rf.log[toAppend].(LogEntry))
	}
	rf.mu.Unlock()

	rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	// Handle reply and lock mutex
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// Stop leader process and convert to follower when received newer term
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		atomic.StoreInt32(&rf.leaderRunning, 0)
		log.Printf(
			"[%v] send AppendEntries, newer term %v from [%v], convert to follower from %v",
			rf.me, reply.Term, server, rf.state,
		)
		rf.state = ST_FOLLOWER
	} else if reply.Success {
		// Increase nextIndex and matchIndex when successed
		rf.nextIndex[server]++
		rf.matchIndex[server]++
	} else if !isHeartbeat {
		// Decrease nextIndex and retry when failed
		rf.nextIndex[server]--
		log.Printf("[%v] send AppendEntries, failed with index %v, try again",
			rf.me, args.Entries[0].(LogEntry).index,
		)
		go rf.sendAppendEntries(server, false)
	}
	// Heartbeat will return false, but no need to handle
}

func (rf *Raft) StartLeaderProcess() {
	// Init for leader
	rf.initLeader()
	log.Printf("[%v] start leader process", rf.me)

	for {
		// Judge if leader should continue by leaderRunning atomic int
		if atomic.LoadInt32(&rf.leaderRunning) == 0 {
			break
		}
		// Send heartbeats to each peer
		for server := range rf.peers {
			if server != rf.me {
				go rf.sendAppendEntries(server, true)
			}
		}
		// Sleep for 150 milliseconds
		time.Sleep(150 * time.Millisecond)
	}

	log.Printf("[%v] stop leader process", rf.me)
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
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// Stop election, convert to follower
		rf.state = ST_FOLLOWER
		log.Printf(
			"[%v] send RequestVote, newer term %v from [%v], lose election",
			rf.me, reply.Term, server,
		)
	} else if reply.VoteGranted {
		// Update agreed votes
		atomic.AddInt32(&rf.et.agreed, 1)
		if atomic.LoadInt32(&rf.et.agreed) > int32((len(rf.peers)>>1)) &&
			rf.state == ST_CANDIDATE {
			// Win election, convert to leader and start leaderProcess
			rf.state = ST_LEADER
			go rf.StartLeaderProcess()
			log.Printf(
				"[%v] send RequestVote, win election with term %v",
				rf.me, rf.currentTerm,
			)
		}
	} else {
		// Update disagreed votes
		atomic.AddInt32(&rf.et.disagreed, 1)
		if atomic.LoadInt32(&rf.et.disagreed) > int32(len(rf.peers)>>1) &&
			rf.state == ST_CANDIDATE {
			// Lose election, convert to follower
			rf.state = ST_FOLLOWER
			log.Printf(
				"[%v] send RequestVote, majority disagree, lose election",
				rf.me,
			)
		}
	}
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.votedFor = rf.me
	atomic.StoreInt32(&rf.et.agreed, 1)
	atomic.StoreInt32(&rf.et.disagreed, 0)
	// Convert to candidate and increase currentTerm
	rf.state = ST_CANDIDATE
	rf.currentTerm++
	log.Printf("[%v] start election with term %v", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	// Call RequestVote to each peers
	for server := range rf.peers {
		if server != rf.me {
			go rf.sendRequestVote(server)
		}
	}

	// Wait for election result or 300 ms timeout
	beginTime := time.Now()
	nowTime := time.Now()
	for nowTime.Sub(beginTime) < 300*time.Millisecond {
		// Case 1, newer term or majority decision shutdown the election
		rf.mu.Lock()
		if rf.state != ST_CANDIDATE {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		nowTime = time.Now()
	}
	// Case 2, election timeout, clear votes to invalidate subsequent votes
	atomic.StoreInt32(&rf.et.agreed, 0)
	atomic.StoreInt32(&rf.et.disagreed, 0)
	rf.mu.Lock()
	rf.state = ST_FOLLOWER
	rf.mu.Unlock()

	log.Printf("[%v] lose election, timeout", rf.me)
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
			rf.et.reset()
			go rf.StartElection()
		} else {
			time.Sleep(25 * time.Millisecond)
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
