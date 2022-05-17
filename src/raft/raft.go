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
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

type State int

const (
	ST_FOLLOWER  State = 1
	ST_CANDIDATE State = 2
	ST_LEADER    State = 3
)

func MinInt(lhs int, rhs int) int {
	if lhs < rhs {
		return lhs
	} else {
		return rhs
	}
}

func MaxInt(lhs int, rhs int) int {
	if lhs > rhs {
		return lhs
	} else {
		return rhs
	}
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
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
	// Random timeout range at [200, 400]
	randTimeOut := time.Duration(rand.Intn(200)+200) * time.Millisecond
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
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
	// Volatile state on all servers:
	commitIndex int
	lastApplied int
	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	et            ElectionTools
	state         State
	applyCh       chan ApplyMsg
	leaderRunning int32
}

func (rf *Raft) init(applyCh chan ApplyMsg) {
	rf.applyCh = applyCh
	rf.CurrentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.VotedFor = -1
	rf.state = ST_FOLLOWER
	rf.et.beginTime = time.Now()

	// Set sentinel as first log at index 0
	emptyLog := LogEntry{}
	emptyLog.Index = 0
	emptyLog.Term = 0
	rf.Log = append(rf.Log, emptyLog)
}

func (rf *Raft) initLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	atomic.StoreInt32(&rf.leaderRunning, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.Log)
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	log.Printf(
		"[%v] start leader process with term %v, init nextIndex as %v",
		rf.me, rf.CurrentTerm, len(rf.Log),
	)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log []LogEntry
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&Log) != nil {
		log.Printf("Read from persist failed")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Log = Log
	}
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
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.CurrentTerm
	// Update currentTerm and vote
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		if rf.state == ST_LEADER || rf.state == ST_CANDIDATE {
			log.Printf(
				"[%v] received RequestVote from [%v] with newer term %v, "+
					"convert to follower from %v",
				rf.me, args.CandidateId, args.Term, rf.state,
			)
			rf.state = ST_FOLLOWER
			atomic.StoreInt32(&rf.leaderRunning, 0)
		}
	}
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		lastLog := rf.Log[len(rf.Log)-1]
		if lastLog.Term < args.LastLogTerm ||
			(lastLog.Term == args.LastLogTerm && lastLog.Index <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.et.reset()
		}
	}
	// Otherwise, reply.VoteGranted default to be false
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// Args to support fast backup
	XTerm  int
	XIndex int
	XLen   int
}

// Make sure that mutex is got before call this function
func (rf *Raft) findFirstIndexInTermX(term int) int {
	// Can be optimised with dichotomous search
	for k, _ := range rf.Log {
		if rf.Log[k].Term == term {
			return k
		}
	}
	return -1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.CurrentTerm
	if rf.CurrentTerm > args.Term {
		// Out-of-date request, do nothing
		log.Printf(
			"[%v] received old term %v, now term %v",
			rf.me, args.Term, rf.CurrentTerm,
		)
		return
	} else if rf.CurrentTerm < args.Term {
		// Update currentTerm, set votedFor to null (-1)
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		if rf.state != ST_FOLLOWER {
			log.Printf(
				"[%v] received AppendEntries from [%v], "+
					"newer term with %v, convert to follower",
				rf.me, args.LeaderId, args.Term,
			)
			// When stray leader get back, may received AppendEntries with newer term
			if rf.state == ST_LEADER {
				atomic.StoreInt32(&rf.leaderRunning, 0)
			}
			rf.state = ST_FOLLOWER
		}
	}

	// Received request from current leader, reset timer
	rf.et.reset()

	// When candidate received valid AppendEntries, stop election
	if rf.state == ST_CANDIDATE {
		rf.state = ST_FOLLOWER
		log.Printf(
			"[%v] received AppendEntries from [%v], convert to follower from %v",
			rf.me, args.LeaderId, rf.state,
		)
	}

	// Check for prev log
	if len(rf.Log)-1 < args.PrevLogIndex {
		// Conflict arise, missing prev log, record XLen to AppendEntriesReply
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = args.PrevLogIndex - (len(rf.Log) - 1)
		log.Printf(
			"[%v] append failed, missing prev log at index %v",
			rf.me, args.PrevLogIndex,
		)
		return
	} else if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Conflict arise, different prev log, record XTerm and XIndex to AppendEntriesReply
		reply.XTerm = rf.Log[args.PrevLogIndex].Term
		reply.XIndex = rf.findFirstIndexInTermX(reply.XTerm)
		reply.XLen = -1
		log.Printf(
			"[%v] append failed, different prev log at index %v with term %v",
			rf.me, args.PrevLogIndex, args.PrevLogTerm,
		)
		return
	}

	// Append new entries
	if len(args.Entries) > 0 {
		reply.Success = true
		beginIndex := args.Entries[0].Index
		if len(rf.Log)-1 >= beginIndex {
			log.Printf(
				"[%v] delete logs from %v to %v",
				rf.me, beginIndex, len(rf.Log)-1,
			)
		}
		rf.Log = append(rf.Log[:beginIndex], args.Entries...)
		log.Printf("[%v] append successed, log size: %v", rf.me, len(rf.Log)-1)
	}

	// Update commitIndex
	oldCommitIndex := rf.commitIndex
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = MinInt(args.LeaderCommit, len(rf.Log)-1)
		log.Printf(
			"[%v] set commitIndex from %v to %v, lastApplied is %v",
			rf.me, oldCommitIndex, rf.commitIndex, rf.lastApplied,
		)
	}
	// Update lastApplied
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		if rf.lastApplied < len(rf.Log) {
			// Send message to applyCh
			msg := ApplyMsg{}
			msg.CommandValid = true
			msg.Command = rf.Log[rf.lastApplied].Command
			msg.CommandIndex = rf.Log[rf.lastApplied].Index
			rf.applyCh <- msg
			log.Printf(
				"[%v] applied %+v, log length %v",
				rf.me, rf.Log[rf.lastApplied], len(rf.Log)-1,
			)
		}
	}
}

// Make sure that mutex is got before call this function
func (rf *Raft) findMajorityMatch() int {
	array := make([]int, 0)
	for ii := range rf.peers {
		if ii != rf.me {
			array = append(array, rf.matchIndex[ii])
		}
	}
	sort.Ints(array)
	return array[len(array)/2]
}

func (rf *Raft) sendAppendEntries(server int, isHeartbeat bool) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	// Fill in arguments
	rf.mu.Lock()
	// In case prev RPC changed term and state, this RPC use newer term
	if rf.state != ST_LEADER {
		rf.mu.Unlock()
		return
	}
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
	// No newer log, can only send heartbeat
	if !isHeartbeat && rf.nextIndex[server] >= len(rf.Log) {
		isHeartbeat = true
	}
	if !isHeartbeat {
		toAppend := rf.nextIndex[server]
		args.PrevLogIndex = toAppend - 1
		args.PrevLogTerm = rf.Log[toAppend-1].Term
		args.Entries = append(args.Entries, rf.Log[toAppend:]...)
	}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		log.Printf("[%v] send AppendEntries to [%v] timeout", rf.me, server)
		return
	}

	// Handle reply and lock mutex
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// If term changed or reply already handled when waiting rpc return, do nothing
	if rf.CurrentTerm != args.Term ||
		(!isHeartbeat && rf.nextIndex[server] != args.Entries[0].Index) {
		return
	}
	if reply.Term > rf.CurrentTerm {
		// Stop leader process and convert to follower when received newer term
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		atomic.StoreInt32(&rf.leaderRunning, 0)
		if !isHeartbeat {
			log.Printf(
				"[%v] send AppendEntries to [%v] with {index: %v, term: %v}, "+
					"newer term %v, convert to follower from %v",
				rf.me, server, args.Entries[0].Index, args.Entries[0].Term,
				reply.Term, rf.state,
			)
		} else {
			log.Printf(
				"[%v] send AppendEntries to [%v], newer term %v, convert to follower from %v",
				rf.me, server, reply.Term, rf.state,
			)
		}
		rf.state = ST_FOLLOWER
	} else if reply.Success {
		// Increase nextIndex and matchIndex when successed
		rf.nextIndex[server] = MaxInt(rf.nextIndex[server], args.Entries[0].Index+1)
		rf.matchIndex[server] = MaxInt(rf.matchIndex[server], args.Entries[0].Index)
		majorityMatch := rf.findMajorityMatch()
		log.Printf(
			"[%v] got majorityMatch as %v, matchIndex: %v",
			rf.me, majorityMatch, rf.matchIndex,
		)
		// Try to update commitIndex
		if majorityMatch > rf.commitIndex &&
			rf.Log[majorityMatch].Term == rf.CurrentTerm {
			rf.commitIndex = majorityMatch
		}
		// Update lastApplied and send message to applyCh
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			if rf.lastApplied < len(rf.Log) {
				msg := ApplyMsg{}
				msg.CommandValid = true
				msg.Command = rf.Log[rf.lastApplied].Command
				msg.CommandIndex = rf.Log[rf.lastApplied].Index
				rf.applyCh <- msg
				log.Printf(
					"[%v] applied %+v, log length %v",
					rf.me, rf.Log[rf.lastApplied], len(rf.Log)-1,
				)
			}
		}
		log.Printf(
			"[%v] send AppendEntries to [%v] with {index: %v, term: %v}, "+
				"commitIndex at %v, lastApplied at %v",
			rf.me, server, args.Entries[0].Index, args.Entries[0].Term,
			rf.commitIndex, rf.lastApplied,
		)
	} else if !isHeartbeat {
		// Fail to append because of inconsistency, decrease nextIndex by reply args
		if reply.XTerm != -1 {
			// if rf.Log[reply.XIndex].Term == reply.XTerm {
			// 	rf.nextIndex[server] = MaxInt(1, reply.XIndex+1)
			// } else {
			// 	rf.nextIndex[server] = MaxInt(1, reply.XIndex)
			// }
			rf.nextIndex[server] = MaxInt(1, reply.XIndex)
		} else {
			rf.nextIndex[server] = MaxInt(1, rf.nextIndex[server]-reply.XLen)
		}
		log.Printf(
			"[%v] send AppendEntries to [%v] with "+
				"{term: %v, prevIndex: %v, prevTerm: %v, entries: %v, leaderCommit: %v} "+
				"entries from %v to %v, "+
				"reply {XTerm: %v, XIndex: %v, XLen: %v}, "+
				"new nextIndex is %v, try again",
			rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTerm,
			len(args.Entries), args.LeaderCommit, args.Entries[0].Index,
			args.Entries[len(args.Entries)-1].Index, reply.XTerm, reply.XIndex,
			reply.XLen, rf.nextIndex[server],
		)
		go rf.sendAppendEntries(server, false)
	}
	// Heartbeat will return false, but no need to handle
}

func (rf *Raft) StartLeaderProcess() {
	// Init for leader
	rf.initLeader()

	// Send heartbeats to each peer upon leaderProcess running
	for server := range rf.peers {
		if server != rf.me {
			go rf.sendAppendEntries(server, true)
		}
	}
	log.Printf("[%v] done with first heartbeat", rf.me)

	// Main while-loop
	for {
		// Judge if leader should continue by leaderRunning atomic int
		if atomic.LoadInt32(&rf.leaderRunning) == 0 {
			break
		}
		// Send heartbeats to each peer
		for server := range rf.peers {
			if server != rf.me {
				go rf.sendAppendEntries(server, false)
			}
		}
		// Sleep for 100 milliseconds
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("[%v] stop leader process", rf.me)
}

func (rf *Raft) sendRequestVote(server int) {
	// Call RequestVote to server
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	args.CandidateId = rf.me
	rf.mu.Lock()
	// In case prev RPC changed state to follower, this RPC send with newer term
	if rf.state == ST_FOLLOWER {
		rf.mu.Unlock()
		return
	}
	args.Term = rf.CurrentTerm
	args.LastLogIndex = rf.Log[len(rf.Log)-1].Index
	args.LastLogTerm = rf.Log[len(rf.Log)-1].Term
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !ok {
		log.Printf("[%v] send RequestVote to [%v] timeout", rf.me, server)
		return
	}

	// Check vote result
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// If self state or term changed when waiting for rpc reply, do nothing and return
	if rf.CurrentTerm != args.Term {
		return
	}
	if reply.Term > rf.CurrentTerm {
		// Stop election, convert to follower
		rf.state = ST_FOLLOWER
		log.Printf(
			"[%v] send RequestVote, newer term %v from [%v], lose election",
			rf.me, reply.Term, server,
		)
	} else if reply.VoteGranted {
		// Update agreed votes
		atomic.AddInt32(&rf.et.agreed, 1)
		log.Printf("[%v] voted from [%v]", rf.me, server)
		if atomic.LoadInt32(&rf.et.agreed) > int32((len(rf.peers)>>1)) &&
			rf.state == ST_CANDIDATE {
			// Win election, convert to leader and start leaderProcess
			rf.state = ST_LEADER
			go rf.StartLeaderProcess()
			log.Printf(
				"[%v] send RequestVote, win election with term %v",
				rf.me, rf.CurrentTerm,
			)
		}
	} else {
		// Update disagreed votes
		atomic.AddInt32(&rf.et.disagreed, 1)
		log.Printf("[%v] not voted from [%v]", rf.me, server)
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
	rf.VotedFor = rf.me
	atomic.StoreInt32(&rf.et.agreed, 1)
	atomic.StoreInt32(&rf.et.disagreed, 0)
	// Convert to candidate and increase currentTerm
	rf.state = ST_CANDIDATE
	rf.CurrentTerm++
	log.Printf("[%v] start election with term %v", rf.me, rf.CurrentTerm)
	rf.persist()
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
	rf.mu.Lock()
	// If server is not leader, or leader init not finished, return false
	if rf.state != ST_LEADER || atomic.LoadInt32(&rf.leaderRunning) == 0 {
		rf.mu.Unlock()
		return index, term, false
	}
	newEntry := LogEntry{}
	newEntry.Index = len(rf.Log)
	newEntry.Term = rf.CurrentTerm
	newEntry.Command = command
	rf.Log = append(rf.Log, newEntry)
	rf.persist()
	rf.mu.Unlock()

	log.Printf(
		"[%v] Call Start, append {index: %v, term: %v, cmd: %v}",
		rf.me, newEntry.Index, newEntry.Term, newEntry.Command,
	)

	return newEntry.Index, newEntry.Term, isLeader
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
	rf.init(applyCh)
	// Load persist data from persister
	rf.readPersist(persister.ReadRaftState())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func init() {
	log.SetFlags(log.Ltime)
}
