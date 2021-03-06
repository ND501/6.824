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

const RaftDebug = false

func RaftDPrintf(format string, a ...interface{}) (n int, err error) {
	if RaftDebug {
		log.Printf(format, a...)
	}
	return
}

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
	CommandTerm  int

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
	applyCh   chan ApplyMsg       // apply message to state machine

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

	// Support snapshot, persist data
	SnapshotLog       []byte
	LastSnapshotIndex int
	LastSnapshotTerm  int

	et          ElectionTools
	state       State
	cond        sync.Cond
	skipSleepCh chan bool
}

func (rf *Raft) init(applyCh chan ApplyMsg) {
	rf.applyCh = applyCh
	rf.CurrentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.VotedFor = -1
	rf.state = ST_FOLLOWER
	rf.et.beginTime = time.Now()
	rf.cond = *sync.NewCond(&rf.mu)
	rf.skipSleepCh = make(chan bool)

	rf.LastSnapshotIndex = 0
	rf.LastSnapshotTerm = 0
	rf.SnapshotLog = nil
}

func (rf *Raft) initLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.LogMax() + 1
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	RaftDPrintf(
		"[%v] start leader process with term %v, init nextIndex as %v",
		rf.me, rf.CurrentTerm, rf.LogMax()+1,
	)
}

//
// Get the current highest index, `mu` should be acquired.
//
func (rf *Raft) LogMax() int {
	return rf.LastSnapshotIndex + len(rf.Log)
}

//
// Transfer log entry index to slice index, `mu` should be acquired.
//
func (rf *Raft) IndexInSlice(index int) int {
	return index - rf.LastSnapshotIndex - 1
}

//
// Get log entry where `index` pointed without checking slice boundary.
// `mu` should be acquired.
//
func (rf *Raft) LogAt(index int) *LogEntry {
	return &rf.Log[rf.IndexInSlice(index)]
}

//
// Get entry's Index where `index` pointed after checking boundary. `mu`
// should be acquired.
//
func (rf *Raft) LogIndexAt(index int) int {
	ii := rf.IndexInSlice(index)
	if ii == -1 {
		return rf.LastSnapshotIndex
	} else if ii < -1 {
		return 0
	} else {
		return rf.LogAt(index).Index
	}
}

//
// Get entry's Term like `LogIndexAt` did. `mu` should be acquired.
//
func (rf *Raft) LogTermAt(index int) int {
	ii := rf.IndexInSlice(index)
	if ii == -1 {
		return rf.LastSnapshotTerm
	} else if ii < -1 {
		return 0
	} else {
		return rf.LogAt(index).Term
	}
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

	// Encode persist data
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	// Encode snapshot state
	e.Encode(rf.LastSnapshotIndex)
	e.Encode(rf.LastSnapshotTerm)

	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.SnapshotLog)
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
	var LastSnapshotIndex int
	var LastSnapshotTerm int
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&Log) != nil ||
		d.Decode(&LastSnapshotIndex) != nil ||
		d.Decode(&LastSnapshotTerm) != nil {
		RaftDPrintf("[%v] read from persist failed", rf.me)
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Log = Log
		rf.LastSnapshotIndex = LastSnapshotIndex
		rf.LastSnapshotTerm = LastSnapshotTerm
	}
	// Update commitIndex and lastApplied whenever snapshot loaded
	if rf.LastSnapshotIndex > 0 {
		rf.commitIndex = rf.LastSnapshotIndex
		rf.lastApplied = rf.LastSnapshotIndex
	}
	RaftDPrintf(
		"[%v] read from persist, Term: %v, votedFor: %v, len of Log: %v, "+
			"LastSnapshotIndex: %v, LastSnapshotTerm: %v",
		rf.me, CurrentTerm, VotedFor, len(Log), LastSnapshotIndex, LastSnapshotTerm,
	)
}

//
// Load snapshot from persist data.
//
func (rf *Raft) readPersistSnapshot(data []byte) {
	rf.SnapshotLog = data
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// If server already has a snapshot with `index`, simply return
	if index <= rf.LastSnapshotIndex {
		return
	}

	RaftDPrintf(
		"[%v] call snapshot with index %v, current lastSnapshotIndex %v, "+
			"lastSnapshotTerm %v, log size %v, log len %v",
		rf.me, index, rf.LastSnapshotIndex,
		rf.LastSnapshotTerm, rf.LogMax(), len(rf.Log),
	)

	rf.SnapshotLog = snapshot
	rf.LastSnapshotTerm = rf.LogAt(index).Term
	rf.Log = rf.Log[rf.IndexInSlice(index)+1:]
	rf.LastSnapshotIndex = index
}

func (rf *Raft) SignalPeriodically() {
	for !rf.killed() {
		time.Sleep(100 * time.Millisecond)
		rf.cond.Signal()
	}
}

func (rf *Raft) EndlessApply() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.cond.Wait()
		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			continue
		}
		// If there are appliable entries, wake up from cond.Wait()
		toApply := make([]LogEntry, rf.commitIndex-rf.lastApplied)
		copy(toApply, rf.Log[rf.IndexInSlice(rf.lastApplied+1):rf.IndexInSlice(rf.commitIndex+1)])
		curCommitIndex := rf.commitIndex
		rf.mu.Unlock()

		// Apply entries
		for _, entry := range toApply {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
			RaftDPrintf("[%v] applied %+v", rf.me, entry)
		}

		// Update lastApplied
		rf.mu.Lock()
		rf.lastApplied = MaxInt(curCommitIndex, rf.lastApplied)
		if len(toApply) > 0 {
			RaftDPrintf(
				"[%v] applied %v entries, commitIndex at %v, lastApplied at %v, log size %v",
				rf.me, len(toApply), rf.commitIndex, rf.lastApplied, rf.LogMax(),
			)
		}
		rf.mu.Unlock()
	}
	RaftDPrintf("[%v] stop EndlessApply goroutine", rf.me)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// `Offset` always be 0, no need for checking `Done`, no need to wait for more chunks
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	} else if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		if rf.state == ST_LEADER || rf.state == ST_CANDIDATE {
			RaftDPrintf(
				"[%v] received InstallSnapshotRPC from [%v] with newer term %v, "+
					"convert to follower from %v",
				rf.me, args.LeaderId, args.Term, rf.state,
			)
			rf.state = ST_FOLLOWER
		}
	}
	// Reset election timer
	rf.et.reset()
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	// Validate RPC data. Never accept old snapshot
	if rf.LastSnapshotTerm >= args.LastIncludedTerm &&
		rf.LastSnapshotIndex >= args.LastIncludedIndex {
		RaftDPrintf(
			"[%v] discard old snapshot {LastIncludedIndex: %v, LastIncludedTerm: %v}, "+
				"current snapshot {LastSnapshotIndex: %v, LastSnapshotTerm: %v}",
			rf.me, args.LastIncludedIndex, args.LastIncludedTerm,
			rf.LastSnapshotIndex, rf.LastSnapshotTerm,
		)
		return
	}
	// Replace snapshot file
	rf.SnapshotLog = args.Data
	// Trim logs
	if rf.LogTermAt(rf.LogMax()) >= args.LastIncludedTerm &&
		rf.LogIndexAt(rf.LogMax()) >= args.LastIncludedIndex &&
		rf.LogTermAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		rf.Log = rf.Log[rf.IndexInSlice(args.LastIncludedIndex)+1:]
	} else {
		rf.Log = nil
	}
	// Set LastSnapshotIndex and LastSnapshotTerm
	rf.LastSnapshotIndex = args.LastIncludedIndex
	rf.LastSnapshotTerm = args.LastIncludedTerm
	// Update lastApplied and commitIndex when apply snapshot
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	RaftDPrintf(
		"[%v] accept snapshot {LastIncludedIndex: %v, LastIncludedTerm: %v}",
		rf.me, rf.LastSnapshotIndex, rf.LastSnapshotTerm,
	)
	// Apply to client
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	go func(msg ApplyMsg) {
		rf.applyCh <- msg
		RaftDPrintf(
			"[%v] applied snapshot {LastIncludedIndex: %v, LastIncludedTerm: %v}",
			rf.me, msg.SnapshotIndex, msg.SnapshotTerm,
		)
	}(msg)
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

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
			RaftDPrintf(
				"[%v] received RequestVote from [%v] with newer term %v, "+
					"convert to follower from %v",
				rf.me, args.CandidateId, args.Term, rf.state,
			)
			rf.state = ST_FOLLOWER
		}
	}
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		lastIndex := rf.LogIndexAt(rf.LogMax())
		lastTerm := rf.LogTermAt(rf.LogMax())
		if lastTerm < args.LastLogTerm ||
			(lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex) {
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

// Make sure the mutex is acquired before call this function
func (rf *Raft) findFirstIndexInTermX(term int) int {
	// Can be optimised with dichotomous search
	for _, v := range rf.Log {
		if v.Term == term {
			return v.Index
		}
	}
	return rf.LastSnapshotIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.CurrentTerm
	if rf.CurrentTerm > args.Term {
		return
	} else if rf.CurrentTerm < args.Term {
		// Update currentTerm, set votedFor to null (-1)
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		if rf.state != ST_FOLLOWER {
			RaftDPrintf(
				"[%v] received AppendEntries from [%v], "+
					"newer term with %v, convert to follower",
				rf.me, args.LeaderId, args.Term,
			)
			rf.state = ST_FOLLOWER
		}
	}

	// Received request from current leader, reset timer
	rf.et.reset()

	// When candidate received valid AppendEntries, stop election
	if rf.state == ST_CANDIDATE {
		rf.state = ST_FOLLOWER
		RaftDPrintf(
			"[%v] received AppendEntries from [%v], convert to follower from %v",
			rf.me, args.LeaderId, rf.state,
		)
	}

	// Check for prev log
	if rf.LogMax() < args.PrevLogIndex {
		// Conflict arise, missing prev log, record XLen to AppendEntriesReply
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = args.PrevLogIndex - rf.LogMax()
		if len(args.Entries) > 0 {
			RaftDPrintf(
				"[%v] append failed, missing prev log at index %v, log size %v",
				rf.me, args.PrevLogIndex, rf.LogMax(),
			)
		}
		return
	} else if rf.LogTermAt(args.PrevLogIndex) != args.PrevLogTerm {
		// Conflict arise, different prev log, record XTerm and XIndex to AppendEntriesReply
		reply.XTerm = rf.LogTermAt(args.PrevLogIndex)
		reply.XIndex = rf.findFirstIndexInTermX(reply.XTerm)
		reply.XLen = -1
		if len(args.Entries) > 0 {
			RaftDPrintf(
				"[%v] append failed, different prev log at index %v with term %v, log size %v",
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.LogMax(),
			)
		}
		return
	}

	// Handle entries
	if len(args.Entries) > 0 {
		// Set to `true` directly. If append success (if there are conflicting entries,
		// deal with conflict first), it should be `true`. If entries in RPC already
		// exist locally, leader should be notified with `true` to update `nextIndex`,
		// so that leader can send newer log entries in next RPC.
		reply.Success = true
		// Append entries if required
		appendCount := 0
		for index, entry := range args.Entries {
			// Only needed if there is a conflict
			if rf.LogMax() < entry.Index || rf.LogAt(entry.Index).Term != entry.Term {
				oldLen := rf.LogMax()
				appendCount = len(args.Entries) - index
				deleteBegin := rf.IndexInSlice(entry.Index)
				rf.Log = append(rf.Log[:deleteBegin], args.Entries[index:]...)
				RaftDPrintf(
					"[%v] current logMax %v, append from %v to %v, args begin with %v: "+
						"{Term:%v, Id:%v, prevIndex:%v, prevTerm:%v, leaderCommit:%v}",
					rf.me, oldLen, entry.Index, rf.LogMax(), args.Entries[0].Index,
					args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit,
				)
				break
			}
		}
		if appendCount == 0 {
			RaftDPrintf(
				"[%v] already exist logs to %v, args from %v to %v: "+
					"{Term:%v, Id:%v, prevIndex:%v, prevTerm:%v, leaderCommit:%v}",
				rf.me, rf.LogMax(), args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index,
				args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit,
			)
		}
	}

	// Update commitIndex
	oldCommitIndex := rf.commitIndex
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = MinInt(args.LeaderCommit, rf.LogMax())
		RaftDPrintf(
			"[%v] set commitIndex from %v to %v, lastApplied is %v",
			rf.me, oldCommitIndex, rf.commitIndex, rf.lastApplied,
		)
		// Notify EndlessApply goroutine to apply msg
		rf.cond.Signal()
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
	args.PrevLogIndex = rf.LogIndexAt(rf.nextIndex[server] - 1)
	args.PrevLogTerm = rf.LogTermAt(rf.nextIndex[server] - 1)
	// No newer log, can only send heartbeat
	if !isHeartbeat && rf.nextIndex[server] > rf.LogMax() {
		isHeartbeat = true
	}
	if !isHeartbeat {
		// Entries to be appended in snapshot, turn to InstallSnapshot RPC
		if rf.LastSnapshotIndex > 0 && rf.nextIndex[server] <= rf.LastSnapshotIndex {
			go rf.sendInstallSnapshot(server)
			rf.mu.Unlock()
			return
		}
		toAppend := rf.IndexInSlice(rf.nextIndex[server])
		args.Entries = append(args.Entries, rf.Log[toAppend:]...)
	}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		// RaftDPrintf("[%v] send AppendEntries to [%v] timeout", rf.me, server)
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
		RaftDPrintf(
			"[%v] send AppendEntries to [%v], newer term %v, convert to follower from %v",
			rf.me, server, reply.Term, rf.state,
		)
		rf.state = ST_FOLLOWER
	} else if reply.Success {
		// Increase nextIndex and matchIndex when successed
		endIndex := len(args.Entries) - 1
		rf.nextIndex[server] = MaxInt(rf.nextIndex[server], args.Entries[endIndex].Index+1)
		rf.matchIndex[server] = MaxInt(rf.matchIndex[server], args.Entries[endIndex].Index)
		majorityMatch := rf.findMajorityMatch()
		RaftDPrintf(
			"[%v] got majorityMatch as %v, matchIndex: %v",
			rf.me, majorityMatch, rf.matchIndex,
		)
		// Try to update commitIndex
		if majorityMatch > rf.commitIndex &&
			rf.LogAt(majorityMatch).Term == rf.CurrentTerm {
			oldCommitIndex := rf.commitIndex
			rf.commitIndex = majorityMatch
			RaftDPrintf(
				"[%v] set commitIndex from %v to %v, lastApplied is %v",
				rf.me, oldCommitIndex, rf.commitIndex, rf.lastApplied,
			)
			// Notify EndlessApply goroutine to apply msg
			rf.cond.Signal()
		}
		RaftDPrintf(
			"[%v] send AppendEntries to [%v] with {index: %v, term: %v, len: %v}, "+
				"commitIndex at %v, lastApplied at %v",
			rf.me, server, args.Entries[0].Index, args.Entries[0].Term, len(args.Entries),
			rf.commitIndex, rf.lastApplied,
		)
	} else if !isHeartbeat {
		// Fail to append because of inconsistency, decrease nextIndex by reply args
		if reply.XTerm != -1 {
			rf.nextIndex[server] = MaxInt(1, reply.XIndex)
		} else {
			rf.nextIndex[server] = MaxInt(1, rf.nextIndex[server]-reply.XLen)
		}
		RaftDPrintf(
			"[%v] send AppendEntries to [%v] failed with "+
				"{term: %v, prevIndex: %v, prevTerm: %v, entries: %v, leaderCommit: %v} "+
				"entries from %v to %v, "+
				"reply {XTerm: %v, XIndex: %v, XLen: %v}, "+
				"new nextIndex is %v",
			rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTerm,
			len(args.Entries), args.LeaderCommit, args.Entries[0].Index,
			args.Entries[len(args.Entries)-1].Index, reply.XTerm, reply.XIndex,
			reply.XLen, rf.nextIndex[server],
		)
		if rf.LastSnapshotIndex > 0 && rf.nextIndex[server] <= rf.LastSnapshotIndex {
			go rf.sendInstallSnapshot(server)
			RaftDPrintf("[%v] try again with InstallSnapshot to [%v]", rf.me, server)
		} else {
			go rf.sendAppendEntries(server, false)
			RaftDPrintf("[%v] try again with AppendEntries to [%v]", rf.me, server)
		}
	}
	// Heartbeat will return false, but no need to handle
}

func (rf *Raft) sendInstallSnapshot(server int) {
	args := InstallSnapshotArgs{}
	reply := InstallSnapshotReply{}
	rf.mu.Lock()
	// Fill in arguments
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.LastSnapshotIndex
	args.LastIncludedTerm = rf.LastSnapshotTerm
	args.Data = rf.SnapshotLog
	args.Done = true
	// Record current nextIndex
	nextIndex := rf.nextIndex[server]
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		// RaftDPrintf("[%v] send InstallSnapshot to [%v] timeout", rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// Ignore out-of-date reply
	if nextIndex != rf.nextIndex[server] || rf.CurrentTerm != args.Term {
		return
	}
	// Handle reply
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		RaftDPrintf(
			"[%v] send InstallSnapshot to [%v], newer term %v, convert to follower from %v",
			rf.me, server, reply.Term, rf.state,
		)
		rf.state = ST_FOLLOWER
	} else {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		RaftDPrintf(
			"[%v] send InstallSnapshot to [%v] successed, set nextIndex to %v",
			rf.me, server, rf.nextIndex[server],
		)
	}
}

func (rf *Raft) StartLeaderProcess() {
	rf.initLeader()
	// Get currentTerm
	rf.mu.Lock()
	leaderTerm := rf.CurrentTerm
	rf.mu.Unlock()

	// First heartbeats upon leader init done
	for server := range rf.peers {
		if server != rf.me {
			go rf.sendAppendEntries(server, true)
		}
	}
	RaftDPrintf("[%v] first heartbeat done", rf.me)

	for {
		// Stop while-loop when leader convert to follower
		rf.mu.Lock()
		if rf.state != ST_LEADER || rf.CurrentTerm != leaderTerm {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		// Send heartbeats or AppendEntries
		for server := range rf.peers {
			if server != rf.me {
				go rf.sendAppendEntries(server, false)
			}
		}
		// Sleep for 100 milliseconds, or skip sleep message come
		select {
		case <-rf.skipSleepCh:
		case <-time.After(100 * time.Millisecond):
		}
	}

	RaftDPrintf("[%v] stop leader process with term %v", rf.me, leaderTerm)
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
	args.LastLogIndex = rf.LogIndexAt(rf.LogMax())
	args.LastLogTerm = rf.LogTermAt(rf.LogMax())
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !ok {
		// RaftDPrintf("[%v] send RequestVote to [%v] timeout", rf.me, server)
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
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		RaftDPrintf(
			"[%v] send RequestVote, newer term %v from [%v], lose election",
			rf.me, reply.Term, server,
		)
	} else if reply.VoteGranted {
		// Update agreed votes
		atomic.AddInt32(&rf.et.agreed, 1)
		RaftDPrintf("[%v] voted from [%v] with %+v", rf.me, server, args)
		if atomic.LoadInt32(&rf.et.agreed) > int32((len(rf.peers)>>1)) &&
			rf.state == ST_CANDIDATE {
			// Win election, convert to leader and start leaderProcess
			rf.state = ST_LEADER
			go rf.StartLeaderProcess()
			RaftDPrintf(
				"[%v] send RequestVote, win election with term %v",
				rf.me, rf.CurrentTerm,
			)
		}
	} else {
		// Update disagreed votes
		atomic.AddInt32(&rf.et.disagreed, 1)
		RaftDPrintf("[%v] not voted from [%v] with %+v", rf.me, server, args)
		if atomic.LoadInt32(&rf.et.disagreed) > int32(len(rf.peers)>>1) &&
			rf.state == ST_CANDIDATE {
			// Lose election, convert to follower
			rf.state = ST_FOLLOWER
			RaftDPrintf(
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
	RaftDPrintf("[%v] start election with term %v", rf.me, rf.CurrentTerm)
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

	RaftDPrintf("[%v] lose election, timeout", rf.me)
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
	// Return false if server is not leader
	if rf.state != ST_LEADER {
		rf.mu.Unlock()
		return index, term, false
	}
	newEntry := LogEntry{}
	newEntry.Index = rf.LogMax() + 1
	newEntry.Term = rf.CurrentTerm
	newEntry.Command = command
	rf.Log = append(rf.Log, newEntry)
	rf.persist()
	rf.mu.Unlock()

	go func() { rf.skipSleepCh <- true }()

	RaftDPrintf(
		"[%v] Call Start, append {index: %v, term: %v, cmd: %v}, log size %v",
		rf.me, newEntry.Index, newEntry.Term, newEntry.Command, rf.LogMax(),
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
	RaftDPrintf("[%v] is killed\n", rf.me)
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readPersistSnapshot(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine
	go rf.EndlessApply()
	go rf.SignalPeriodically()

	return rf
}

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}
