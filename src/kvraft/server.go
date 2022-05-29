package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const EXECUTE_TIMEOUT time.Duration = 200 * time.Millisecond

const (
	ST_INIT  int = 0
	ST_DOING int = 1
	ST_DONE  int = 2
)

const (
	OP_GET    int = 1
	OP_PUT    int = 2
	OP_APPEND int = 3
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   int
	OpKey    string
	OpValue  string
	ClientId int
	CmdSeq   int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap map[string]string

	replyMap map[int]int
	rpcChans map[int]chan string
}

func (kv *KVServer) HandleMsg(index int, term int, op *Op) string {
	ret := ""
	switch op.OpType {
	case OP_GET:
		ret = kv.kvMap[op.OpKey]
	case OP_PUT:
		if !kv.isRetransmitRPC(op.ClientId, op.CmdSeq) {
			kv.kvMap[op.OpKey] = op.OpValue
			kv.replyMap[op.ClientId] = op.CmdSeq
		}
	case OP_APPEND:
		if !kv.isRetransmitRPC(op.ClientId, op.CmdSeq) {
			_, ok := kv.kvMap[op.OpKey]
			if !ok {
				kv.kvMap[op.OpKey] = op.OpValue
			} else {
				kv.kvMap[op.OpKey] += op.OpValue
			}
			kv.replyMap[op.ClientId] = op.CmdSeq
		}
	default:
		DPrintf("[%v] HandleMsg unknown OpType %v", kv.me, op.OpType)
	}
	return ret
}

func (kv *KVServer) ApplyMsgDispatch() {
	DPrintf("[%v] start ApplyMsgDispatch", kv.me)
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				// Log applied
				cmd := msg.Command.(Op)
				DPrintf(
					"[%v] applied log {Index: %v, Cmd: %+v}",
					kv.me, msg.CommandIndex, cmd,
				)
				kv.mu.Lock()
				// Get notify channel from rpcChans
				ch, ok := kv.rpcChans[msg.CommandIndex]
				// Handle Get/PutAppend
				ret := kv.HandleMsg(msg.CommandIndex, msg.CommandTerm, &cmd)
				kv.mu.Unlock()
				if ok {
					// Notify RPC goroutine
					go func() { ch <- ret }()
				}
			} else if msg.SnapshotValid {
				// Snapshot applied
				DPrintf(
					"[%v] applied snapshot {Index: %v, Term: %v}",
					kv.me, msg.SnapshotIndex, msg.SnapshotTerm,
				)
			} else {
				// Should never happen!
				DPrintf("[%v] applied unknown type msg %+v", kv.me, msg)
				kv.Kill()
				return
			}
		case <-time.After(50 * time.Millisecond):
			// Do nothing
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	logCommand := Op{}
	logCommand.OpType = OP_GET
	logCommand.OpKey = args.Key

	kv.mu.Lock()
	logIndex, _, isLeader := kv.rf.Start(logCommand)
	// Register rpcChannel for current RPC handler
	ch := make(chan string)
	kv.rpcChans[logIndex] = ch
	kv.mu.Unlock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		goto unregister_and_return
	}

	// Wait for operation log apply
	select {
	case reply.Value = <-ch:
		reply.Err = OK
	case <-time.After(EXECUTE_TIMEOUT):
		reply.Err = ErrTimeout
	}
	DPrintf("[%v] Get, args: %+v, reply: %+v", kv.me, args, reply)

unregister_and_return:
	kv.mu.Lock()
	delete(kv.rpcChans, logIndex)
	kv.mu.Unlock()
}

func (kv *KVServer) isRetransmitRPC(clientId int, cmdSeq int) bool {
	if _, ok := kv.replyMap[clientId]; ok {
		return kv.replyMap[clientId] >= cmdSeq
	} else {
		return false
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	logCommand := Op{0, args.Key, args.Value, args.ClientId, args.CmdSeq}
	if args.Op == "Put" {
		logCommand.OpType = OP_PUT
	} else {
		logCommand.OpType = OP_APPEND
	}

	kv.mu.Lock()
	if kv.isRetransmitRPC(args.ClientId, args.CmdSeq) {
		// Reply OK to re-transmit RPC
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	logIndex, _, isLeader := kv.rf.Start(logCommand)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	// Register rpcChannel for current RPC handler
	ch := make(chan string)
	kv.rpcChans[logIndex] = ch
	kv.mu.Unlock()

	// Wait for operation log apply
	select {
	case <-ch:
		reply.Err = OK
	case <-time.After(EXECUTE_TIMEOUT):
		reply.Err = ErrTimeout
	}
	DPrintf("[%v] PutAppend, args: %+v, reply: %+v", kv.me, args, reply)

	kv.mu.Lock()
	delete(kv.rpcChans, logIndex)
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.replyMap = make(map[int]int)
	kv.rpcChans = make(map[int]chan string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.ApplyMsgDispatch()

	return kv
}
