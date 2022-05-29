package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int
	leaderId int
	cmdseq   int
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
	// You'll have to add code here.
	ck.clientId = int(nrand())
	ck.leaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.cmdseq++
	args := GetArgs{key, ck.clientId, ck.cmdseq}

	DPrintf("[%v] try to PutAppend %+v", ck.clientId, args)

	for ii := 1; ; ii++ {
		if ii%100 == 0 {
			DPrintf("[%v] tried Get %v times", ck.clientId, ii)
		}

		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			DPrintf("[%v] Get OK, args: %+v, return: %v", ck.clientId, args, reply.Value)
			return reply.Value
		case ErrNoKey:
			DPrintf("[%v] Get ErrNoKey, args: %+v", ck.clientId, args)
			return ""
		case ErrWrongLeader:
			// Tried a loop, wait for election finish
			if ii%len(ck.servers) == 0 {
				DPrintf("[%v] Get tried a loop, sleep a while", ck.clientId)
				time.Sleep(100 * time.Millisecond)
			}
			// Reset leaderId
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		case ErrTimeout:

		default:
			DPrintf("[%v] Get unexpected err %v", ck.clientId, reply.Err)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.cmdseq++
	args := PutAppendArgs{key, value, op, ck.clientId, ck.cmdseq}

	DPrintf("[%v] try to PutAppend %+v to [%v]", ck.clientId, args, ck.leaderId)

	for ii := 1; ; ii++ {
		if ii%100 == 0 {
			DPrintf("[%v] tried PutAppend to [%v] %v times", ck.clientId, ck.leaderId, ii)
		}

		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			DPrintf("[%v] PutAppend to [%v] OK, args: %+v", ck.clientId, ck.leaderId, args)
			return
		case ErrWrongLeader:
			time.Sleep(25 * time.Millisecond)
			// Reset leaderId
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		case ErrTimeout:
			time.Sleep(25 * time.Millisecond)
		default:
			DPrintf("[%v] PutAppend unexpected err %v", ck.clientId, reply.Err)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
