package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/raft"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me        int
	leaderId  int
	requestId int32 // lock needed??
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var ckId int32 = 0

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = int(atomic.AddInt32(&ckId, 1))
	ck.leaderId = -1
	ck.requestId = 0 // default cycle buffer is 0 in KV
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
	iter := 0
	var serverId int
	rq_id := int(atomic.AddInt32(&ck.requestId, 1))
	raft.Debug(raft.DKV, "C%d ready to send Get RPC request id %d", ck.me, rq_id)
	for {
		args := GetArgs{key, ck.me, rq_id}
		var reply GetReply
		if ck.leaderId == -1 {
			time.Sleep(time.Millisecond * 100)
			serverId = iter % len(ck.servers)
			iter++
			raft.Debug(raft.DClerk, "C%d don't know leader, iter with Get RPC Key %s", ck.me, key)
		} else {
			serverId = ck.leaderId
			raft.Debug(raft.DClerk, "C%d know leader id %d send Get RPC with Key %s", ck.me, serverId, key)
		}

		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		if !ok {
			raft.Debug(raft.DClerk, "C%d send Get RPC timeout with Key %s", ck.me, key)
			continue
		}
		if reply.Err != ErrWrongLeader {
			// if no key, "" is returned
			ck.leaderId = serverId
			raft.Debug(raft.DClerk, "C%d Get OK, Key %s", ck.me, key)
			return reply.Value
		} else {
			ck.leaderId = -1
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

	iter := 0
	var serverId int
	rq_id := int(atomic.AddInt32(&ck.requestId, 1))
	raft.Debug(raft.DKV, "C%d ready to send PutAppend RPC request id %d", ck.me, rq_id)
	for {
		args := PutAppendArgs{key, value, op, ck.me, rq_id}
		var reply PutAppendReply
		if ck.leaderId == -1 {
			time.Sleep(time.Millisecond * 100)
			serverId = iter % len(ck.servers)
			iter++
			raft.Debug(raft.DClerk, "C%d don't know leader, iter with PutAppend RPC Key %s", ck.me, key)
		} else {
			serverId = ck.leaderId
			raft.Debug(raft.DClerk, "C%d know leader id, send PutAppend RPC with Key %s", ck.me, key)
		}

		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			raft.Debug(raft.DClerk, "C%d send PutAppend RPC timeout with Key %s", ck.me, key)
			continue
		}
		if reply.Err != ErrWrongLeader {
			ck.leaderId = serverId
			raft.Debug(raft.DClerk, "C%d PutAppend OK, Key %s", ck.me, args.Key)
			break
		} else {
			ck.leaderId = -1
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
