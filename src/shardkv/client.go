package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	me        int
	leaderId  map[int]int // gid -> leader id
	requestId int32
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
var ckId int32 = 0

func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.me = int(atomic.AddInt32(&ckId, 1))
	ck.leaderId = make(map[int]int)
	ck.requestId = 0 // default cycle buffer is 0 in KV
	return ck
}

// get group's leader id by key gid
func (ck *Clerk) getGidLeaderId(gid int) int {
	_, ok := ck.leaderId[gid]
	if !ok {
		ck.leaderId[gid] = -1
	}
	return ck.leaderId[gid]
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {

	rq_id := int(atomic.AddInt32(&ck.requestId, 1))
	raft.Debug(raft.DSKVClk, "SKC%d ready to send Get RPC request id %d with key %s", ck.me, rq_id, key)

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			raft.Debug(raft.DSKVClk, "SKC%d try to send Get RPC with request id %d and shard %d to group %d", ck.me, rq_id, shard, gid)
			// if know leader id, try it first
			leader_offset := ck.getGidLeaderId(gid)
			if leader_offset == -1 {
				// don't know leader, try all servers in index order
				leader_offset = 0
				raft.Debug(raft.DSKVClk, "SKC%d don't know group %d leader id", ck.me, gid)
			} else {
				raft.Debug(raft.DSKVClk, "SKC%d know group %d leader id %d", ck.me, gid, leader_offset)
			}
			for si := 0; si < len(servers); si++ {
				real_si := (si + leader_offset) % len(servers)
				srv := ck.make_end(servers[real_si])
				args := GetArgs{key, ck.me, rq_id, shard}
				var reply GetReply

				ok := srv.Call("ShardKV.Get", &args, &reply)
				if !ok {
					ck.leaderId[gid] = -1
					raft.Debug(raft.DSKVClk, "SKC%d send Get RPC with request id %d and shard %d timeout", ck.me, rq_id, shard)
					continue
				}

				if reply.Err == ErrWrongGroup {
					raft.Debug(raft.DSKVClk, "SKC%d send Get RPC with request id %d and shard %d to wrong group %d", ck.me, rq_id, shard, gid)
					break
				} else if reply.Err == ErrWrongLeader {
					raft.Debug(raft.DSKVClk, "SKC%d send Get RPC with request id %d and shard %d to corret group %d but wrong leader %d", ck.me, rq_id, shard, gid, real_si)
					ck.leaderId[gid] = -1
					time.Sleep(100 * time.Millisecond)
				} else {
					// OK or NoKey
					raft.Debug(raft.DSKVClk, "SKC%d send Get RPC with request id %d and shard %d finish OK", ck.me, rq_id, shard)
					ck.leaderId[gid] = real_si
					return reply.Value
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	rq_id := int(atomic.AddInt32(&ck.requestId, 1))
	raft.Debug(raft.DSKVClk, "SKC%d ready to send PutAppend RPC request id %d with key %s", ck.me, rq_id, key)

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			raft.Debug(raft.DSKVClk, "SKC%d try to send PutAppend RPC with request id %d and shard %d to group %d", ck.me, rq_id, shard, gid)
			// if know leader id, try it first
			leader_offset := ck.getGidLeaderId(gid)
			if leader_offset == -1 {
				// don't know leader, try all servers in index order
				leader_offset = 0
				raft.Debug(raft.DSKVClk, "SKC%d don't know group %d leader id", ck.me, gid)
			} else {
				raft.Debug(raft.DSKVClk, "SKC%d know group %d leader id %d", ck.me, gid, leader_offset)
			}
			for si := 0; si < len(servers); si++ {
				real_si := (si + leader_offset) % len(servers)
				srv := ck.make_end(servers[real_si])
				args := PutAppendArgs{key, value, op, ck.me, rq_id, shard}
				var reply PutAppendReply

				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if !ok {
					ck.leaderId[gid] = -1
					raft.Debug(raft.DSKVClk, "SKC%d send PutAppend RPC with request id %d and shard %d timeout", ck.me, rq_id, shard)
					continue
				}

				if reply.Err == ErrWrongGroup {
					raft.Debug(raft.DSKVClk, "SKC%d send PutAppend RPC with request id %d and shard %d to wrong group %d", ck.me, rq_id, shard, gid)
					break
				} else if reply.Err == ErrWrongLeader {
					raft.Debug(raft.DSKVClk, "SKC%d send PutAppend RPC with request id %d and shard %d to corret group %d but wrong leader %d", ck.me, rq_id, shard, gid, real_si)
					ck.leaderId[gid] = -1
					time.Sleep(100 * time.Millisecond)
				} else {
					// OK or NoKey
					raft.Debug(raft.DSKVClk, "SKC%d send PutAppend RPC with request id %d and shard %d finish OK", ck.me, rq_id, shard)
					ck.leaderId[gid] = real_si
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
