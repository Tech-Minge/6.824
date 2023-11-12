package shardctrler

//
// Shardctrler clerk.
//

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
	// Your data here.
	me        int
	leaderId  int
	requestId int32
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
	// Your code here.
	ck.me = int(atomic.AddInt32(&ckId, 1))
	ck.leaderId = -1
	ck.requestId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {

	// Your code here.
	iter := 0
	var serverId int
	rq_id := int(atomic.AddInt32(&ck.requestId, 1))
	raft.Debug(raft.DSClerk, "SClerk%d ready to Query config %d", ck.me, num)

	for {
		args := QueryArgs{num, ck.me, rq_id}
		var reply QueryReply
		// try each known server.
		if ck.leaderId == -1 {
			serverId = iter % len(ck.servers)
			iter++
			raft.Debug(raft.DSClerk, "SClerk%d don't know leader, iter with Query config %d", ck.me, num)
		} else {
			serverId = ck.leaderId
			raft.Debug(raft.DSClerk, "SClerk%d kown leader, send Query config %d", ck.me, num)
		}

		ok := ck.servers[serverId].Call("ShardCtrler.Query", &args, &reply)
		if !ok {
			ck.leaderId = -1
			raft.Debug(raft.DSClerk, "SClerk%d send Query config %d timeout", ck.me, num)
			continue
		}

		if reply.WrongLeader {
			ck.leaderId = -1
		} else {
			ck.leaderId = serverId
			raft.Debug(raft.DSClerk, "SClerk%d Query config %d OK", ck.me, num)
			return reply.Config
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {

	iter := 0
	var serverId int
	rq_id := int(atomic.AddInt32(&ck.requestId, 1))
	server_len := len(servers)
	raft.Debug(raft.DSClerk, "SClerk%d ready to Join with server len %d", ck.me, server_len)

	for {
		args := JoinArgs{servers, ck.me, rq_id}
		var reply JoinReply
		// try each known server.
		if ck.leaderId == -1 {
			serverId = iter % len(ck.servers)
			iter++
			raft.Debug(raft.DSClerk, "SClerk%d don't know leader, iter with Join server len %d", ck.me, server_len)
		} else {
			serverId = ck.leaderId
			raft.Debug(raft.DSClerk, "SClerk%d kown leader, send Join server len %d", ck.me, server_len)
		}

		ok := ck.servers[serverId].Call("ShardCtrler.Join", &args, &reply)
		if !ok {
			ck.leaderId = -1
			raft.Debug(raft.DSClerk, "SClerk%d send Join server len %d timeout", ck.me, server_len)
			continue
		}

		if reply.WrongLeader {
			ck.leaderId = -1
		} else {
			ck.leaderId = serverId
			raft.Debug(raft.DSClerk, "SClerk%d Join server len %d OK", ck.me, server_len)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {

	iter := 0
	var serverId int
	rq_id := int(atomic.AddInt32(&ck.requestId, 1))
	gid_len := len(gids)
	raft.Debug(raft.DSClerk, "SClerk%d ready to Leave with gid len %d", ck.me, gid_len)

	for {
		args := LeaveArgs{gids, ck.me, rq_id}
		var reply LeaveReply
		// try each known server.
		if ck.leaderId == -1 {
			serverId = iter % len(ck.servers)
			iter++
			raft.Debug(raft.DSClerk, "SClerk%d don't know leader, iter with Leave gid len %d", ck.me, gid_len)
		} else {
			serverId = ck.leaderId
			raft.Debug(raft.DSClerk, "SClerk%d kown leader, send Leave gid len %d", ck.me, gid_len)
		}

		ok := ck.servers[serverId].Call("ShardCtrler.Leave", &args, &reply)
		if !ok {
			ck.leaderId = -1
			raft.Debug(raft.DSClerk, "SClerk%d send Leave gid len %d timeout", ck.me, gid_len)
			continue
		}

		if reply.WrongLeader {
			ck.leaderId = -1
		} else {
			ck.leaderId = serverId
			raft.Debug(raft.DSClerk, "SClerk%d Leave gid len %d OK", ck.me, gid_len)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {

	iter := 0
	var serverId int
	rq_id := int(atomic.AddInt32(&ck.requestId, 1))
	raft.Debug(raft.DSClerk, "SClerk%d ready to Move with shard %d and gid %d", ck.me, shard, gid)

	for {
		args := MoveArgs{shard, gid, ck.me, rq_id}
		var reply MoveReply
		// try each known server.
		if ck.leaderId == -1 {
			serverId = iter % len(ck.servers)
			iter++
			raft.Debug(raft.DSClerk, "SClerk%d don't know leader, iter with Move shard %d and gid %d", ck.me, shard, gid)
		} else {
			serverId = ck.leaderId
			raft.Debug(raft.DSClerk, "SClerk%d kown leader, send Move shard %d and gid %d", ck.me, shard, gid)
		}

		ok := ck.servers[serverId].Call("ShardCtrler.Move", &args, &reply)
		if !ok {
			ck.leaderId = -1
			raft.Debug(raft.DSClerk, "SClerk%d send Move shard %d and gid %d timeout", ck.me, shard, gid)
			continue
		}

		if reply.WrongLeader {
			ck.leaderId = -1
		} else {
			ck.leaderId = serverId
			raft.Debug(raft.DSClerk, "SClerk%d Move shard %d and gid %d OK", ck.me, shard, gid)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
