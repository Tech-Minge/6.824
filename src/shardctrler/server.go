package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Result struct {
	RequestId int
	Con       Config // for Query only
}

type Gid2Count struct {
	gid         int
	shard_count int
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead                int32
	configs             []Config // indexed by config num
	lastApplyIndex      int
	persister           *raft.Persister
	pendingRequestCount int
	nextResultIndex     map[int]int // next index to store result
	cacheRequestNum     int
	result              map[int][]Result // clerk id -> result (circle buffer)
	condForApply        *sync.Cond
}

type OpType int

const JOIN OpType = 0
const LEAVE OpType = 1
const MOVE OpType = 2
const QUERY OpType = 3

type Op struct {
	// Your data here.
	Type      OpType
	Servers   map[int][]string // For Join, new GID -> servers
	GidSlice  []int            // For Leave
	Shard     int              // For Move
	Gid       int              // For Move
	ConfigNum int              // For Query
	ClerkId   int
	RequestId int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var leader bool
	var init_term int
	var cmd_index int
	server_len := len(args.Servers)
	if _, leader = sc.rf.GetState(); !leader {
		reply.WrongLeader = true
		raft.Debug(raft.DSCtrl, "SCtrl%d receive Join RPC with server len %d, but not leader(early check)", sc.me, server_len)
		return
	}

	sc.mu.Lock()
	sc.pendingRequestCount++
	res_index := sc.getRequestResult(args.ClerkId, args.RequestId)
	if res_index == -1 {
		op := Op{JOIN, args.Servers, []int{}, -1, -1, -1, args.ClerkId, args.RequestId}
		cmd_index, init_term, leader = sc.rf.Start(op)

		// check leader
		if !leader {
			reply.WrongLeader = true
			raft.Debug(raft.DSCtrl, "SCtrl%d receive Join RPC with server len %d, but not leader", sc.me, server_len)
			sc.pendingRequestCount--
			sc.mu.Unlock()
			return
		}
		raft.Debug(raft.DSCtrl, "SCtrl%d know raft maybe place Join RPC at index %d with server len %d", sc.me, cmd_index, server_len)

		for res_index == -1 {
			sc.condForApply.Wait()
			// maybe check term is enough, no need to check is leader or not
			if curr_term, curr_leader := sc.rf.GetState(); !curr_leader || curr_term != init_term {
				reply.WrongLeader = true
				raft.Debug(raft.DSCtrl, "SCtrl%d wait Join RPC to finish with SClerk%d request id %d, server len %d, but leader %t, current term %d, initial term %d", sc.me, args.ClerkId, args.RequestId, server_len, curr_leader, curr_term, init_term)
				sc.pendingRequestCount--
				sc.mu.Unlock()
				return
			}
			res_index = sc.getRequestResult(args.ClerkId, args.RequestId)
		}
	} else {
		raft.Debug(raft.DSCtrl, "SCtrl%d know Join RPC with SClerk%d request id %d duplicate(early check)", sc.me, args.ClerkId, args.RequestId)
	}

	sc.pendingRequestCount--
	sc.mu.Unlock()

	reply.WrongLeader = false
	raft.Debug(raft.DSCtrl, "SCtrl%d know Join RPC finish with SClerk%d request id %d, server len %d in success", sc.me, args.ClerkId, args.RequestId, server_len)

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var leader bool
	var init_term int
	var cmd_index int
	gid_len := len(args.GIDs)
	if _, leader = sc.rf.GetState(); !leader {
		reply.WrongLeader = true
		raft.Debug(raft.DSCtrl, "SCtrl%d receive Leave RPC with gid len %d, but not leader(early check)", sc.me, gid_len)
		return
	}

	sc.mu.Lock()
	sc.pendingRequestCount++
	res_index := sc.getRequestResult(args.ClerkId, args.RequestId)
	if res_index == -1 {
		op := Op{LEAVE, map[int][]string{}, args.GIDs, -1, -1, -1, args.ClerkId, args.RequestId}
		cmd_index, init_term, leader = sc.rf.Start(op)

		// check leader
		if !leader {
			reply.WrongLeader = true
			raft.Debug(raft.DSCtrl, "SCtrl%d receive Leave RPC with gid len %d, but not leader", sc.me, gid_len)
			sc.pendingRequestCount--
			sc.mu.Unlock()
			return
		}
		raft.Debug(raft.DSCtrl, "SCtrl%d know raft maybe place Leave RPC at index %d with gid len %d", sc.me, cmd_index, gid_len)

		for res_index == -1 {
			sc.condForApply.Wait()
			// maybe check term is enough, no need to check is leader or not
			if curr_term, curr_leader := sc.rf.GetState(); !curr_leader || curr_term != init_term {
				reply.WrongLeader = true
				raft.Debug(raft.DSCtrl, "SCtrl%d wait Leave RPC to finish with SClerk%d request id %d, gid len %d, but leader %t, current term %d, initial term %d", sc.me, args.ClerkId, args.RequestId, gid_len, curr_leader, curr_term, init_term)
				sc.pendingRequestCount--
				sc.mu.Unlock()
				return
			}
			res_index = sc.getRequestResult(args.ClerkId, args.RequestId)
		}
	} else {
		raft.Debug(raft.DSCtrl, "SCtrl%d know Leave RPC with SClerk%d request id %d duplicate(early check)", sc.me, args.ClerkId, args.RequestId)
	}

	sc.pendingRequestCount--
	sc.mu.Unlock()

	reply.WrongLeader = false
	raft.Debug(raft.DSCtrl, "SCtrl%d know Leave RPC finish with SClerk%d request id %d, gid len %d in success", sc.me, args.ClerkId, args.RequestId, gid_len)

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var leader bool
	var init_term int
	var cmd_index int

	if _, leader = sc.rf.GetState(); !leader {
		reply.WrongLeader = true
		raft.Debug(raft.DSCtrl, "SCtrl%d receive Move RPC with shard %d and gid %d, but not leader(early check)", sc.me, args.Shard, args.GID)
		return
	}

	sc.mu.Lock()
	sc.pendingRequestCount++
	res_index := sc.getRequestResult(args.ClerkId, args.RequestId)
	if res_index == -1 {
		op := Op{MOVE, map[int][]string{}, []int{}, args.Shard, args.GID, -1, args.ClerkId, args.RequestId}
		cmd_index, init_term, leader = sc.rf.Start(op)

		// check leader
		if !leader {
			reply.WrongLeader = true
			raft.Debug(raft.DSCtrl, "SCtrl%d receive Move RPC with shard %d and gid %d, but not leader", sc.me, args.Shard, args.GID)
			sc.pendingRequestCount--
			sc.mu.Unlock()
			return
		}
		raft.Debug(raft.DSCtrl, "SCtrl%d know raft maybe place Move RPC at index %d with shard %d and gid %d", sc.me, cmd_index, args.Shard, args.GID)

		for res_index == -1 {
			sc.condForApply.Wait()
			// maybe check term is enough, no need to check is leader or not
			if curr_term, curr_leader := sc.rf.GetState(); !curr_leader || curr_term != init_term {
				reply.WrongLeader = true
				raft.Debug(raft.DSCtrl, "SCtrl%d wait Move RPC to finish with SClerk%d request id %d, shard %d and gid %d, but leader %t, current term %d, initial term %d", sc.me, args.ClerkId, args.RequestId, args.Shard, args.GID, curr_leader, curr_term, init_term)
				sc.pendingRequestCount--
				sc.mu.Unlock()
				return
			}
			res_index = sc.getRequestResult(args.ClerkId, args.RequestId)
		}
	} else {
		raft.Debug(raft.DSCtrl, "SCtrl%d know Move RPC with SClerk%d request id %d duplicate(early check)", sc.me, args.ClerkId, args.RequestId)
	}

	sc.pendingRequestCount--
	sc.mu.Unlock()

	reply.WrongLeader = false
	raft.Debug(raft.DSCtrl, "SCtrl%d know Move RPC finish with SClerk%d request id %d, shard %d and gid %d in success", sc.me, args.ClerkId, args.RequestId, args.Shard, args.GID)

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// Your code here.
	var leader bool
	var init_term int
	var cmd_index int
	if _, leader = sc.rf.GetState(); !leader {
		reply.WrongLeader = true
		raft.Debug(raft.DSCtrl, "SCtrl%d receive Query RPC with config num %d, but not leader(early check)", sc.me, args.Num)
		return
	}

	sc.mu.Lock()
	// if not latest config query, directly reply
	if args.Num != -1 && args.Num < len(sc.configs) {
		reply.WrongLeader = false
		reply.Config = sc.configs[args.Num]
		raft.Debug(raft.DSCtrl, "SCtrl%d receive Query RPC with previous config num %d, early reply", sc.me, args.Num)
		sc.mu.Unlock()
		return
	}
	sc.pendingRequestCount++
	res_index := sc.getRequestResult(args.ClerkId, args.RequestId)
	if res_index == -1 {
		op := Op{QUERY, map[int][]string{}, []int{}, -1, -1, args.Num, args.ClerkId, args.RequestId}
		cmd_index, init_term, leader = sc.rf.Start(op)

		// check leader
		if !leader {
			reply.WrongLeader = true
			raft.Debug(raft.DSCtrl, "SCtrl%d receive Query RPC with config num %d, but not leader", sc.me, args.Num)
			sc.pendingRequestCount--
			sc.mu.Unlock()
			return
		}
		raft.Debug(raft.DSCtrl, "SCtrl%d know raft maybe place Query RPC at index %d with config num %d", sc.me, cmd_index, args.Num)

		for res_index == -1 {
			sc.condForApply.Wait()
			// maybe check term is enough, no need to check is leader or not
			if curr_term, curr_leader := sc.rf.GetState(); !curr_leader || curr_term != init_term {
				reply.WrongLeader = true
				raft.Debug(raft.DSCtrl, "SCtrl%d wait Query RPC to finish with SClerk%d request id %d, config num %d, but leader %t, current term %d, initial term %d", sc.me, args.ClerkId, args.RequestId, args.Num, curr_leader, curr_term, init_term)
				sc.pendingRequestCount--
				sc.mu.Unlock()
				return
			}
			res_index = sc.getRequestResult(args.ClerkId, args.RequestId)
		}
	} else {
		raft.Debug(raft.DSCtrl, "SCtrl%d know Query RPC with SClerk%d request id %d duplicate(early check)", sc.me, args.ClerkId, args.RequestId)
	}
	res := sc.result[args.ClerkId][res_index]
	sc.pendingRequestCount--
	sc.mu.Unlock()

	reply.WrongLeader = false
	reply.Config = res.Con
	raft.Debug(raft.DSCtrl, "SCtrl%d know Query RPC finish with SClerk%d request id %d, config num %d in success", sc.me, args.ClerkId, args.RequestId, args.Num)

}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// check Op duplicate
func (sc *ShardCtrler) getRequestResult(clerk_id int, request_id int) int {
	res_slice, ok := sc.result[clerk_id]
	if !ok {
		sc.result[clerk_id] = make([]Result, sc.cacheRequestNum) // default request id is 0, so client start with 1
		sc.nextResultIndex[clerk_id] = 0
	}
	for i := 0; i < len(res_slice); i++ {
		if res_slice[i].RequestId == request_id {
			return i
		}
	}
	return -1
}

func (sc *ShardCtrler) joinHandler(servers map[int][]string) {
	con_len := len(sc.configs)
	last_con := sc.configs[con_len-1]
	con := Config{con_len, last_con.Shards, make(map[int][]string)}

	// copy previous group
	shard_count := make(map[int]int) // gid -> shard count
	for k, v := range last_con.Groups {
		con.Groups[k] = v
		shard_count[k] = 0
	}

	receiver_gid := []int{}
	for k, v := range servers {
		con.Groups[k] = v
		shard_count[k] = 0
	}

	can_be_used := []int{} // index of shard that can be used
	allowed_more_than1 := len(con.Shards) % len(con.Groups)
	average := len(con.Shards) / len(con.Groups)
	for i := 0; i < len(con.Shards); i++ {
		if con.Shards[i] == 0 {
			// invalid
			can_be_used = append(can_be_used, i)
			continue
		}
		shard_count[con.Shards[i]]++
		if shard_count[con.Shards[i]] > average {
			if shard_count[con.Shards[i]] == average+1 && allowed_more_than1 > 0 {
				allowed_more_than1--
			} else {
				can_be_used = append(can_be_used, i)
			}
		}
	}

	// iter with ordered kv pair
	var ordered_shard_count []Gid2Count
	for k, v := range shard_count {
		ordered_shard_count = append(ordered_shard_count, Gid2Count{k, v})
	}
	sort.Slice(ordered_shard_count, func(i, j int) bool {
		return ordered_shard_count[i].shard_count > ordered_shard_count[j].shard_count ||
			(ordered_shard_count[i].shard_count == ordered_shard_count[j].shard_count &&
				ordered_shard_count[i].gid > ordered_shard_count[j].gid)
	})

	for i := 0; i < len(ordered_shard_count); i++ {
		k := ordered_shard_count[i].gid
		v := ordered_shard_count[i].shard_count
		if v < average {
			receiver_gid = append(receiver_gid, k)
			extra := 0
			if allowed_more_than1 > 0 {
				extra = 1
				allowed_more_than1--
			}
			shard_count[k] = average - v + extra
		} else if allowed_more_than1 > 0 && shard_count[k] == average {
			allowed_more_than1--
			receiver_gid = append(receiver_gid, k)
			shard_count[k] = 1
		}
	}

	iter_avail_shard := 0

	for i := 0; i < len(receiver_gid); i++ {
		for j := 0; j < shard_count[receiver_gid[i]]; j++ {
			con.Shards[can_be_used[iter_avail_shard]] = receiver_gid[i]
			iter_avail_shard++
		}
	}

	if iter_avail_shard < len(can_be_used) {
		panic("Join can't use all available shard")
	}
	if allowed_more_than1 > 0 {
		panic("Join remainder error")
	}

	raft.Debug(raft.DSCtrl, "SCtrl%d receive Join with server %v, previous shard %v, current shard %v", sc.me, servers, last_con.Shards, con.Shards)
	sc.configs = append(sc.configs, con)
}

func (sc *ShardCtrler) moveHandler(shard, gid int) {
	con_len := len(sc.configs)
	last_con := sc.configs[con_len-1]
	con := Config{con_len, last_con.Shards, make(map[int][]string)}
	// copy previous group
	for k, v := range last_con.Groups {
		con.Groups[k] = v
	}

	con.Shards[shard] = gid
	raft.Debug(raft.DSCtrl, "SCtrl%d receive Move with shard %d and gid %d, previous shard %v, current shard %v", sc.me, shard, gid, last_con.Shards, con.Shards)
	sc.configs = append(sc.configs, con)
}

func (sc *ShardCtrler) leaveHandler(gid []int) {
	con_len := len(sc.configs)
	last_con := sc.configs[con_len-1]
	con := Config{con_len, last_con.Shards, make(map[int][]string)}

	// copy previous group
	shard_count := make(map[int]int) // gid -> shard count
	for k, v := range last_con.Groups {
		con.Groups[k] = v
		shard_count[k] = 0
	}
	delete_gid := map[int]bool{}
	for i := 0; i < len(gid); i++ {
		delete(con.Groups, gid[i])
		delete(shard_count, gid[i])
		delete_gid[gid[i]] = true
	}

	if len(con.Groups) == 0 {
		// all group leave
		for i := 0; i < len(con.Shards); i++ {
			con.Shards[i] = 0
		}

		// don't forget to append
		sc.configs = append(sc.configs, con)
		return
	}
	receiver_gid := []int{}
	can_be_used := []int{} // index of shard that can be used
	allowed_more_than1 := len(con.Shards) % len(con.Groups)
	average := len(con.Shards) / len(con.Groups)
	for i := 0; i < len(con.Shards); i++ {
		if con.Shards[i] == 0 {
			// invalid
			can_be_used = append(can_be_used, i)
			continue
		} else if _, ok := delete_gid[con.Shards[i]]; ok {
			can_be_used = append(can_be_used, i)
			continue
		}

		shard_count[con.Shards[i]]++
		if shard_count[con.Shards[i]] > average {
			if shard_count[con.Shards[i]] == average+1 && allowed_more_than1 > 0 {
				allowed_more_than1--
			} else {
				can_be_used = append(can_be_used, i)
			}
		}
	}

	// iter with ordered kv pair
	var ordered_shard_count []Gid2Count
	for k, v := range shard_count {
		ordered_shard_count = append(ordered_shard_count, Gid2Count{k, v})
	}
	sort.Slice(ordered_shard_count, func(i, j int) bool {
		return ordered_shard_count[i].shard_count > ordered_shard_count[j].shard_count ||
			(ordered_shard_count[i].shard_count == ordered_shard_count[j].shard_count &&
				ordered_shard_count[i].gid > ordered_shard_count[j].gid)
	})

	for i := 0; i < len(ordered_shard_count); i++ {
		k := ordered_shard_count[i].gid
		v := ordered_shard_count[i].shard_count
		if v < average {
			extra := 0
			if allowed_more_than1 > 0 {
				extra = 1
				allowed_more_than1--
			}
			receiver_gid = append(receiver_gid, k)
			shard_count[k] = average - v + extra
		} else if allowed_more_than1 > 0 && shard_count[k] == average {
			allowed_more_than1--
			receiver_gid = append(receiver_gid, k)
			shard_count[k] = 1
		}
	}

	iter_avail_shard := 0
	for i := 0; i < len(receiver_gid); i++ {
		for j := 0; j < shard_count[receiver_gid[i]]; j++ {
			con.Shards[can_be_used[iter_avail_shard]] = receiver_gid[i]
			iter_avail_shard++
		}
	}

	if iter_avail_shard < len(can_be_used) {
		panic("Leave can't use all available shard")
	}
	if allowed_more_than1 > 0 {
		panic("Leave remainder error")
	}
	raft.Debug(raft.DSCtrl, "SCtrl%d receive Leave with gid slice %v, previous shard %v, current shard %v", sc.me, gid, last_con.Shards, con.Shards)

	sc.configs = append(sc.configs, con)
}

func (sc *ShardCtrler) queryHandler(config int) Config {

	raft.Debug(raft.DSCtrl, "SCtrl%d receive Query with config num %d in handler, current total config len %d", sc.me, config, len(sc.configs))

	if config == -1 || config >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[config]
}

func (sc *ShardCtrler) applyCommittedLog() {
	for !sc.killed() {
		applymsg := <-sc.applyCh
		if applymsg.CommandValid {
			// commnad
			op, ok := applymsg.Command.(Op)
			if !ok {
				panic("Apply Channel Error")
			}
			cmd_index := applymsg.CommandIndex
			sc.mu.Lock()
			if sc.lastApplyIndex >= cmd_index {
				panic("ShardCtrler cmd index too small")
			}
			sc.lastApplyIndex = cmd_index
			may_duplicate_index := sc.getRequestResult(op.ClerkId, op.RequestId)
			if may_duplicate_index != -1 {
				raft.Debug(raft.DSCtrl, "SCtrl%d find Duplicate RPC with C%d and request id %d, command index %d, duplicate index %d", sc.me, op.ClerkId, op.RequestId, cmd_index, may_duplicate_index)
				sc.mu.Unlock()
				continue
			}

			raft.Debug(raft.DSCtrl, "SCtrl%d apply committed command index %d with C%d request id %d", sc.me, cmd_index, op.ClerkId, op.RequestId)
			res_index := sc.nextResultIndex[op.ClerkId]
			sc.nextResultIndex[op.ClerkId] = (sc.nextResultIndex[op.ClerkId] + 1) % sc.cacheRequestNum
			ret_con := Config{}
			switch op.Type {
			case JOIN:
				sc.joinHandler(op.Servers)
			case MOVE:
				sc.moveHandler(op.Shard, op.Gid)
			case LEAVE:
				sc.leaveHandler(op.GidSlice)
			case QUERY:
				ret_con = sc.queryHandler(op.ConfigNum)
			default:
				panic("ShardCrtler receive unknow operation type")
			}
			sc.result[op.ClerkId][res_index] = Result{op.RequestId, ret_con}

			// notify
			sc.condForApply.Broadcast()
			sc.mu.Unlock()
		} else if applymsg.SnapshotValid {
			// snapshot
			panic("ShardCtrler receive snapshot operation")
		}

	}
}

// check pending count when no longer is leader/term change
func (sc *ShardCtrler) notifier() {
	term := 0
	for !sc.killed() {
		time.Sleep(time.Millisecond * 100)
		curr_term, leader := sc.rf.GetState()
		if leader && curr_term == term {
			continue
		}

		sc.mu.Lock()
		if sc.pendingRequestCount > 0 {
			raft.Debug(raft.DSCtrl, "SCtrl%d has pending request count %d, leader %t, current term %d, previous check term %d, notify it to reply", sc.me, sc.pendingRequestCount, leader, curr_term, term)
			term = curr_term
			sc.condForApply.Broadcast()
		}
		sc.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplyIndex = 0
	sc.persister = persister
	sc.pendingRequestCount = 0
	sc.nextResultIndex = make(map[int]int)
	sc.cacheRequestNum = 3
	sc.result = make(map[int][]Result)
	sc.condForApply = sync.NewCond(&sc.mu)

	go sc.applyCommittedLog()
	go sc.notifier()

	return sc
}
