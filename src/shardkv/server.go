package shardkv

import (
	"bytes"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type OpType int

const GET OpType = 0
const PUT OpType = 1
const APPEND OpType = 2
const TRANSTART OpType = 3
const TRANOK OpType = 4
const WAITOK OpType = 5
const NOOP OpType = 6

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type       OpType
	Key        string
	Value      string
	ShardSlice []int               // concerned with config relevant operation
	DBSlice    []map[string]string // concerned with config relevant operation
	ResultMap  map[int]Result
	ClerkId    int
	RequestId  int
	Shard      int // concerned with Get/Put/Append
}

type Executestatus int

const SUCCESS Executestatus = 0
const FAIL Executestatus = 1

type Result struct {
	RequestId int
	Shard     int
	Success   Executestatus
	Value     string // for Get only
}

type ShardState int

const OTHERS ShardState = 0   // other's shard
const MINE ShardState = 1     // my shard
const TRANSFER ShardState = 2 // previous mine, but need to transfer to others
const WAIT ShardState = 3     // previous others, but current mine, wait for others to transfer

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead                int32
	clerkOfContrller    *shardctrler.Clerk
	currentConfig       shardctrler.Config
	leaderId            map[int]int // gid -> leader id
	shardStatus         [shardctrler.NShards]ShardState
	waiting             bool
	lastShardOpTerm     int // config shard relevant term
	lastApplyIndex      int
	persister           *raft.Persister
	pendingRequestCount int
	nextResultIndex     map[int]int // next index to store result
	cacheRequestNum     int
	db                  [shardctrler.NShards]map[string]string
	result              map[int][]Result // clerk id -> result (circle buffer)
	condForApply        *sync.Cond
	condForConfig       *sync.Cond
}

func gid2ClerkId(gid int) int {
	return math.MaxInt/2 + gid
}

func clerkId2Gid(clerk int) int {
	return clerk - math.MaxInt/2
}

func (kv *ShardKV) getGidLeaderId(gid int) int {
	_, ok := kv.leaderId[gid]
	if !ok {
		kv.leaderId[gid] = -1
	}
	return kv.leaderId[gid]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var leader bool
	var init_term int
	var cmd_index int
	if _, leader = kv.rf.GetState(); !leader {
		reply.Err = ErrWrongLeader
		raft.Debug(raft.DSKV, "G%d SK%d receive Get RPC with Key %s, but not leader(early check)", kv.gid, kv.me, args.Key)
		return
	}

	kv.mu.Lock()
	if kv.shardStatus[args.Shard] == OTHERS || kv.shardStatus[args.Shard] == TRANSFER {
		reply.Err = ErrWrongGroup
		raft.Debug(raft.DSKV, "G%d SK%d receive Get RPC with Key %s and shard %d, but this shard belongs to others, transfer %t", kv.gid, kv.me, args.Key, args.Shard, kv.shardStatus[args.Shard] == TRANSFER)
		kv.mu.Unlock()
		return
	} else if kv.shardStatus[args.Shard] == WAIT {
		panic("Not Expected For Get")
	}
	kv.pendingRequestCount++
	res_index := kv.getRequestResult(args.ClerkId, args.RequestId)
	if res_index == -1 {
		op := Op{
			Type:      GET,
			Key:       args.Key,
			ClerkId:   args.ClerkId,
			RequestId: args.RequestId,
			Shard:     args.Shard,
		}
		cmd_index, init_term, leader = kv.rf.Start(op)

		// check leader
		if !leader {
			reply.Err = ErrWrongLeader
			raft.Debug(raft.DSKV, "G%d SK%d receive Get RPC with Key %s, but not leader", kv.gid, kv.me, args.Key)
			kv.pendingRequestCount--
			kv.mu.Unlock()
			return
		}
		raft.Debug(raft.DSKV, "G%d SK%d know raft maybe place Get RPC at index %d with Key %s, clerk id %d, request id %d and shard %d", kv.gid, kv.me, cmd_index, args.Key, args.ClerkId, args.RequestId, args.Shard)

		for res_index == -1 {
			raft.Debug(raft.DSKV, "G%d SK%d wait Get RPC for commit command index %d with clerk id %d, request id %d", kv.gid, kv.me, cmd_index, args.ClerkId, args.RequestId)
			kv.condForApply.Wait()
			res_index = kv.getRequestResult(args.ClerkId, args.RequestId)
			if res_index != -1 {
				break
			}
			if kv.lastApplyIndex >= cmd_index {
				reply.Err = ErrWrongGroup
				raft.Debug(raft.DSKV, "G%d SK%d previous OK for Get RPC with SKC%d request id %d, but later find not OK, skip apply", kv.gid, kv.me, args.ClerkId, args.RequestId)
				kv.pendingRequestCount--
				kv.mu.Unlock()
				return
			}
			// maybe check term is enough, no need to check is leader or not
			if curr_term, curr_leader := kv.rf.GetState(); !curr_leader || curr_term != init_term {
				reply.Err = ErrWrongLeader
				raft.Debug(raft.DSKV, "G%d SK%d wait Get RPC to finish with SKC%d request id %d, Key %s, but leader %t, current term %d, initial term %d", kv.gid, kv.me, args.ClerkId, args.RequestId, args.Key, curr_leader, curr_term, init_term)
				kv.pendingRequestCount--
				kv.mu.Unlock()
				return
			}
			res_index = kv.getRequestResult(args.ClerkId, args.RequestId)
		}
	} else {
		raft.Debug(raft.DSKV, "G%d SK%d know Get RPC with SKC%d request id %d duplicate(early check)", kv.gid, kv.me, args.ClerkId, args.RequestId)
	}
	res := kv.result[args.ClerkId][res_index]
	kv.pendingRequestCount--
	kv.mu.Unlock()

	reply.Value = res.Value
	if res.Success == SUCCESS {
		raft.Debug(raft.DSKV, "G%d SK%d know Get RPC finish with SKC%d request id %d, shard %d and Key %s in success", kv.gid, kv.me, args.ClerkId, args.RequestId, args.Shard, args.Key)
		reply.Err = OK
	} else {
		raft.Debug(raft.DSKV, "G%d SK%d know Get RPC finish with SKC%d request id %d, shard %d and Key %s but no such Key", kv.gid, kv.me, args.ClerkId, args.RequestId, args.Shard, args.Key)
		reply.Err = ErrNoKey
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var leader bool
	var init_term int
	var cmd_index int
	if _, leader = kv.rf.GetState(); !leader {
		reply.Err = ErrWrongLeader
		raft.Debug(raft.DSKV, "G%d SK%d receive PutAppend RPC with Key %s, but not leader(early check)", kv.gid, kv.me, args.Key)
		return
	}

	kv.mu.Lock()
	if kv.shardStatus[args.Shard] == OTHERS || kv.shardStatus[args.Shard] == TRANSFER {
		reply.Err = ErrWrongGroup
		raft.Debug(raft.DSKV, "G%d SK%d receive PutAppend RPC with Key %s and shard %d, but this shard belongs to others, transfer %t", kv.gid, kv.me, args.Key, args.Shard, kv.shardStatus[args.Shard] == TRANSFER)
		kv.mu.Unlock()
		return
	} else if kv.shardStatus[args.Shard] == WAIT {
		panic("Not Expected For PutAppend")
	}
	kv.pendingRequestCount++
	res_index := kv.getRequestResult(args.ClerkId, args.RequestId)
	if res_index == -1 {
		op := Op{
			Type:      PUT,
			Key:       args.Key,
			Value:     args.Value,
			ClerkId:   args.ClerkId,
			RequestId: args.RequestId,
			Shard:     args.Shard,
		}

		if args.Op != "Put" {
			op.Type = APPEND
		}
		cmd_index, init_term, leader = kv.rf.Start(op)

		// check leader
		if !leader {
			reply.Err = ErrWrongLeader
			raft.Debug(raft.DSKV, "G%d SK%d receive PutAppend RPC with Key %s, but not leader", kv.gid, kv.me, args.Key)
			kv.pendingRequestCount--
			kv.mu.Unlock()
			return
		}
		raft.Debug(raft.DSKV, "G%d SK%d know raft maybe place PutAppend RPC at index %d with Key %s, clerk id %d, request id %d and shard %d", kv.gid, kv.me, cmd_index, args.Key, args.ClerkId, args.RequestId, args.Shard)

		for res_index == -1 {
			raft.Debug(raft.DSKV, "G%d SK%d wait PutAppend RPC for commit command index %d with clerk id %d, request id %d", kv.gid, kv.me, cmd_index, args.ClerkId, args.RequestId)
			kv.condForApply.Wait()
			res_index = kv.getRequestResult(args.ClerkId, args.RequestId)
			if res_index != -1 {
				break
			}
			if kv.lastApplyIndex >= cmd_index {
				reply.Err = ErrWrongGroup
				raft.Debug(raft.DSKV, "G%d SK%d previous OK for PutAppend RPC with SKC%d request id %d, but later find not OK, skip apply", kv.gid, kv.me, args.ClerkId, args.RequestId)
				kv.pendingRequestCount--
				kv.mu.Unlock()
				return
			}
			if curr_term, curr_leader := kv.rf.GetState(); !curr_leader || curr_term != init_term {
				reply.Err = ErrWrongLeader
				raft.Debug(raft.DSKV, "G%d SK%d wait PutAppend RPC to finish with SKC%d request id %d, Key %s, but leader %t, current term %d, initial term %d", kv.gid, kv.me, args.ClerkId, args.RequestId, args.Key, curr_leader, curr_term, init_term)
				kv.pendingRequestCount--
				kv.mu.Unlock()
				return
			}
			res_index = kv.getRequestResult(args.ClerkId, args.RequestId)
		}
	} else {
		raft.Debug(raft.DSKV, "G%d SK%d know PutAppend RPC with SKC%d request id %d duplicate(early check)", kv.gid, kv.me, args.ClerkId, args.RequestId)
	}
	res := kv.result[args.ClerkId][res_index]
	kv.pendingRequestCount--
	kv.mu.Unlock()

	raft.Debug(raft.DSKV, "G%d SK%d know PutAppend RPC finish with SKC%d request id %d, shard %d and Key %s in success", kv.gid, kv.me, args.ClerkId, args.RequestId, args.Shard, args.Key)
	if res.Success != SUCCESS {
		panic("KVserver Put Append Fail")
	}
	reply.Err = OK
}

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {
	var leader bool
	var init_term int
	var cmd_index int
	gid := clerkId2Gid(args.ClerkId)

	if _, leader = kv.rf.GetState(); !leader {
		reply.Err = ErrWrongLeader
		raft.Debug(raft.DSKV, "G%d SK%d receive Transfer RPC with G%d config num %d, but not leader(early check)", kv.gid, kv.me, gid, args.RequestId)
		return
	}

	kv.mu.Lock()
	if args.RequestId <= kv.currentConfig.Num {
		reply.Err = OK
		raft.Debug(raft.DSKV, "G%d SK%d receive Transfer RPC from G%d but old config %d, current config %d", kv.gid, kv.me, gid, args.RequestId, kv.currentConfig.Num)
		kv.mu.Unlock()
		return
	} else if args.RequestId > kv.currentConfig.Num+1 {
		// it's ok
		raft.Debug(raft.DSKV, "G%d SK%d receive ahead Transfer RPC from G%d with its config %d, my %d", kv.gid, kv.me, gid, args.RequestId, kv.currentConfig.Num)
		for args.RequestId > kv.currentConfig.Num+1 {
			kv.condForConfig.Wait()
		}
		raft.Debug(raft.DSKV, "G%d SK%d receive ahead Transfer RPC from G%d with its config %d, but current OK, my config %d", kv.gid, kv.me, gid, args.RequestId, kv.currentConfig.Num)
	}

	if kv.allWaitOK(args.ShardSlice) {
		reply.Err = OK
		raft.Debug(raft.DSKV, "G%d SK%d receive Transfer RPC with G%d config num %d, but already finish transferring", kv.gid, kv.me, gid, args.RequestId)
		kv.mu.Unlock()
		return
	}

	kv.pendingRequestCount++
	res_index := kv.getRequestResult(args.ClerkId, args.RequestId)
	if res_index == -1 {
		op := Op{
			Type:       WAITOK,
			ShardSlice: args.ShardSlice,
			DBSlice:    args.ShardDB,
			ResultMap:  args.ResultMap,
			ClerkId:    args.ClerkId,
			RequestId:  args.RequestId,
		}
		cmd_index, init_term, leader = kv.rf.Start(op)

		// check leader
		if !leader {
			reply.Err = ErrWrongLeader
			raft.Debug(raft.DSKV, "G%d SK%d receive Transfer RPC with G%d config num %d, but not leader", kv.gid, kv.me, gid, args.RequestId)
			kv.pendingRequestCount--
			kv.mu.Unlock()
			return
		}
		raft.Debug(raft.DSKV, "G%d SK%d know raft maybe place Transfer RPC at index %d with G%d config num %d", kv.gid, kv.me, cmd_index, gid, args.RequestId)

		for res_index == -1 {
			kv.condForApply.Wait()
			if curr_term, curr_leader := kv.rf.GetState(); !curr_leader || curr_term != init_term {
				reply.Err = ErrWrongLeader
				raft.Debug(raft.DSKV, "G%d SK%d wait Transfer RPC to finish with G%d config num %d, but leader %t, current term %d, initial term %d", kv.gid, kv.me, gid, args.RequestId, curr_leader, curr_term, init_term)
				kv.pendingRequestCount--
				kv.mu.Unlock()
				return
			}
			res_index = kv.getRequestResult(args.ClerkId, args.RequestId)
		}
	} else {
		raft.Debug(raft.DSKV, "G%d SK%d know Transfer RPC with G%d config num %d duplicate(early check)", kv.gid, kv.me, gid, args.RequestId)
		panic("Transfer Result Error")
	}
	res := kv.result[args.ClerkId][res_index]
	kv.pendingRequestCount--
	kv.mu.Unlock()

	raft.Debug(raft.DSKV, "G%d SK%d know Transfer RPC finish with G%d config num %d, transfer slice %v in success", kv.gid, kv.me, gid, args.RequestId, args.ShardSlice)
	if res.Success != SUCCESS {
		panic("KVserver Transfer Fail")
	}
	reply.Err = OK
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// check Op duplicate
func (kv *ShardKV) getRequestResult(clerk_id int, request_id int) int {
	res_slice, ok := kv.result[clerk_id]
	if !ok {
		kv.result[clerk_id] = make([]Result, kv.cacheRequestNum) // default request id is 0, so client start with 1
		kv.nextResultIndex[clerk_id] = 0
	}
	for i := 0; i < len(res_slice); i++ {
		if res_slice[i].RequestId == request_id {
			return i
		}
	}
	return -1
}

func (kv *ShardKV) putLatestRequestResult(result map[int]Result) {
	for clerk_id, res := range result {
		_, ok := kv.result[clerk_id]
		if !ok {
			kv.result[clerk_id] = make([]Result, kv.cacheRequestNum) // default request id is 0, so client start with 1
			kv.nextResultIndex[clerk_id] = 0
		}
		next := kv.nextResultIndex[clerk_id]
		latest_index := (next - 1 + kv.cacheRequestNum) % kv.cacheRequestNum
		latest_rq_id := kv.result[clerk_id][latest_index].RequestId
		if latest_rq_id >= res.RequestId {
			raft.Debug(raft.DSKV, "G%d SK%d fail to install request result with shard %d, clerk id %d and request id %d, due to latest request id %d", kv.gid, kv.me, res.Shard, clerk_id, res.RequestId, latest_rq_id)
			continue
		}
		kv.result[clerk_id][next] = res
		kv.nextResultIndex[clerk_id] = (next + 1) % kv.cacheRequestNum
		raft.Debug(raft.DSKV, "G%d SK%d install request result with shard %d, clerk id %d and request id %d at index %d", kv.gid, kv.me, res.Shard, clerk_id, res.RequestId, next)
	}
}

func (kv *ShardKV) setShardState(shard_idx []int, status ShardState) {
	for _, v := range shard_idx {
		kv.shardStatus[v] = status
	}
}

func (kv *ShardKV) possibleWait(gid, target_config int) {
	if target_config <= kv.currentConfig.Num {
		panic("Config Too Small")
	}
	wait := false
	for target_config > kv.currentConfig.Num+1 {
		wait = true
		raft.Debug(raft.DSKV, "G%d SK%d wait due to encounter ahead config num %d from G%d while current config num %d", kv.gid, kv.me, target_config, gid, kv.currentConfig.Num)
		kv.condForConfig.Wait()
	}
	if wait {
		raft.Debug(raft.DSKV, "G%d SK%d finish wait, target config num %d from G%d and current config num %d", kv.gid, kv.me, target_config, gid, kv.currentConfig.Num)
	}
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		applymsg := <-kv.applyCh
		if applymsg.CommandValid {
			// commnad
			op, ok := applymsg.Command.(Op)
			if !ok {
				panic("Apply Channel Error")
			}
			cmd_index := applymsg.CommandIndex
			kv.mu.Lock()
			if kv.lastApplyIndex >= cmd_index {
				panic("Command Index Too Small")
			}

			possible_gid := clerkId2Gid(op.ClerkId)
			possible_config_num := op.RequestId
			if possible_gid == kv.gid {
				possible_config_num /= 2
			}

			may_duplicate_index := kv.getRequestResult(op.ClerkId, op.RequestId)
			if may_duplicate_index != -1 {
				if possible_gid < 0 {
					// not gid
					raft.Debug(raft.DSKV, "G%d SK%d find duplicate RPC with SKC%d and request id %d, command index %d, duplicate index %d", kv.gid, kv.me, op.ClerkId, op.RequestId, cmd_index, may_duplicate_index)
				} else {
					// gid and check config num
					raft.Debug(raft.DSKV, "G%d SK%d find duplicate RPC with G%d and config num %d with request id %d, command index %d, duplicate index %d", kv.gid, kv.me, possible_gid, possible_config_num, op.RequestId, cmd_index, may_duplicate_index)
				}
				// update last apply
				kv.lastApplyIndex = cmd_index
				kv.mu.Unlock()
				continue
			}

			if possible_gid < 0 {
				raft.Debug(raft.DSKV, "G%d SK%d ready to apply committed command index %d with SKC%d request id %d", kv.gid, kv.me, cmd_index, op.ClerkId, op.RequestId)
			} else {
				raft.Debug(raft.DSKV, "G%d SK%d ready to apply committed command index %d with G%d config num %d and request id %d", kv.gid, kv.me, cmd_index, possible_gid, possible_config_num, op.RequestId)
			}
			v, status := kv.db[op.Shard][op.Key]

			curr_res := Result{op.RequestId, op.Shard, SUCCESS, ""}
			skip := false
			switch op.Type {
			case GET:
				if kv.shardStatus[op.Shard] != MINE {
					skip = true
					raft.Debug(raft.DSKV, "G%d SK%d applier detect shard %d state %d, skip apply GET with SKC%d and request id %d", kv.gid, kv.me, op.Shard, kv.shardStatus[op.Shard], op.ClerkId, op.RequestId)
					break
				}
				if !status {
					curr_res.Success = FAIL
					raft.Debug(raft.DSKV, "G%d SK%d applier GET Key %s fail from SKC%d, request id %d with shard %d", kv.gid, kv.me, op.Key, op.ClerkId, op.RequestId, op.Shard)
				} else {
					curr_res.Value = v
					raft.Debug(raft.DSKV, "G%d SK%d applier GET Key %s and Value %s from SKC%d, request id %d with shard %d", kv.gid, kv.me, op.Key, v, op.ClerkId, op.RequestId, op.Shard)
				}
			case APPEND:
				if kv.shardStatus[op.Shard] != MINE {
					skip = true
					raft.Debug(raft.DSKV, "G%d SK%d applier detect shard %d state %d, skip apply APPEND with SKC%d and request id %d", kv.gid, kv.me, op.Shard, kv.shardStatus[op.Shard], op.ClerkId, op.RequestId)
					break
				}
				kv.db[op.Shard][op.Key] = v + op.Value
				raft.Debug(raft.DSKV, "G%d SK%d applier APPEND Key %s and Value %s from SKC%d, request id %d with shard %d", kv.gid, kv.me, op.Key, v+op.Value, op.ClerkId, op.RequestId, op.Shard)
			case PUT:
				if kv.shardStatus[op.Shard] != MINE {
					skip = true
					raft.Debug(raft.DSKV, "G%d SK%d applier detect shard %d state %d, skip apply PUT with SKC%d and request id %d", kv.gid, kv.me, op.Shard, kv.shardStatus[op.Shard], op.ClerkId, op.RequestId)
					break
				}
				kv.db[op.Shard][op.Key] = op.Value
				raft.Debug(raft.DSKV, "G%d SK%d applier PUT Key %s and Value %s from SKC%d, request id %d with shard %d", kv.gid, kv.me, op.Key, op.Value, op.ClerkId, op.RequestId, op.Shard)
			case NOOP:
				raft.Debug(raft.DSKV, "G%d SK%d applier receive NO-OP", kv.gid, kv.me)
			case WAITOK:
				// wait in case of config too ahead!
				kv.possibleWait(possible_gid, possible_config_num)
				for i := 0; i < len(op.ShardSlice); i++ {
					target_shard := op.ShardSlice[i]
					for k, v := range op.DBSlice[i] {
						kv.db[target_shard][k] = v
						raft.Debug(raft.DSKV, "G%d SK%d applier get shard %d Key %s and Value %s from G%d config %d", kv.gid, kv.me, target_shard, k, v, possible_gid, possible_config_num)
					}
				}
				kv.putLatestRequestResult(op.ResultMap)
				kv.setShardState(op.ShardSlice, MINE)
				raft.Debug(raft.DSKV, "G%d SK%d applier set shard %v to MINE with config %d and install request result len %d", kv.gid, kv.me, op.ShardSlice, possible_config_num, len(op.ResultMap))
			case TRANSTART:
				kv.possibleWait(possible_gid, possible_config_num)
				kv.setShardState(op.ShardSlice, TRANSFER)
				raft.Debug(raft.DSKV, "G%d SK%d applier set shard %v to TRANSFER with config %d", kv.gid, kv.me, op.ShardSlice, possible_config_num)
			case TRANOK:
				kv.possibleWait(possible_gid, possible_config_num)
				kv.setShardState(op.ShardSlice, OTHERS)
				raft.Debug(raft.DSKV, "G%d SK%d applier set shard %v to OTHERS with config %d", kv.gid, kv.me, op.ShardSlice, possible_config_num)
			default:
				panic("OP Type Error in ShardKV")
			}

			if !skip {
				res_index := kv.nextResultIndex[op.ClerkId]
				kv.nextResultIndex[op.ClerkId] = (res_index + 1) % kv.cacheRequestNum
				kv.result[op.ClerkId][res_index] = curr_res
				raft.Debug(raft.DSKV, "G%d SK%d put result of clerk id %d and request id %d at index %d", kv.gid, kv.me, op.ClerkId, op.RequestId, res_index)
				if op.Type == GET {
					// invalid previous request result, save space
					prev_index := (res_index - 1 + kv.cacheRequestNum) % kv.cacheRequestNum
					kv.result[op.ClerkId][prev_index].Value = ""
					raft.Debug(raft.DSKV, "G%d SK%d invalid Get request result at index %d", kv.gid, kv.me, prev_index)
				}
			}
			// if put code below ahead of possible wait, there is risk
			// due to possible wait may block and release lock
			// at this time snapshot, cause wrong last apply index be snapshotted
			kv.lastApplyIndex = cmd_index
			// notify
			kv.condForApply.Broadcast()
			kv.mu.Unlock()
		} else if applymsg.SnapshotValid {
			// snapshot
			kv.mu.Lock()
			if applymsg.SnapshotIndex <= kv.lastApplyIndex {
				raft.Debug(raft.DSKV, "G%d SK%d receive old snapshot with index %d and term %d, current apply index %d", kv.gid, kv.me, applymsg.SnapshotIndex, applymsg.SnapshotTerm, kv.lastApplyIndex)
				panic("Snapshot Index Too Small")
			} else {
				rec_last_apply := kv.lastApplyIndex
				kv.rebuildStatus(applymsg.Snapshot) // will update kv.lastApplyIndex
				raft.Debug(raft.DSKV, "G%d SK%d receive usable snapshot with index %d and term %d, current apply index %d while previous apply index %d", kv.gid, kv.me, applymsg.SnapshotIndex, applymsg.SnapshotTerm, kv.lastApplyIndex, rec_last_apply)
			}
			// cond notify??
			kv.mu.Unlock()
		}

	}
}

// 1. check pending count when no longer is leader/term change
// 2. ensure previous logs are committed when be leader
// 3. make sure agreement of shard and transfer shard actually happens
func (kv *ShardKV) pendingNotifierAndCommitEnsurer() {
	term := 0
	last_op_term := 0

	for !kv.killed() {
		time.Sleep(time.Millisecond * 100)
		curr_term, leader := kv.rf.GetState()

		if leader && curr_term != last_op_term {
			raft.Debug(raft.DSKV, "G%d SK%d detect I'm leader and last issue NO-OP at term %d, current term %d, make sure to commit all previous logs", kv.gid, kv.me, last_op_term, curr_term)
			op := Op{
				Type:      NOOP,
				ClerkId:   -1,
				RequestId: -1,
			}
			last_op_term = curr_term
			kv.rf.Start(op)
		}

		kv.mu.Lock()
		if leader && kv.waiting && curr_term != kv.lastShardOpTerm {
			raft.Debug(raft.DSKV, "G%d SK%d detect I'm leader and waiting, last check at term %d, current term %d, make sure agreement to progress", kv.gid, kv.me, kv.lastShardOpTerm, curr_term)
			kv.condForApply.Broadcast()
		}
		kv.mu.Unlock()

		if leader && curr_term == term {
			continue
		}

		kv.mu.Lock()
		if kv.pendingRequestCount > 0 {
			raft.Debug(raft.DSKV, "G%d SK%d has pending request count %d, leader %t, current term %d, previous check term %d, notify it to reply", kv.gid, kv.me, kv.pendingRequestCount, leader, curr_term, term)
			term = curr_term
			kv.condForApply.Broadcast()
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) doSnapshot() {
	kv.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastApplyIndex)
	e.Encode(kv.db)
	e.Encode(kv.result)
	e.Encode(kv.nextResultIndex)
	e.Encode(kv.shardStatus)
	e.Encode(kv.currentConfig)
	data := w.Bytes()

	kv.rf.Snapshot(kv.lastApplyIndex, data)
	raft.Debug(raft.DSKV, "G%d SK%d finish snapshot with total size %d and last snapshot index %d", kv.gid, kv.me, len(data), kv.lastApplyIndex)
	kv.mu.Unlock()
}

// check raft state size, if too large do snapshot
func (kv *ShardKV) snapShotter() {
	for !kv.killed() {
		time.Sleep(time.Millisecond * 100)
		state_size := kv.persister.RaftStateSize()
		if state_size >= kv.maxraftstate {
			raft.Debug(raft.DSKV, "G%d SK%d detect raft state size too large %d, threshold %d", kv.gid, kv.me, state_size, kv.maxraftstate)
			kv.doSnapshot()
		}
	}
}

func (kv *ShardKV) rebuildStatus(data []byte) {
	if data == nil || len(data) < 1 {
		raft.Debug(raft.DSKV, "G%d SK%d want to rebuild status but nothing to do", kv.gid, kv.me)
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastApply int
	var database [shardctrler.NShards]map[string]string
	var nextIndex map[int]int
	var res map[int][]Result
	var state [shardctrler.NShards]ShardState
	var config shardctrler.Config

	if d.Decode(&lastApply) != nil || d.Decode(&database) != nil || d.Decode(&res) != nil ||
		d.Decode(&nextIndex) != nil || d.Decode(&state) != nil || d.Decode(&config) != nil {
		panic("KV decode error")
	}
	kv.lastApplyIndex = lastApply
	kv.db = database
	kv.result = res
	kv.nextResultIndex = nextIndex
	kv.currentConfig = config
	kv.shardStatus = state

	raft.Debug(raft.DSKV, "G%d SK%d rebuild status with config num %d, shard state %v, last apply index %d, database len %d, result len %d, next index len %d", kv.gid, kv.me, kv.currentConfig.Num, kv.shardStatus, lastApply, len(kv.db), len(kv.result), len(kv.nextResultIndex))
}

func (kv *ShardKV) getLatestRequestResult(trans_slice []int) map[int]Result {
	trans_res := make(map[int]Result)

	kv.mu.Lock()
	for clerk, result_slice := range kv.result {
		if clerkId2Gid(clerk) >= 0 || clerk == -1 {
			continue
		}
		latest_index := (kv.nextResultIndex[clerk] - 1 + kv.cacheRequestNum) % kv.cacheRequestNum
		latest_shard := result_slice[latest_index].Shard
		raft.Debug(raft.DSKV, "G%d SK%d find latest result of clerk id %d is request id %d and shard %d, at index %d", kv.gid, kv.me, clerk, result_slice[latest_index].RequestId, latest_shard, latest_index)
		if result_slice[latest_index].RequestId == 0 {
			continue
		}
		for _, shard := range trans_slice {
			if shard == latest_shard {
				trans_res[clerk] = result_slice[latest_index]
				raft.Debug(raft.DSKV, "G%d SK%d transfer shard %d with clerk id %d, request id %d", kv.gid, kv.me, latest_shard, clerk, trans_res[clerk].RequestId)
				break
			}
		}
	}
	raft.Debug(raft.DSKV, "G%d SK%d transfer shard with total clerk request len %d", kv.gid, kv.me, len(trans_res))
	kv.mu.Unlock()
	return trans_res
}

func (kv *ShardKV) transferShard(to_be_transfer []int, next_config *shardctrler.Config) {
	// unlock!
	kv.mu.Unlock()
	leader := true

	// transfer to group with all shard in once
	gid2ShardSlice := make(map[int][]int)
	for _, shard_idx := range to_be_transfer {
		gid := next_config.Shards[shard_idx]
		gid2ShardSlice[gid] = append(gid2ShardSlice[gid], shard_idx)
	}
	for gid, trans_slice := range gid2ShardSlice {
		db_slice := make([]map[string]string, len(trans_slice))
		for idx, shard_idx := range trans_slice {
			db_slice[idx] = make(map[string]string)
			for k, v := range kv.db[shard_idx] {
				db_slice[idx][k] = v
			}
		}
		res_map := kv.getLatestRequestResult(trans_slice)

		servers := next_config.Groups[gid]
		leader_offset := kv.getGidLeaderId(gid)
		if leader_offset == -1 {
			// don't know leader, try all servers in index order
			leader_offset = 0
			raft.Debug(raft.DSKV, "G%d SK%d don't know group %d leader id", kv.gid, kv.me, gid)
		} else {
			raft.Debug(raft.DSKV, "G%d SK%d know group %d leader id %d", kv.gid, kv.me, gid, leader_offset)
		}

		for !kv.killed() {
			exit := false
			for si := 0; si < len(servers); si++ {
				real_si := (si + leader_offset) % len(servers)
				srv := kv.make_end(servers[real_si])

				args := TransferArgs{
					ShardSlice: trans_slice,
					ShardDB:    db_slice,
					ResultMap:  res_map,
					ClerkId:    gid2ClerkId(kv.gid),
					RequestId:  next_config.Num,
				}

				if _, leader = kv.rf.GetState(); !leader {
					exit = true
					raft.Debug(raft.DSKV, "G%d SK%d find I'm not leader when actually transfer shard, stop transfer", kv.gid, kv.me)
					break
				}

				var reply TransferReply
				raft.Debug(raft.DSKV, "G%d SK%d ready to send Transfer RPC with config num %d to G%d with shard %v", kv.gid, kv.me, next_config.Num, gid, trans_slice)
				ok := srv.Call("ShardKV.Transfer", &args, &reply)
				if !ok {
					kv.leaderId[gid] = -1
					raft.Debug(raft.DSKV, "G%d SK%d send Transfer RPC with config num %d timeout", kv.gid, kv.me, next_config.Num)
					continue
				}

				if reply.Err == ErrWrongGroup {
					raft.Debug(raft.DSKV, "G%d SK%d send Transfer RPC with config num %d to wrong group %d", kv.gid, kv.me, next_config.Num, gid)
					panic("Transfer Wrong Group")
				} else if reply.Err == ErrWrongLeader {
					raft.Debug(raft.DSKV, "G%d SK%d send Transfer RPC with config num %d to corret group %d but wrong leader %d", kv.gid, kv.me, next_config.Num, gid, real_si)
					kv.leaderId[gid] = -1
					time.Sleep(100 * time.Millisecond)
				} else {
					raft.Debug(raft.DSKV, "G%d SK%d send Transfer RPC with config num %d to G%d with shard %v finish OK", kv.gid, kv.me, next_config.Num, gid, trans_slice)
					kv.leaderId[gid] = real_si
					exit = true
					break
				}
			}
			if exit {
				break
			}
		}
		if !leader {
			break
		}
	}

	kv.mu.Lock() // avoid wake up lose
	if !leader {
		raft.Debug(raft.DSKV, "G%d SK%d stop actually transfer shard due to not leader", kv.gid, kv.me)
		return
	}
	op := Op{
		Type:       TRANOK,
		ShardSlice: to_be_transfer,
		ClerkId:    gid2ClerkId(kv.gid),
		RequestId:  next_config.Num*2 + 1,
	}
	cmd_index, _, isleader := kv.rf.Start(op)
	if isleader {
		raft.Debug(raft.DSKV, "G%d SK%d may put transfer finish OK at %d with config num %d", kv.gid, kv.me, cmd_index, next_config.Num)
	}
}

// check whether all wait shard has been transferred to me
// call this when holding lock
func (kv *ShardKV) allWaitOK(wait_slice []int) bool {
	for _, shard_idx := range wait_slice {
		if kv.shardStatus[shard_idx] != MINE {
			return false
		}
	}
	return true
}

// check whether all to be transferred shard has no pending clerk request
// call this when holding lock
// func (kv *ShardKV) allPendingFinish(transfer_slice []int) bool {
// 	for _, shard_idx := range transfer_slice {
// 		if kv.pendingRequestOfShard[shard_idx] > 0 {
// 			return false
// 		}
// 	}
// 	return true
// }

// delete all transferred key-value pair
// call this when holding lock
func (kv *ShardKV) deleteTransferShards(transfer_slice []int) {
	for _, shard_idx := range transfer_slice {
		kv.db[shard_idx] = make(map[string]string)
	}
}

// catch up to config
func (kv *ShardKV) configCatchUper(next_config *shardctrler.Config) {

	var to_be_transfer []int
	var to_be_wait []int
	for i := 0; i < shardctrler.NShards; i++ {
		prev_mine := kv.currentConfig.Shards[i] == kv.gid
		curr_mine := next_config.Shards[i] == kv.gid
		if prev_mine && !curr_mine {
			// kv.shardStatus[i] = TRANSFER
			to_be_transfer = append(to_be_transfer, i)
		} else if !prev_mine && curr_mine {
			if kv.currentConfig.Shards[i] == 0 {
				// previous invalid, directly become mine
				kv.shardStatus[i] = MINE
			} else {
				// kv.shardStatus[i] = WAIT
				to_be_wait = append(to_be_wait, i)
			}
		}
	}

	// firstly ensure transfer
	if len(to_be_transfer) > 0 {
		// wait for raft log commit
		raft.Debug(raft.DSKV, "G%d SK%d wait for agreement on transfer shard with config num %d", kv.gid, kv.me, next_config.Num)
		op := Op{
			Type:       TRANSTART,
			ShardSlice: to_be_transfer,
			ClerkId:    gid2ClerkId(kv.gid),
			RequestId:  next_config.Num * 2,
		}

		res_index := kv.getRequestResult(gid2ClerkId(kv.gid), next_config.Num*2)
		kv.waiting = true
		kv.lastShardOpTerm = 0
		for res_index == -1 {
			// avoid duplicate issue op
			if term, leader := kv.rf.GetState(); leader && term != kv.lastShardOpTerm && kv.lastApplyIndex == kv.rf.GetLastLogIndex() {
				cmd_index, _, _ := kv.rf.Start(op)
				kv.lastShardOpTerm = term
				raft.Debug(raft.DSKV, "G%d SK%d may put transfer shard command at %d with config num %d", kv.gid, kv.me, cmd_index, next_config.Num)
			}
			kv.condForApply.Wait()
			if kv.currentConfig.Num >= next_config.Num {
				raft.Debug(raft.DSKV, "G%d SK%d find catch uper fall behind in transfer shard, target config num %d while current config num %d", kv.gid, kv.me, next_config.Num, kv.currentConfig.Num)
				kv.waiting = false
				return
			}
			res_index = kv.getRequestResult(gid2ClerkId(kv.gid), next_config.Num*2)
		}
		kv.waiting = false
		raft.Debug(raft.DSKV, "G%d SK%d know agreement on transfer shard with config num %d has reached", kv.gid, kv.me, next_config.Num)

		// wait for all requests relevant to transferred shards have been finished
		// raft.Debug(raft.DSKV, "G%d SK%d wait for pending requests of transferred shards with config num %d to finish", kv.gid, kv.me, next_config.Num)
		// for !kv.allPendingFinish(to_be_transfer) {
		// 	kv.condForPending.Wait()
		// }
		// raft.Debug(raft.DSKV, "G%d SK%d know all pending requests of transferred shards with config num %d have been finished", kv.gid, kv.me, next_config.Num)

		raft.Debug(raft.DSKV, "G%d SK%d wait for agreement on transfer shard finish OK with config num %d", kv.gid, kv.me, next_config.Num)
		res_index = kv.getRequestResult(gid2ClerkId(kv.gid), next_config.Num*2+1)
		kv.waiting = true
		kv.lastShardOpTerm = 0
		for res_index == -1 {
			// avoid duplicate issue op
			if term, leader := kv.rf.GetState(); leader && term != kv.lastShardOpTerm && kv.lastApplyIndex == kv.rf.GetLastLogIndex() {
				kv.lastShardOpTerm = term
				raft.Debug(raft.DSKV, "G%d SK%d may start to actually transfer shard with config num %d", kv.gid, kv.me, next_config.Num)
				kv.transferShard(to_be_transfer, next_config)
			}
			if kv.currentConfig.Num >= next_config.Num {
				raft.Debug(raft.DSKV, "G%d SK%d find catch uper fall behind in actually transfer shard before cond.wait, target config num %d while current config num %d", kv.gid, kv.me, next_config.Num, kv.currentConfig.Num)
				kv.waiting = false
				return
			}
			// important!
			if kv.getRequestResult(gid2ClerkId(kv.gid), next_config.Num*2+1) != -1 {
				break
			}
			kv.condForApply.Wait()
			if kv.currentConfig.Num >= next_config.Num {
				raft.Debug(raft.DSKV, "G%d SK%d find catch uper fall behind in actually transfer shard after cond.wait, target config num %d while current config num %d", kv.gid, kv.me, next_config.Num, kv.currentConfig.Num)
				kv.waiting = false
				return
			}
			res_index = kv.getRequestResult(gid2ClerkId(kv.gid), next_config.Num*2+1)
		}
		kv.waiting = false
		raft.Debug(raft.DSKV, "G%d SK%d know agreement on transfer shard finish OK with config num %d has reached", kv.gid, kv.me, next_config.Num)

		if kv.currentConfig.Num >= next_config.Num {
			panic("Current Config Ahead before Delete Shard")
		}
		// delete shards
		kv.deleteTransferShards(to_be_transfer)
	} else {
		raft.Debug(raft.DSKV, "G%d SK%d know nothing to transfer when catching up to config num %d", kv.gid, kv.me, next_config.Num)
	}

	// secondly check wait
	if len(to_be_wait) > 0 {
		// ?? should use pending??
		raft.Debug(raft.DSKV, "G%d SK%d wait for agreement on wait shard with config num %d", kv.gid, kv.me, next_config.Num)
		for !kv.allWaitOK(to_be_wait) {
			kv.condForApply.Wait()
			if kv.currentConfig.Num >= next_config.Num {
				raft.Debug(raft.DSKV, "G%d SK%d find catch uper fall behind in wait shard, target config num %d while current config num %d", kv.gid, kv.me, next_config.Num, kv.currentConfig.Num)
				return
			}
		}
		raft.Debug(raft.DSKV, "G%d SK%d know agreement on wait shard finish OK with config num %d and wait shard %v has reached", kv.gid, kv.me, next_config.Num, to_be_wait)
	} else {
		raft.Debug(raft.DSKV, "G%d SK%d know nothing to wait when catching up to config num %d", kv.gid, kv.me, next_config.Num)
	}

	if kv.currentConfig.Num >= next_config.Num {
		panic("Current Config Ahead")
	}

	kv.currentConfig = *next_config
	raft.Debug(raft.DSKV, "G%d SK%d catch up to config num %d, current shard status %v", kv.gid, kv.me, next_config.Num, kv.shardStatus)
	kv.condForConfig.Broadcast()
}

// fetch latest config from shard controller
func (kv *ShardKV) configPuller() {
	for !kv.killed() {
		kv.mu.Lock()
		next_expected_config_num := kv.currentConfig.Num + 1
		kv.mu.Unlock()

		latest_config := kv.clerkOfContrller.Query(next_expected_config_num)

		kv.mu.Lock()
		if latest_config.Num == kv.currentConfig.Num+1 {
			raft.Debug(raft.DSKV, "G%d SK%d pull new config num %d from shard controller, prepare to catch up, shard slice %v", kv.gid, kv.me, next_expected_config_num, latest_config.Shards)
			// blocked until catch up to latest config
			kv.configCatchUper(&latest_config)
		} else {
			raft.Debug(raft.DSKV, "G%d SK%d pull old config num %d from shard controller", kv.gid, kv.me, latest_config.Num)
			time.Sleep(time.Millisecond * 100)
		}
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.clerkOfContrller = shardctrler.MakeClerk(ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.currentConfig = shardctrler.Config{}
	kv.currentConfig.Groups = map[int][]string{}
	kv.leaderId = make(map[int]int)
	kv.lastShardOpTerm = 0

	kv.lastApplyIndex = 0
	kv.persister = persister
	kv.pendingRequestCount = 0
	kv.nextResultIndex = make(map[int]int)
	kv.cacheRequestNum = 2
	kv.result = make(map[int][]Result)
	kv.condForApply = sync.NewCond(&kv.mu)
	kv.condForConfig = sync.NewCond(&kv.mu)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardStatus[i] = OTHERS
		kv.db[i] = make(map[string]string)
	}

	kv.rebuildStatus(kv.persister.ReadSnapshot()) // include db, config, shard status and so on

	go kv.applier()
	go kv.pendingNotifierAndCommitEnsurer()
	go kv.configPuller()

	if kv.maxraftstate != -1 {
		go kv.snapShotter()
	}

	return kv
}
