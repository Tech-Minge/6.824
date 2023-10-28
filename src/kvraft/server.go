package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type OpType int

const GET OpType = 0
const PUT OpType = 1
const APPEND OpType = 2

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      OpType
	Key       string
	Value     string
	ClerkId   int
	RequestId int
}

type status int

const SUCCESS status = 0
const FAIL status = 1

type Result struct {
	RequestId int
	Success   status
	Value     string // for Get only
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	lastApplyIndex      int
	persister           *raft.Persister
	pendingRequestCount int
	nextResultIndex     map[int]int // next index to store result
	cacheRequestNum     int
	db                  map[string]string
	result              map[int][]Result // clerk id -> result (circle buffer)
	condForApply        *sync.Cond
	// condForAheadRequest *sync.Cond
}

// check Op duplicate
func (kv *KVServer) getRequestResult(clerk_id int, request_id int) int {
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var leader bool
	var init_term int
	var cmd_index int
	if _, leader = kv.rf.GetState(); !leader {
		reply.Err = ErrWrongLeader
		raft.Debug(raft.DKV, "K%d receive Get RPC with Key %s, but not leader(early check)", kv.me, args.Key)
		return
	}

	kv.mu.Lock()
	kv.pendingRequestCount++
	res_index := kv.getRequestResult(args.ClerkId, args.RequestId)
	if res_index == -1 {
		op := Op{GET, args.Key, "", args.ClerkId, args.RequestId}
		cmd_index, init_term, leader = kv.rf.Start(op)

		// check leader
		if !leader {
			reply.Err = ErrWrongLeader
			raft.Debug(raft.DKV, "K%d receive Get RPC with Key %s, but not leader", kv.me, args.Key)
			kv.pendingRequestCount--
			kv.mu.Unlock()
			return
		}
		raft.Debug(raft.DKV, "K%d know raft maybe place Get RPC at index %d with Key %s", kv.me, cmd_index, args.Key)

		for res_index == -1 {
			kv.condForApply.Wait()
			// maybe check term is enough, no need to check is leader or not
			if curr_term, curr_leader := kv.rf.GetState(); !curr_leader || curr_term != init_term {
				reply.Err = ErrWrongLeader
				raft.Debug(raft.DKV, "K%d wait Get RPC to finish with C%d request id %d, Key %s, but leader %t, current term %d, initial term %d", kv.me, args.ClerkId, args.RequestId, args.Key, curr_leader, curr_term, init_term)
				kv.pendingRequestCount--
				kv.mu.Unlock()
				return
			}
			res_index = kv.getRequestResult(args.ClerkId, args.RequestId)
		}
	} else {
		raft.Debug(raft.DKV, "K%d know Get RPC with C%d request id %d duplicate(early check)", kv.me, args.ClerkId, args.RequestId)
	}
	res := kv.result[args.ClerkId][res_index]
	kv.pendingRequestCount--
	kv.mu.Unlock()

	reply.Value = res.Value
	if res.Success == SUCCESS {
		raft.Debug(raft.DKV, "K%d know Get RPC finish with C%d request id %d, Key %s in success", kv.me, args.ClerkId, args.RequestId, args.Key)
		reply.Err = OK
	} else {
		raft.Debug(raft.DKV, "K%d know Get RPC finish with C%d request id %d, Key %s but no such Key", kv.me, args.ClerkId, args.RequestId, args.Key)
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var leader bool
	var init_term int
	var cmd_index int
	if _, leader = kv.rf.GetState(); !leader {
		reply.Err = ErrWrongLeader
		raft.Debug(raft.DKV, "K%d receive PutAppend RPC with Key %s, but not leader(early check)", kv.me, args.Key)
		return
	}

	kv.mu.Lock()
	kv.pendingRequestCount++
	res_index := kv.getRequestResult(args.ClerkId, args.RequestId)
	if res_index == -1 {
		var op Op
		if args.Op == "Put" {
			op = Op{PUT, args.Key, args.Value, args.ClerkId, args.RequestId}
		} else {
			op = Op{APPEND, args.Key, args.Value, args.ClerkId, args.RequestId}
		}
		cmd_index, init_term, leader = kv.rf.Start(op)

		// check leader
		if !leader {
			reply.Err = ErrWrongLeader
			raft.Debug(raft.DKV, "K%d receive PutAppend RPC with Key %s, but not leader", kv.me, args.Key)
			kv.pendingRequestCount--
			kv.mu.Unlock()
			return
		}
		raft.Debug(raft.DKV, "K%d know raft maybe place PutAppend RPC at index %d with Key %s", kv.me, cmd_index, args.Key)

		for res_index == -1 {
			kv.condForApply.Wait()
			if curr_term, curr_leader := kv.rf.GetState(); !curr_leader || curr_term != init_term {
				reply.Err = ErrWrongLeader
				raft.Debug(raft.DKV, "K%d wait PutAppend RPC to finish with C%d request id %d, Key %s, but leader %t, current term %d, initial term %d", kv.me, args.ClerkId, args.RequestId, args.Key, curr_leader, curr_term, init_term)
				kv.pendingRequestCount--
				kv.mu.Unlock()
				return
			}
			res_index = kv.getRequestResult(args.ClerkId, args.RequestId)
		}
	} else {
		raft.Debug(raft.DKV, "K%d know PutAppend RPC with C%d request id %d duplicate(early check)", kv.me, args.ClerkId, args.RequestId)
	}
	res := kv.result[args.ClerkId][res_index]
	kv.pendingRequestCount--
	kv.mu.Unlock()

	raft.Debug(raft.DKV, "K%d know PutAppend RPC finish with C%d request id %d, Key %s", kv.me, args.ClerkId, args.RequestId, args.Key)
	if res.Success != SUCCESS {
		panic("KVserver Put Append Fail")
	}
	reply.Err = OK
}

func (kv *KVServer) applyCommittedLog() {
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
				panic("KV cmd index too small")
			}
			kv.lastApplyIndex = cmd_index
			may_duplicate_index := kv.getRequestResult(op.ClerkId, op.RequestId)
			if may_duplicate_index != -1 {
				raft.Debug(raft.DKV, "K%d find Duplicate RPC with C%d and request id %d, command index %d, duplicate index %d", kv.me, op.ClerkId, op.RequestId, cmd_index, may_duplicate_index)
				kv.mu.Unlock()
				continue
			}

			v, status := kv.db[op.Key]
			raft.Debug(raft.DKV, "K%d apply committed command index %d with C%d request id %d", kv.me, cmd_index, op.ClerkId, op.RequestId)
			res_index := kv.nextResultIndex[op.ClerkId]
			kv.nextResultIndex[op.ClerkId] = (kv.nextResultIndex[op.ClerkId] + 1) % kv.cacheRequestNum
			if op.Type == GET {
				if !status {
					kv.result[op.ClerkId][res_index] = Result{op.RequestId, FAIL, ""}
				} else {
					kv.result[op.ClerkId][res_index] = Result{op.RequestId, SUCCESS, v}
				}
			} else if op.Type == APPEND {
				kv.db[op.Key] = v + op.Value
				kv.result[op.ClerkId][res_index] = Result{op.RequestId, SUCCESS, ""}
			} else {
				kv.db[op.Key] = op.Value
				kv.result[op.ClerkId][res_index] = Result{op.RequestId, SUCCESS, ""}
			}
			// notify
			kv.condForApply.Broadcast()
			kv.mu.Unlock()
		} else if applymsg.SnapshotValid {
			// snapshot
			kv.mu.Lock()
			if applymsg.SnapshotIndex <= kv.lastApplyIndex {
				raft.Debug(raft.DKV, "K%d receive old snapshot with index %d and term %d, current apply index %d", kv.me, applymsg.SnapshotIndex, applymsg.SnapshotTerm, kv.lastApplyIndex)
				panic("KV snapshot index too small")
			} else {
				rec_last_apply := kv.lastApplyIndex
				kv.rebuildStatus(applymsg.Snapshot) // will update kv.lastApplyIndex
				raft.Debug(raft.DKV, "K%d receive usable snapshot with index %d and term %d, current apply index %d while previous apply index %d", kv.me, applymsg.SnapshotIndex, applymsg.SnapshotTerm, kv.lastApplyIndex, rec_last_apply)
			}
			kv.mu.Unlock()
		}

	}
}

// check pending count when no longer is leader/term change
func (kv *KVServer) notifier() {
	term := 0
	for !kv.killed() {
		time.Sleep(time.Millisecond * 100)
		curr_term, leader := kv.rf.GetState()
		if leader && curr_term == term {
			continue
		}

		kv.mu.Lock()
		if kv.pendingRequestCount > 0 {
			raft.Debug(raft.DKV, "K%d has pending request count %d, leader %t, current term %d, previous check term %d, notify it to reply", kv.me, kv.pendingRequestCount, leader, curr_term, term)
			term = curr_term
			kv.condForApply.Broadcast()
		}
		kv.mu.Unlock()
	}
}

// call it when holding lock
func (kv *KVServer) doSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastApplyIndex)
	e.Encode(kv.db)
	e.Encode(kv.result)
	e.Encode(kv.nextResultIndex)
	data := w.Bytes()

	kv.rf.Snapshot(kv.lastApplyIndex, data)
	raft.Debug(raft.DKV, "K%d finish snapshot with total size %d and last snapshot index %d", kv.me, len(data), kv.lastApplyIndex)
}

// check raft state size, if too large do snapshot
func (kv *KVServer) snapshotter() {
	for !kv.killed() {
		time.Sleep(time.Millisecond * 100)
		state_size := kv.persister.RaftStateSize()
		if state_size >= kv.maxraftstate {
			raft.Debug(raft.DKV, "K%d detect raft state size too large %d, threshold %d", kv.me, state_size, kv.maxraftstate)
			kv.mu.Lock()
			kv.doSnapshot()
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) rebuildStatus(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastApply int
	var database map[string]string
	var nextIndex map[int]int
	var res map[int][]Result

	if d.Decode(&lastApply) != nil || d.Decode(&database) != nil || d.Decode(&res) != nil || d.Decode(&nextIndex) != nil {
		panic("KV decode error")
	}
	kv.lastApplyIndex = lastApply
	kv.db = database
	kv.result = res
	kv.nextResultIndex = nextIndex
	raft.Debug(raft.DKV, "K%d rebuild status with last apply index %d, database len %d, result len %d, next index len %d", kv.me, lastApply, len(kv.db), len(kv.result), len(kv.nextResultIndex))
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.lastApplyIndex = 0
	kv.persister = persister
	kv.pendingRequestCount = 0
	kv.nextResultIndex = make(map[int]int)
	kv.cacheRequestNum = 3
	kv.db = make(map[string]string)
	kv.result = make(map[int][]Result)
	kv.condForApply = sync.NewCond(&kv.mu)
	// kv.condForAheadRequest = sync.NewCond(&kv.mu)

	kv.rebuildStatus(kv.persister.ReadSnapshot())

	go kv.applyCommittedLog()
	go kv.notifier()
	if kv.maxraftstate != -1 {
		go kv.snapshotter()
	}

	return kv
}
