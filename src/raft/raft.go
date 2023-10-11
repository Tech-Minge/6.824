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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

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

type LogEntry struct {
	Term    int
	Command interface{}
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
	currentTerm       int
	votedFor          int
	commitIndex       int
	lastSnapShotIndex int
	lastSnapShotTerm  int
	installInterval   int64

	isLeader            bool
	receiveHeartBeat    bool
	heartbeatTimeout    int // millisecond
	electionTimeoutLow  int // millisecond
	electionTimeoutHigh int // millisecond
	batchNum            int
	batchThreshold      int
	maxSendLogLen       int

	installTime      []time.Time // reduce some Install RPC call
	log              []LogEntry
	nextIndex        []int
	condForVote      *sync.Cond
	condForCommitLog *sync.Cond
	applyCh          chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// all function call to persist() is holding lock
	// no need to acquire lock here
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.lastSnapShotIndex)
	e.Encode(rf.lastSnapShotTerm)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// perisit both raft state && snapshot
// also include lastSnapshotIndex && lastSnapshotTerm
// call it when holding lock
func (rf *Raft) persist2(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.lastSnapShotIndex)
	e.Encode(rf.lastSnapShotTerm)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var last_snapshot_index int
	var last_snapshot_term int
	var term int
	var vote int
	var log []LogEntry
	if d.Decode(&last_snapshot_index) != nil || d.Decode(&last_snapshot_term) != nil || d.Decode(&term) != nil || d.Decode(&vote) != nil || d.Decode(&log) != nil {
		panic("Decode error")
	} else {
		rf.lastSnapShotIndex = last_snapshot_index
		rf.lastSnapShotTerm = last_snapshot_term
		rf.currentTerm = term
		rf.votedFor = vote
		rf.log = log
		Debug(dInfo, "S%d read persistent data, snapshot index %d with snapshot term %d, current term %d vote for %d, relative log len %d with last term %d", rf.me, last_snapshot_index, last_snapshot_term, term, vote, len(log), rf.termByRelativeIndex(len(log)-1))
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

// helper function to get the index in rf.log when given actual index
// call it when holding lock
// may return negative, make sure can handle it
func (rf *Raft) relativeIndex(actual_index int) int {
	return actual_index - rf.lastSnapShotIndex - 1
}

// helper function to get the absolute index when given relative index
// call it when holding lock
func (rf *Raft) absoluteIndex(rlt_index int) int {
	return rlt_index + rf.lastSnapShotIndex + 1
}

// helper function to get the term when given relative index
// call it when holding lock
// panic when rlt_index < -1
func (rf *Raft) termByRelativeIndex(rlt_index int) int {
	if rlt_index < -1 {
		Debug(dError, "S%d call termByRelativeIndex by passing relaive index %d", rf.me, rlt_index)
		panic("Parameter error in termByRelativeIndex")
	} else if rlt_index == -1 {
		return rf.lastSnapShotTerm
	} else {
		return rf.log[rlt_index].Term
	}
}

// check interval when call install rpc
// help reduce install rpc
// call it when holding lock
func (rf *Raft) checkInstallInterval(i int) bool {
	cur := time.Now()
	if cur.Sub(rf.installTime[i]).Milliseconds() <= rf.installInterval {
		return false
	}
	rf.installTime[i] = cur
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
	if rf.lastSnapShotIndex >= index {
		Debug(dInfo, "S%d receive outdated Snapshot Request from server with index %d, current snapshot index %d", rf.me, index, rf.lastSnapShotIndex)
		return
	}
	rlt_index := rf.relativeIndex(index)
	new_last_snapshot_term := rf.termByRelativeIndex(rlt_index)
	Debug(dInfo, "S%d receive Snapshot Request from server, current snapshot index %d and term %d, while prev snapshot index %d and term %d", rf.me, index, new_last_snapshot_term, rf.lastSnapShotIndex, rf.lastSnapShotTerm)
	// truncate log
	rf.log = rf.log[rlt_index+1:]
	Debug(dInfo, "S%d after snapshot, current relative log len(not include snapshot len) %d", rf.me, len(rf.log))

	rf.lastSnapShotIndex = index
	rf.lastSnapShotTerm = new_last_snapshot_term
	// save snapshot
	rf.persist2(snapshot)

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
	LastLogterm  int
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

// args of AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Logs         []LogEntry
}

// reply of AppendEntries RPC
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// args of InstallSnapshot RPC
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// reply of InstallSnapshot RPC
type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		Debug(dInfo, "S%d do not vote for S%d due to its older term of %d", rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = false
		return
	} else if rf.currentTerm == args.Term && rf.votedFor != -1 {
		Debug(dInfo, "S%d do not vote for S%d due to already vote for S%d", rf.me, args.CandidateId, rf.votedFor)
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		Debug(dInfo, "S%d update current term from %d to %d by Vote RPC", rf.me, rf.currentTerm, args.Term)
		reply.Term = args.Term
		rf.currentTerm = args.Term // update to new term
		rf.votedFor = -1
		// persist
		rf.persist()
		if rf.isLeader {
			rf.isLeader = false // important
			Debug(dLeader, "S%d from leader turn to follower by Vote RPC", rf.me)
		}
	}
	rlt_last_index := len(rf.log) - 1
	abs_last_index := rf.absoluteIndex(rlt_last_index)
	lastTerm := rf.termByRelativeIndex(rlt_last_index)
	if lastTerm > args.LastLogterm || (lastTerm == args.LastLogterm && abs_last_index > args.LastLogIndex) {
		reply.VoteGranted = false
		Debug(dInfo, "S%d reject vote for S%d at term %d due to its old log", rf.me, args.CandidateId, rf.currentTerm)
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.receiveHeartBeat = true // set true, detail in paper figure 2
		Debug(dInfo, "S%d vote for S%d at term %d, heart beat at time %s", rf.me, args.CandidateId, rf.currentTerm, time.Now())
	}

}

// handler of AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	if rf.currentTerm > args.Term {
		Debug(dInfo, "S%d reject Append RPC from S%d due to its older term of %d", rf.me, args.LeaderId, args.Term)
		reply.Success = false
		return
	}
	// only leader call AppendEntries RPC
	if args.Term > rf.currentTerm {
		Debug(dInfo, "S%d update current term from %d to %d by Append RPC", rf.me, rf.currentTerm, args.Term)
		reply.Term = args.Term
		rf.currentTerm = args.Term // update term
		rf.votedFor = -1           // needed??
		// persist
		rf.persist()
		if rf.isLeader {
			rf.isLeader = false
			Debug(dLeader, "S%d from leader turn to follower by Append RPC", rf.me)
		}
	}
	if rf.isLeader {
		// at same term, two leaders
		Debug(dError, "S%d and S%d are leaders at same term %d", rf.me, args.LeaderId, rf.currentTerm)
		panic("Two leaders at same term")
	}
	Debug(dInfo, "S%d receive Append RPC normally from S%d, heart beat at time %s", rf.me, args.LeaderId, time.Now())
	rf.receiveHeartBeat = true
	// rf.votedFor = -1 // no need to reset, will cause multi vote for others
	reply.Success = true

	rlt_log_len := len(rf.log)
	abs_log_len := rf.absoluteIndex(rlt_log_len)
	rlt_args_prev_log_index := rf.relativeIndex(args.PrevLogIndex) // relative, maybe < -1??
	if rlt_args_prev_log_index == -1 && rf.lastSnapShotTerm != args.PrevLogTerm {
		Debug(dError, "S%d know leader wants to check my snapshot index %d with term %d, but leader has term %d", rf.me, rf.lastSnapShotIndex, rf.lastSnapShotTerm, args.PrevLogTerm)
		panic("Snapshot inconsistency")
	}

	if args.PrevLogIndex >= abs_log_len {
		Debug(dInfo, "S%d reject Append RPC due to leader previous log index too long, my log len %d, previous check at %d", rf.me, abs_log_len, args.PrevLogIndex)
		reply.Success = false
		reply.ConflictIndex = abs_log_len
	} else if rlt_args_prev_log_index >= 0 && rf.termByRelativeIndex(rlt_args_prev_log_index) != args.PrevLogTerm { // already test rlt_args_prev_log_index == -1 above
		term_of_args_prev_log := rf.termByRelativeIndex(rlt_args_prev_log_index)
		Debug(dInfo, "S%d reject Append RPC due to inconsistent, my log at %d is term %d, but leader term %d", rf.me, args.PrevLogIndex, term_of_args_prev_log, args.PrevLogTerm)
		reply.Success = false
		reply.ConflictTerm = term_of_args_prev_log
		var conflict_index int
		for i := 0; i <= rlt_args_prev_log_index; i++ {
			if rf.log[i].Term == term_of_args_prev_log {
				conflict_index = rf.absoluteIndex(i)
				break
			}
		}
		reply.ConflictIndex = conflict_index
		// truncate log
		rf.log = rf.log[:rlt_args_prev_log_index]
		Debug(dInfo, "S%d delete all logs after(including) index %d", rf.me, args.PrevLogIndex)
		rf.persist()
	} else {
		Debug(dInfo, "S%d pass Append RPC consistency check with previous index %d and previous term %d, leader commit %d, args log len %d", rf.me, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Logs))
		// check conflict in log
		end_check := rf.absoluteIndex(len(rf.log))
		longer_than_leader := false
		if end_check >= args.PrevLogIndex+1+len(args.Logs) {
			longer_than_leader = true
			end_check = args.PrevLogIndex + 1 + len(args.Logs)
		}
		var i int
		start_check := args.PrevLogIndex + 1
		if start_check < rf.lastSnapShotIndex+1 {
			start_check = rf.lastSnapShotIndex + 1
			Debug(dInfo, "S%d know leader S%d wants to check previous index %d, but my snapshot index %d", rf.me, args.LeaderId, args.PrevLogIndex, rf.lastSnapShotIndex)
		}
		for i = start_check; i < end_check; i++ {
			my_term := rf.log[rf.relativeIndex(i)].Term
			leader_term := args.Logs[i-args.PrevLogIndex-1].Term
			if my_term == leader_term {
				continue
			} else {
				// find conflict
				Debug(dInfo, "S%d find inconsistency though passing previous check, at log index %d, my term %d leader term %d", rf.me, i, my_term, leader_term)
				rf.log = rf.log[:rf.relativeIndex(i)]
				rf.log = append(rf.log, args.Logs[i-args.PrevLogIndex-1:]...)
				Debug(dInfo, "S%d delete log after(including) index %d, append leader log len %d", rf.me, i, len(args.Logs[i-args.PrevLogIndex-1:]))
				break
			}
		}
		if i == end_check {
			Debug(dInfo, "S%d find no conflict with leader log, my log len %d", rf.me, rf.absoluteIndex(len(rf.log)))
			if !longer_than_leader {
				// no conflict, but leader is longer
				rf.log = append(rf.log, args.Logs[rf.absoluteIndex(len(rf.log)-1)-args.PrevLogIndex:]...)
				Debug(dInfo, "S%d append leader log due to leader is longer, my log current len %d", rf.me, rf.absoluteIndex(len(rf.log)))
			}
		}

		// persist
		rf.persist()
		// advance commit index
		if args.LeaderCommit > rf.commitIndex {
			rec_index := rf.commitIndex
			if args.LeaderCommit > rf.absoluteIndex(len(rf.log)-1) {
				rf.commitIndex = rf.absoluteIndex(len(rf.log) - 1)
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			Debug(dInfo, "S%d advance its commit index from %d to %d", rf.me, rec_index, rf.commitIndex)
			rf.condForCommitLog.Broadcast()
		}
	}
}

// handler of InstallSnapshot RPC
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		Debug(dInfo, "S%d reject Install RPC from S%d due to its older term of %d", rf.me, args.LeaderId, args.Term)
		return
	}
	// only leader call AppendEntries RPC
	if args.Term > rf.currentTerm {
		Debug(dInfo, "S%d update current term from %d to %d by Install RPC", rf.me, rf.currentTerm, args.Term)
		reply.Term = args.Term
		rf.currentTerm = args.Term // update term
		rf.votedFor = -1
		// persist
		rf.persist()
		if rf.isLeader {
			rf.isLeader = false
			Debug(dLeader, "S%d from leader turn to follower by Install RPC", rf.me)
		}
	}

	if rf.lastSnapShotIndex > args.LastIncludedIndex {
		Debug(dInfo, "S%d recieve outdated Install RPC from S%d, my last snapshot index %d while its %d", rf.me, args.LeaderId, rf.lastSnapShotIndex, args.LastIncludedIndex)
		return
	} else if rf.lastSnapShotIndex == args.LastIncludedIndex && rf.lastSnapShotTerm != args.LastIncludedTerm {
		Debug(dError, "S%d receive Install RPC from S%d with last snapshot index %d, my term %d while its term %d", rf.me, args.LeaderId, rf.lastSnapShotIndex, rf.lastSnapShotTerm, args.LastIncludedTerm)
		panic("Snapshot inconsistency")
	}

	// treat as heartbeat??
	Debug(dInfo, "S%d receive Install RPC from S%d with last snapshot index %d and term %d, heart beat at time %s", rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, time.Now())
	rf.receiveHeartBeat = true

	// notify
	rf.condForCommitLog.Broadcast()
	abs_last_index := rf.absoluteIndex(len(rf.log) - 1)
	if len(rf.log) == 0 {
		Debug(dInfo, "S%d no need to truncate rf.log while its relative len is 0", rf.me)
	} else if args.LastIncludedIndex >= abs_last_index {
		rf.log = rf.log[:0]
		Debug(dInfo, "S%d clear all rf.log while original last log index %d", rf.me, abs_last_index)
	} else {
		rlt_index := rf.relativeIndex(args.LastIncludedIndex)
		rf.log = rf.log[rlt_index+1:]
		Debug(dInfo, "S%d clear rf.log before index %d(while relative index %d), current relative log len %d", rf.me, args.LastIncludedIndex, rlt_index, len(rf.log))
	}
	// put code below at last, otherwise may disturb above code logic
	rf.lastSnapShotIndex = args.LastIncludedIndex
	rf.lastSnapShotTerm = args.LastIncludedTerm
	// persist
	rf.persist2(args.Data)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	try_rpc := false
	rf.mu.Lock()
	index := 0
	term := rf.currentTerm
	isLeader := rf.isLeader
	if isLeader {
		index = rf.absoluteIndex(len(rf.log))
		rf.log = append(rf.log, LogEntry{term, command})
		Debug(dLeader, "S%d get command at log index %d with current term %d", rf.me, index, term)
		rf.batchNum++
		if rf.batchNum == rf.batchThreshold {
			rf.batchNum = 0
			try_rpc = true
		}
		rf.persist()
	}
	rf.mu.Unlock()

	if isLeader && try_rpc {
		go rf.tryAppendEntries(term, false)
	}
	// Your code here (2B).

	return index, term, isLeader
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

// hold the lock when call `checkTerm`
func (rf *Raft) checkTerm(curr_term int, description string) bool {
	if curr_term == rf.currentTerm {
		// no log output when normal
		// Debug(dLeader, "S%d %s check term ok, current term %d", rf.me, description, curr_term)
		return true
	} else {
		Debug(dLeader, "S%d %s check term fail, current term %d but want term %d", rf.me, description, rf.currentTerm, curr_term)
		return false
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		var rand_sleep int
		_, is_leader := rf.GetState()
		if is_leader {
			rand_sleep = rf.heartbeatTimeout
		} else {
			rand_sleep = rand.Int()%(rf.electionTimeoutHigh-rf.electionTimeoutLow) + rf.electionTimeoutLow
		}
		Debug(dInfo, "S%d is about to sleep %d ms", rf.me, rand_sleep)
		sleep_time := 0
		for sleep_time < rand_sleep {
			cur_sleep := rf.heartbeatTimeout
			if rand_sleep-sleep_time < rf.heartbeatTimeout {
				cur_sleep = rand_sleep - sleep_time
			}
			time.Sleep(time.Millisecond * time.Duration(cur_sleep))
			sleep_time += cur_sleep
			_, is_leader = rf.GetState()
			if is_leader && sleep_time != rand_sleep {
				Debug(dInfo, "S%d is leader and early stop sleep, have sleeped %d ms with total %d ms", rf.me, sleep_time, rand_sleep)
				break
			}
		}

		// double check
		// key idea is to bind isLeader and currentTerm together
		Debug(dInfo, "S%d is kicked off at time %s", rf.me, time.Now())
		rf.mu.Lock()
		is_leader = rf.isLeader
		curr_term := rf.currentTerm
		election := !rf.receiveHeartBeat
		rf.receiveHeartBeat = false
		rf.mu.Unlock()

		if is_leader {
			// no guarantee it's leader at newest term when sendHeartBeat
			// maybe someone is leader at newer term
			go rf.tryAppendEntries(curr_term, true)
		} else if election {
			go rf.startElection(curr_term + 1)
		}
	}
}

// check whether new committed log can be send to apply channel
// every raft server need it!
func (rf *Raft) sendCommitedLog() {
	prev_commit_index := 0
	for !rf.killed() {
		rf.mu.Lock()
		// if directly wait without check, will possibly lose wakeup
		// so if no new command comes in, will never send commit log to channel
		for prev_commit_index == rf.commitIndex {
			rf.condForCommitLog.Wait()
		}

		to_send := make([]ApplyMsg, 0)
		// check snapshot
		if rf.lastSnapShotIndex > prev_commit_index {
			prev_commit_index = rf.lastSnapShotIndex
			// send snapshot through channel
			data := rf.persister.ReadSnapshot()
			// Debug(dLog, "S%d read snapshot byte %v", data)
			to_send = append(to_send, ApplyMsg{false, 0, 0, true, data, rf.lastSnapShotTerm, rf.lastSnapShotIndex})
			Debug(dInfo, "S%d confirm log commit index %d to last snapshot index %d", rf.me, prev_commit_index, rf.lastSnapShotIndex)
		}

		rec_prev_commit := prev_commit_index
		for prev_commit_index < rf.commitIndex {
			prev_commit_index++
			rlt_commit_index := rf.relativeIndex(prev_commit_index)
			command := rf.log[rlt_commit_index].Command
			to_send = append(to_send, ApplyMsg{true, command, prev_commit_index, false, []byte{}, 0, 0})
		}
		if rec_prev_commit < rf.commitIndex {
			Debug(dInfo, "S%d confirm commit log index from %d to %d by sending to apply channel", rf.me, rec_prev_commit+1, rf.commitIndex)
		}
		rf.mu.Unlock()
		// don't hold lock when send to channel
		for i := 0; i < len(to_send); i++ {
			rf.applyCh <- to_send[i]
		}

	}
}

// try to append log entries to follower
func (rf *Raft) tryAppendEntries(curr_term int, timeout bool) {
	keep_send := true
	success := 1
	total := len(rf.peers)

	rf.mu.Lock()
	// 1.check term
	// 2.fix same actual log len for every go routine below
	// eliminate unnecessary Append RPC when isn't heartbeat timeout
	abs_log_len := rf.absoluteIndex(len(rf.log))
	if !rf.checkTerm(curr_term, "before targeting last log index") || (!timeout && rf.maxSendLogLen == abs_log_len) {
		rf.mu.Unlock()
		return
	}
	if rf.maxSendLogLen > abs_log_len {
		Debug(dLeader, "S%d max send log len %d, but current target log len %d", rf.me, rf.maxSendLogLen, abs_log_len)
		panic("Max send log len error")
	}
	rf.maxSendLogLen = abs_log_len
	Debug(dLeader, "S%d with current term %d, prepare to send Append RPC targeting last log index %d with timeout flag %t", rf.me, curr_term, abs_log_len-1, timeout)
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers) && keep_send; i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			// no need to protect rf.me, due to no write on it
			rf.mu.Lock()
			if !rf.checkTerm(curr_term, "before sending Append RPC") {
				rf.mu.Unlock()
				return
			}
			if abs_log_len > rf.absoluteIndex(len(rf.log)) {
				Debug(dError, "S%d previous log len %d, but current log len %d", rf.me, abs_log_len, rf.absoluteIndex(len(rf.log)))
				panic("Leader log is truncated")
			}
			abs_prev_log_index := rf.nextIndex[i] - 1
			rlt_prev_log_index := rf.relativeIndex(abs_prev_log_index)
			// consider how to deal with prev_log_index < -1??
			if rlt_prev_log_index < -1 {
				Debug(dLeader, "S%d want to check previous index %d for S%d, but snapshot last index %d", rf.me, abs_prev_log_index, i, rf.lastSnapShotIndex)
				rf.mu.Unlock()
				// call install snapshot rpc??
				go rf.tryInstallSnapshot(curr_term, i)
				return
			}
			term_of_prev_log := rf.termByRelativeIndex(rlt_prev_log_index)
			if abs_log_len < 1+abs_prev_log_index {
				Debug(dLeader, "S%d want to send Append RPC to S%d from %d to %d, current term %d current total log len %d, early return", rf.me, i, abs_prev_log_index+1, abs_log_len-1, curr_term, rf.absoluteIndex(len(rf.log)))
				rf.mu.Unlock()
				return
			}
			log := make([]LogEntry, abs_log_len-1-abs_prev_log_index)
			if len(log) > 0 {
				Debug(dLeader, "S%d leader send Append RPC to S%d with log start from %d to %d", rf.me, i, abs_prev_log_index+1, abs_log_len-1)
				// should set end position(log_len)
				// otherwise current log len may > log_len, cause committed log may be overwritten
				copy(log, rf.log[rlt_prev_log_index+1:rf.relativeIndex(abs_log_len)])
			} else {
				Debug(dLeader, "S%d leader send Append RPC to S%d with no log entry", rf.me, i)
			}
			commit_index := rf.commitIndex
			rf.mu.Unlock()

			args := AppendEntriesArgs{curr_term, rf.me, abs_prev_log_index, term_of_prev_log, commit_index, log}
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(i, &args, &reply)
			if ok {
				Debug(dLeader, "S%d get Append RPC reply from S%d without timeout, with content: %t, %d, %d, %d", rf.me, i, reply.Success, reply.Term, reply.ConflictIndex, reply.ConflictTerm)
			} else {
				Debug(dLeader, "S%d get Append RPC reply from S%d in failure due to timeout", rf.me, i)
				return
			}

			// get reply and check term
			rf.mu.Lock()
			if !rf.checkTerm(curr_term, "after receiving peer Append RPC reply") {
				rf.mu.Unlock()
				return
			}
			if reply.Term < curr_term {
				Debug(dError, "S%d with current term %d, but get reply from S%d with term %d", rf.me, curr_term, i, reply.Term)
				panic("Reply term is too small")
			}
			// all situation with ok is true
			if reply.Term > curr_term {
				// implicit set keep_send to false
				Debug(dLeader, "S%d get Append RPC reply from S%d but find myself old in term %d, turn to follower and new turn", rf.me, i, curr_term)
				rf.isLeader = false
				rf.currentTerm = reply.Term
				rf.votedFor = -1 // indicate none
				// persist
				rf.persist()
			} else if !reply.Success {
				// inconsistency at log entries
				Debug(dLeader, "S%d find S%d inconsistent at previous log index %d", rf.me, i, abs_prev_log_index)
				// both trg_index && candidate_index are actual index (not relative)
				trg_index := reply.ConflictIndex
				candidate_index := -1
				if reply.ConflictTerm != -1 {
					for i := len(rf.log) - 1; i >= 0; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							candidate_index = rf.absoluteIndex(i + 1)
							break
						}
					}
				}
				if candidate_index != -1 {
					trg_index = candidate_index
				}
				if trg_index > abs_prev_log_index {
					Debug(dError, "S%d when no speed up of next index will set follower S%d to %d, but current set to %d", rf.me, i, abs_prev_log_index, trg_index)
					panic("Next index speed up fail")
				}
				if rf.nextIndex[i] > trg_index {
					Debug(dLeader, "S%d decrement next index of S%d from %d to %d", rf.me, i, rf.nextIndex[i], trg_index)
					rf.nextIndex[i] = trg_index
				}
			} else if reply.Success {
				if len(log) > 0 {
					Debug(dLeader, "S%d append log from %d to %d to follower S%d in success", rf.me, abs_prev_log_index+1, abs_log_len-1, i)
				} else {
					Debug(dLeader, "S%d append no log to follower S%d in success", rf.me, i)
				}
				success++
				// update next index of i if necessary
				if rf.nextIndex[i] < abs_log_len {
					Debug(dLeader, "S%d increment next index of S%d from %d to %d", rf.me, i, rf.nextIndex[i], abs_log_len)
					rf.nextIndex[i] = abs_log_len
				}
				// only commit current term's log
				// if commit index < abs_log_len -1, then last snap shot index < abs_log_len - 1
				if success == total/2+1 && rf.commitIndex < abs_log_len-1 && rf.termByRelativeIndex(rf.relativeIndex(abs_log_len-1)) == curr_term {
					Debug(dLeader, "S%d leader increment log commit index from %d to %d", rf.me, rf.commitIndex, abs_log_len-1)
					rf.commitIndex = abs_log_len - 1
					// notify go routine
					rf.condForCommitLog.Broadcast()
				}
			}
			rf.mu.Unlock()
		}(i)
		rf.mu.Lock()
		keep_send = rf.currentTerm == curr_term
		rf.mu.Unlock()
	}
}

// try to install snapshot for a certain follower
func (rf *Raft) tryInstallSnapshot(curr_term int, i int) {

	rf.mu.Lock()
	// check term
	if !rf.checkTerm(curr_term, "before installing snapshot") || !rf.checkInstallInterval(i) {
		rf.mu.Unlock()
		return
	}

	last_snapshot_index := rf.lastSnapShotIndex // record, in case of new snapshot come in
	snapshot := rf.persister.ReadSnapshot()
	args := InstallSnapshotArgs{curr_term, rf.me, last_snapshot_index, rf.lastSnapShotTerm, snapshot}
	Debug(dLeader, "S%d ready to send Install RPC to S%d with last snapshot index %d and snapshot term %d", rf.me, i, rf.lastSnapShotIndex, rf.lastSnapShotTerm)
	rf.mu.Unlock()

	var reply InstallSnapshotReply
	ok := rf.sendInstallSnapshot(i, &args, &reply)
	if ok {
		Debug(dLeader, "S%d get Install RPC reply from S%d without timeout, with content: %d", rf.me, i, reply.Term)
	} else {
		Debug(dLeader, "S%d get Install RPC reply from S%d in failure due to timeout", rf.me, i)
		return
	}

	// get reply and check term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.checkTerm(curr_term, "after receiving peer Append RPC reply") {
		return
	}
	if reply.Term < curr_term {
		Debug(dError, "S%d with current term %d, but get reply from S%d with term %d", rf.me, curr_term, i, reply.Term)
		panic("Reply term is too small")
	}
	if reply.Term > curr_term {
		Debug(dLeader, "S%d get Install RPC reply from S%d but find myself old in term %d, turn to follower and new turn", rf.me, i, curr_term)
		rf.isLeader = false
		rf.currentTerm = reply.Term
		rf.votedFor = -1 // indicate none
		// persist
		rf.persist()
		return
	}

	if rf.nextIndex[i] < last_snapshot_index+1 {
		Debug(dLeader, "S%d increment next index of S%d from %d to %d by Install RPC", rf.me, i, rf.nextIndex[i], last_snapshot_index+1)
		rf.nextIndex[i] = last_snapshot_index + 1
	}
	// impossible to advance commit index
	// so no need to notify go routine
}

// start a election
func (rf *Raft) startElection(curr_term int) {
	rf.mu.Lock()
	if !rf.checkTerm(curr_term-1, "before starting election") {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	if rf.isLeader {
		rf.isLeader = false
		// just after election timeout, decide to start new election, but be leader of previous term
		Debug(dLeader, "S%d is leader at previous term %d, but start election at next term %d, so be candidate", rf.me, curr_term-1, curr_term)
	}

	Debug(dInfo, "S%d start election due to heartbeat timeout, vote for self, and increment term to %d", rf.me, curr_term)
	rf.votedFor = rf.me
	rf.persist()
	rf.mu.Unlock()

	total := len(rf.peers)
	// protected by rf.mu
	finish := 1
	votes := 1
	keep_election := true

	for i := 0; i < total; i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			// no need to protect rf.me, due to no write on it
			rf.mu.Lock()
			if !rf.checkTerm(curr_term, "before sending Vote RPC") {
				rf.mu.Unlock()
				return
			}
			rlt_last_index := len(rf.log) - 1
			abs_last_index := rf.absoluteIndex(rlt_last_index)
			last_term := rf.termByRelativeIndex(rlt_last_index)
			rf.mu.Unlock()

			args := RequestVoteArgs{curr_term, rf.me, abs_last_index, last_term}
			var reply RequestVoteReply
			ok := rf.sendRequestVote(i, &args, &reply)
			if ok {
				Debug(dLeader, "S%d get Vote RPC reply from S%d without timeout, with content: %t, %d", rf.me, i, reply.VoteGranted, reply.Term)
			} else {
				Debug(dLeader, "S%d get Vote RPC reply from S%d in failure due to timeout", rf.me, i)
				return
			}

			// get reply and check term
			rf.mu.Lock()
			if !rf.checkTerm(curr_term, "after receiving peer Vote RPC reply") {
				rf.mu.Unlock()
				return
			}
			finish++
			if reply.VoteGranted {
				votes++
				Debug(dInfo, "S%d get vote from S%d at term %d", rf.me, i, curr_term)
			} else if reply.Term > curr_term {
				Debug(dInfo, "S%d get Vote RPC reply from S%d but find myself old in term %d, turn to follower and new term", rf.me, i, curr_term)
				if rf.isLeader {
					Debug(dInfo, "S%d be leader before end of entire election, but find S%d with new term %d", rf.me, i, reply.Term)
				}
				rf.isLeader = false
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				keep_election = false
			} else if !reply.VoteGranted {
				Debug(dInfo, "S%d fail to get vote peer S%d", rf.me, i)
			}
			if !keep_election || finish == total || votes > total/2 {
				rf.condForVote.Broadcast()
			}
			rf.mu.Unlock()
		}(i)
		rf.mu.Lock()
		keep_election = rf.currentTerm == curr_term
		if !keep_election {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	// don't forget myself's vote
	for keep_election && finish != total && votes <= total/2 {
		rf.condForVote.Wait()
	}
	if !rf.checkTerm(curr_term, "after finishing election") {
		rf.mu.Unlock()
		return
	}
	if votes <= total/2 || !keep_election {
		// fail to be leader
		Debug(dInfo, "S%d fail to be leader at term %d(no enought votes or detect newer term)", rf.me, curr_term)
		rf.mu.Unlock()
	} else {
		// be leader
		if rf.isLeader {
			Debug(dError, "S%d already be leader at term %d", rf.me, curr_term)
			panic("Already be leader at current term")
		}
		rf.isLeader = true
		Debug(dLeader, "S%d be leader at term %d", rf.me, curr_term)
		// re-init
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.absoluteIndex(len(rf.log))
			rf.installTime[i] = time.Now()
		}
		rf.maxSendLogLen = 0 // important to reset
		// send heart beat to inform all followers
		rf.mu.Unlock()
		go rf.tryAppendEntries(curr_term, true) // immediately announce
	}
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
	rf.currentTerm = 0
	rf.votedFor = -1 // indicate none at first
	rf.isLeader = false
	rf.receiveHeartBeat = false
	rf.commitIndex = 0
	rf.lastSnapShotIndex = -1 // indicate no snapshot yet
	rf.lastSnapShotTerm = -1
	rf.heartbeatTimeout = 150    // ms
	rf.electionTimeoutLow = 450  // ms
	rf.electionTimeoutHigh = 600 // ms
	rf.installInterval = 10      // ms
	rf.batchThreshold = 1
	rf.batchNum = 0
	rf.maxSendLogLen = 0

	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{0, 0}) // sentinel
	rf.nextIndex = make([]int, len(peers))
	rf.installTime = make([]time.Time, len(peers))
	rf.condForVote = sync.NewCond(&rf.mu)
	rf.condForCommitLog = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = rf.absoluteIndex(len(rf.log)) // acutal index
		rf.installTime[i] = time.Now()
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendCommitedLog()
	return rf
}
