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
	//	"bytes"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	currentTerm int
	votedFor    int

	isLeader            int32
	receiveHeartBeat    int32
	heartbeat           int // millisecond
	electionTimeoutLow  int // millisecond
	electionTimeoutHigh int // millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.isLeader != 0
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	// //log entries
	LeaderCommit int
}

// reply of AppendEntries RPC
type AppendEntriesReply struct {
	Term    int
	Success bool
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
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	if args.Term > rf.currentTerm {
		Debug(dInfo, "S%d update current term from %d to %d by Vote RPC", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		if rf.isLeader == 1 {
			Debug(dLeader, "S%d from leader turn to follower by Vote RPC at time %s", rf.me, time.Now())
		}
		rf.isLeader = 0 // important
	}
	Debug(dInfo, "S%d vote for S%d at term %d", rf.me, rf.votedFor, rf.currentTerm)
}

// handler of AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		Debug(dInfo, "S%d reject Append RPC from S%d due to its older term of %d", rf.me, args.LeaderId, args.Term)
		reply.Success = false
		return
	}
	// only leader call AppendEntries RPC
	if args.Term > rf.currentTerm {
		Debug(dInfo, "S%d update current term from %d to %d by Append RPC", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
	}
	Debug(dInfo, "S%d receive Append RPC normally from S%d", rf.me, args.LeaderId)
	rf.receiveHeartBeat = 1
	if rf.isLeader == 1 {
		Debug(dLeader, "S%d from leader turn to follower by Append RPC at time %s", rf.me, time.Now())
	}
	rf.isLeader = 0
	// rf.votedFor = -1 // no need to reset, will cause bug!
	reply.Success = true

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
			rand_sleep = rf.heartbeat
		} else {
			rand_sleep = rand.Int()%(rf.electionTimeoutHigh-rf.electionTimeoutLow) + rf.electionTimeoutLow
		}
		time.Sleep(time.Millisecond * time.Duration(rand_sleep))
		_, is_leader = rf.GetState()
		if is_leader {
			Debug(dTimer, "S%d prepare to send Append RPC at time %s", rf.me, time.Now())
			go rf.sendHeartBeat()
			continue
		}
		received_heart := atomic.LoadInt32(&rf.receiveHeartBeat)
		if received_heart != 0 {
			atomic.StoreInt32(&rf.receiveHeartBeat, 0)
		} else {
			// start election when no heart beat
			Debug(dTimer, "S%d do not receive heartbeat and prepare to start election", rf.me)
			go rf.startElection()
		}
	}
}

// send heart beat to follower
func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	curr_term := rf.currentTerm
	rf.mu.Unlock()
	// early stop by judging whether is leader
	for i := 0; i < len(rf.peers) && atomic.LoadInt32(&rf.isLeader) == 1; i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			// no need to protect rf.me, due to no write on it
			args := AppendEntriesArgs{curr_term, rf.me, 0, 0, 0}
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(i, &args, &reply)
			// for loop if ok is false??
			if ok {
				Debug(dInfo, "S%d send Append RPC to S%d in without timeout, with reply: %t, %d", rf.me, i, reply.Success, reply.Term)
			} else {
				Debug(dInfo, "S%d send Append RPC to S%d in failure due to timeout", rf.me, i)
			}
			if ok && !reply.Success && reply.Term > curr_term {
				Debug(dInfo, "S%d send Append RPC to S%d find myself old in term %d, turn to follower", rf.me, i, curr_term)
				atomic.StoreInt32(&rf.isLeader, 0)
			}
		}(i)
	}
}

// start a election
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm += 1
	if rf.isLeader == 1 {
		log.Panic("Leader for election")
	}
	curr_term := rf.currentTerm

	Debug(dInfo, "S%d at start of elction vote for myself, and increment term to %d", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.mu.Unlock()

	total := len(rf.peers)
	finish := 0   // protected by mutex
	votes := 0    // protected by mutex
	stop := false // protected by mutex
	var mutex sync.Mutex
	cond := sync.NewCond(&mutex)

	for i := 0; i < total; i++ {
		if i == rf.me {
			mutex.Lock()
			finish++
			votes++
			mutex.Unlock()
			continue
		}
		go func(i int) {
			// no need to protect rf.me, due to no write on it
			args := RequestVoteArgs{curr_term, rf.me, 0, 0}
			var reply RequestVoteReply
			ok := rf.sendRequestVote(i, &args, &reply)

			mutex.Lock()
			defer mutex.Unlock()
			finish++
			if ok && reply.VoteGranted {
				votes++
				Debug(dInfo, "S%d get vote from S%d", rf.me, i)
			} else if ok && reply.Term > curr_term {
				stop = true
				Debug(dInfo, "S%d terminate election due to know newer term %d compared to", rf.me, reply.Term, curr_term)
			} else if ok && !reply.VoteGranted {
				Debug(dInfo, "S%d fail to get vote peer already give others", rf.me)
			} else if !ok {
				Debug(dInfo, "S%d fail to get vote due to timeout to reach S%d", rf.me, i)
			}
			if stop || finish == total || votes > total/2 {
				cond.Broadcast()
			}
		}(i)
	}

	mutex.Lock()
	// don't forget myself's vote
	for !stop && finish != total && votes <= total/2 {
		cond.Wait()
	}
	be_leader := true
	if votes <= total/2 || stop {
		be_leader = false
	}
	mutex.Unlock()

	// check result and whether need to be follower
	if be_leader {
		atomic.StoreInt32(&rf.isLeader, 1)
		// send heart beat to inform all followers
		go rf.sendHeartBeat() // is it needed?? maybe error
		Debug(dLeader, "S%d be leader at term %d at time %s", rf.me, rf.currentTerm, time.Now())
	} else {
		Debug(dInfo, "S%d fail to be leader at term %d", rf.me, rf.currentTerm)
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
	rf.isLeader = 0
	rf.receiveHeartBeat = 0
	rf.heartbeat = 200            // ms
	rf.electionTimeoutLow = 800   // ms
	rf.electionTimeoutHigh = 1000 // ms
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
