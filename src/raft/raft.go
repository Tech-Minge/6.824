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

	isLeader            bool
	receiveHeartBeat    bool
	heartbeatTimeout    int // millisecond
	electionTimeoutLow  int // millisecond
	electionTimeoutHigh int // millisecond
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
	// log entries
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
		reply.Term = args.Term
		rf.currentTerm = args.Term // update to new term
		if rf.isLeader {
			rf.isLeader = false // important
			Debug(dLeader, "S%d from leader turn to follower by Vote RPC at time %s", rf.me, time.Now())
		}
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
		reply.Term = args.Term
		rf.currentTerm = args.Term // update term
		if rf.isLeader {
			rf.isLeader = false
			Debug(dLeader, "S%d from leader turn to follower by Append RPC at time %s", rf.me, time.Now())
		}
	}
	if rf.isLeader {
		// at same term, two leaders
		Debug(dError, "%d and %d are leaders at same term %d", rf.me, args.LeaderId, rf.currentTerm)
		panic("Two leaders at same term")
	}
	Debug(dInfo, "S%d receive Append RPC normally from S%d", rf.me, args.LeaderId)
	rf.receiveHeartBeat = true
	// rf.votedFor = -1 // no need to reset, will cause multi vote for others
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
			rand_sleep = rf.heartbeatTimeout
		} else {
			rand_sleep = rand.Int()%(rf.electionTimeoutHigh-rf.electionTimeoutLow) + rf.electionTimeoutLow
		}
		time.Sleep(time.Millisecond * time.Duration(rand_sleep))

		// double check
		// key idea is to bind isLeader and currentTerm together
		rf.mu.Lock()
		is_leader = rf.isLeader
		curr_term := rf.currentTerm
		election := !rf.receiveHeartBeat
		rf.receiveHeartBeat = false
		rf.mu.Unlock()

		if is_leader {
			// no guarantee it's leader at newest term when sendHeartBeat
			// maybe someone is leader at newer term
			go rf.sendHeartBeat(curr_term)
		} else if election {
			go rf.startElection(curr_term + 1)
		}
	}
}

// send heart beat to follower
func (rf *Raft) sendHeartBeat(curr_term int) {
	Debug(dTimer, "S%d prepare to send Append RPC at time %s", rf.me, time.Now())

	// early stop by judging whether at specific term
	keep_send := true
	for i := 0; i < len(rf.peers) && keep_send; i++ {
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
				rf.mu.Lock()
				// implicit set keep_send to false
				if rf.currentTerm == curr_term {
					Debug(dInfo, "S%d send Append RPC to S%d find myself old in term %d, turn to follower and new turn", rf.me, i, curr_term)
					if !rf.isLeader {
						Debug(dError, "S%d isn't leader at term %d", rf.me, curr_term)
						panic("Not leader at current term")
					}
					rf.isLeader = false
					rf.currentTerm = reply.Term
				} else {
					Debug(dInfo, "S%d send Append RPC to S%d althrough find myself old in term, but my term change from %d to %d", rf.me, i, curr_term, rf.currentTerm)
				}
				rf.mu.Unlock()
			}
		}(i)
		rf.mu.Lock()
		keep_send = rf.currentTerm == curr_term
		rf.mu.Unlock()
	}
}

// start a election
func (rf *Raft) startElection(curr_term int) {
	rf.mu.Lock()
	if curr_term != rf.currentTerm+1 {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	if rf.isLeader {
		Debug(dError, "S%d is leader at previous term %d, but start election at next term %d", rf.me, curr_term-1, curr_term)
		panic("Previouis term leader start next term's election")
	}

	Debug(dInfo, "S%d start elction and vote for myself, and increment term to %d", rf.me, curr_term)
	rf.votedFor = rf.me
	rf.mu.Unlock()

	total := len(rf.peers)
	// protected by rf.mu
	finish := 1
	votes := 1
	keep_election := true
	cond := sync.NewCond(&rf.mu)

	for i := 0; i < total && keep_election; i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			// no need to protect rf.me, due to no write on it
			args := RequestVoteArgs{curr_term, rf.me, 0, 0}
			var reply RequestVoteReply
			ok := rf.sendRequestVote(i, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			finish++
			if ok && reply.VoteGranted {
				votes++
				Debug(dInfo, "S%d get vote from S%d", rf.me, i)
			} else if ok && reply.Term > curr_term {
				if rf.currentTerm == curr_term {
					Debug(dInfo, "S%d send Vote RPC to S%d find myself old in term %d, turn to follower and new term", rf.me, i, curr_term)
					if rf.isLeader {
						Debug(dInfo, "S%d be leader before end of entire election, but find someone with new term", rf.me)
					}
					rf.isLeader = false
					rf.currentTerm = reply.Term
				} else {
					Debug(dInfo, "S%d send Vote RPC to S%d althrough find myself old in term, but my term change from %d to %d", rf.me, i, curr_term, rf.currentTerm)
				}
			} else if ok && !reply.VoteGranted {
				Debug(dInfo, "S%d fail to get vote peer already give others", rf.me)
			} else if !ok {
				Debug(dInfo, "S%d fail to get vote due to timeout to reach S%d", rf.me, i)
			}
			if !keep_election || finish == total || votes > total/2 {
				cond.Broadcast()
			}
		}(i)
		rf.mu.Lock()
		keep_election = rf.currentTerm == curr_term
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	// don't forget myself's vote
	for keep_election && finish != total && votes <= total/2 {
		cond.Wait()
	}

	if votes <= total/2 || !keep_election {
		// fail to be leader
		Debug(dInfo, "S%d fail to be leader at term %d(no enought votes or detect newer term)", rf.me, curr_term)
	} else {
		// maybe leader now, check term
		if rf.currentTerm == curr_term {
			// same term, be leader
			if rf.isLeader {
				Debug(dError, "S%d already be leader at term %d before set", rf.me, curr_term)
				panic("Already be leader at current term")
			}
			rf.isLeader = true
			Debug(dLeader, "S%d be leader at term %d at time %s", rf.me, curr_term, time.Now())
			// send heart beat to inform all followers
			go rf.sendHeartBeat(curr_term) // is it needed?? maybe error
		} else {
			Debug(dLeader, "S%d get majority votes, but term changes from %d to %d, fail to be leader", rf.me, curr_term, rf.currentTerm)
		}
	}
	rf.mu.Unlock()

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
	rf.heartbeatTimeout = 200     // ms
	rf.electionTimeoutLow = 800   // ms
	rf.electionTimeoutHigh = 1000 // ms
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
