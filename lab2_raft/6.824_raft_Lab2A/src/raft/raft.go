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
	"math/rand"

	//	"bytes"
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
type State int

const (
	Follower = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "error_no_such_state"
	}
}

type Log struct {
	Command interface{}
	Index 	int
	Term 	int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	currentTerm int
	voteFor     int

	state        State
	electionTime time.Time
	// 2B
	logs []Log
	commitIndex int
	nextIndex []int
	applyCh chan ApplyMsg   	// ??????????????????????????????????????????
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
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
	Term        int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
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

//
// example RequestVote RPC handler.
//
//func (rf *Raft) logisNewerThan(lastLogTerm int, lastLogIndex int) {
//	if rf.getLastLog().Term
//}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	//if rf.logisNewerThan(args.LastLogTerm, args.LastLogIndex)
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm, rf.voteFor = args.Term, -1
	}
	rf.voteFor = args.CandidateId
	rf.electionTime = time.Now()
	reply.Term, reply.VoteGranted = rf.currentTerm, true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = (rf.state == Leader)
	if !isLeader {
		return -1,-1,isLeader
	}

	index = len(rf.logs)
	term = rf.currentTerm
	rf.logs = append(rf.logs, Log{Command: command,Index: index,Term: term})
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, command, rf.currentTerm)
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

func (rf *Raft) getLastLog() Log {
	return rf.logs[ len(rf.logs) - 1 ]
}

func (rf *Raft) election() {
	rf.state = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	voteCount := 1
	flag := true
	rf.electionTime = time.Now()
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm: rf.getLastLog().Term,
	}
	reply := RequestVoteReply{}
	for peer := range rf.peers {
		if rf.me == peer {
			continue
		}
		DPrintf("[Node %d] ?????????????????????", rf.me)
		go func(args RequestVoteArgs, reply RequestVoteReply, peer int) {
			DPrintf("???Node %d???????????? %d??????vote??????", rf.me, peer)
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGranted {
					DPrintf("[Node %d] ????????? [Node %d]", peer, rf.me)
					voteCount++
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.voteFor = -1
						rf.state = Follower // ??????????????????????????????follwer
					}
					return
				}
				if voteCount > len(rf.peers)/2 && flag && rf.state == Candidate {
					DPrintf("[Node %d]??????leader", rf.me)
					flag = false
					rf.state = Leader
					rf.beatHeart()
				}
			} else {
				DPrintf("[Node %d] to [Node %d] ???????????????????????????", args.CandidateId, peer)
			}
		}(args, reply, peer)
	}
}

type BeatHeartArgs struct {
	Term     int
	LeaderId int
	IsAppend bool
	AppendLog Log
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int
}
type BeatHeartReply struct {
	Term    int
	Success bool
}
func (rf *Raft) match(prevLogIndex int, PrevLogterm int) bool {
	// ???????????????????????????match
	if rf.getLastLog().Term == PrevLogterm && rf.getLastLog().Index == prevLogIndex {
		return true
	}
	return false
}
func (rf *Raft) RequestBeatHeart(args *BeatHeartArgs, reply *BeatHeartReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if reply.Term > args.Term {
		reply.Success = false
		return
	}
	if args.IsAppend {
		if rf.match(args.PrevLogIndex, args.PrevLogTerm) {
			rf.logs = append(rf.logs, args.AppendLog) // ???????????????
			reply.Success = true
		} else {
			reply.Success = false
		}
	} else {
		reply.Success = true
	}
	rf.electionTime = time.Now() // ???????????????
}
func (rf *Raft) sendBeatHeart(server int, args *BeatHeartArgs, reply *BeatHeartReply) bool {
	ok := rf.peers[server].Call("Raft.RequestBeatHeart", args, reply)
	return ok
}
func (rf *Raft) beatHeart() {
	args := BeatHeartArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	reply := BeatHeartReply{}
	for peer := range rf.peers {
		if peer == rf.me {
			rf.electionTime = time.Now()
			continue
		}
		if rf.nextIndex[peer] < len(rf.logs) - 1 {
			// ????????????
			args.AppendLog = rf.logs[rf.nextIndex[peer]+1]
			args.PrevLogIndex = rf.logs[rf.nextIndex[peer]].Index
			args.PrevLogTerm = rf.logs[rf.nextIndex[peer]].Term
			args.IsAppend = true // ???????????????????????????
		} else {
			args.IsAppend = false // ????????????
		}

		go func(args BeatHeartArgs, reply BeatHeartReply, peer int) {
			if rf.sendBeatHeart(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.state = Follower // ?????????????????????
					return
				}
				if !reply.Success {
					rf.nextIndex[peer]-- // ??????????????????????????????
				} else {
					rf.nextIndex[peer]++ // ??????????????????????????????????????????
				}
			} else {
				DPrintf("[Node %d] to [Node %d] ???????????????????????????", args.LeaderId, peer)
			}
		}(args, reply, peer)
	}

}

const beatHeartTime = 50 * time.Millisecond

func getRandomElectionTime() time.Duration {
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(beatHeartTime)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.beatHeart()
		}
		if rf.electionTime.Add(getRandomElectionTime()).Before(time.Now()) {
			DPrintf("[Node %d]: ???????????????", rf.me)
			rf.election()
		}
		rf.mu.Unlock()
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
	rf.state = Follower
	rf.voteFor = -1
	rf.currentTerm = 0
	rf.electionTime = time.Now()
	rf.logs = make([]Log, 1)
	rf.logs[0] = Log{Term: -1,Index: -1,Command: nil}
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
