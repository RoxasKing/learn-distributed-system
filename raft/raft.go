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
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoxasKing/learn-distributed-system/labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Command interface{}
	Index   int
	Term    int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

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

	// Persistent state on all servers
	currentTerm int      // latest term server has seen(initialized to 0 on first boot, increases monotonically)
	votedFor    int      // candidated that received vote in current term(or null if none, initialize to -1 as null)
	logs        []*Entry // log entries; each entry contains command for state machine, and term when entry was received by leader(first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be commited(initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine(initialized to 0, increases monotonically)

	// volatile state on leaders
	nextIndexs []int // for each server, index of the next log entry to send to that server(initialized to leader last log index+1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)

	state             State           // server states: follower, candidate or leader(initialize to 0 as follower)
	electionTicker    *time.Ticker    // election timeouts are chosen randomly from a fixed interval(150~300ms)
	heartbeatTicker   *time.Ticker    // keep followers from starting elections
	killAllCtx        context.Context // receive cancel signal
	killAllFunc       func()          // cancel all goroutines
	killHeartbeatCtx  context.Context // receive heartbeat cancel signal
	killHeartbeatFunc func()          // cancel heartbeat goroutine
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
	isleader = rf.state == Leader

	// DPrintf("is leader = %v\n", rf.state == Leader)

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

func getRandomNumber(from, to int) int {
	rand.Seed(rand.Int63())
	return from + rand.Intn(to-from+1)
}

// reset election timeout ticker
func (rf *Raft) resetElectionTimeoutTicker() {
	rf.electionTicker.Reset(time.Duration(getRandomNumber(150, 300)) * time.Millisecond)
}

func (rf *Raft) convertToFollower() {
	if rf.state == Leader {
		rf.killHeartbeatFunc()
	}
	rf.state = Follower // become a follower
	rf.votedFor = -1    // reset when convert to follower
	DPrintf("service %d become follower, term is %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) convertToCandidate() {
	rf.state = Candidate // become candidate
	rf.currentTerm++     // increment current term
	rf.votedFor = rf.me  // vote for self
	DPrintf("service %d become candidate, term is %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader             // become a leader
	rf.electionTicker.Stop()      // stop election timeout
	go rf.heartbeatTimeoutCheck() // start sending heartbeat
	DPrintf("service %d become leader, term is %d\n", rf.me, rf.currentTerm)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// request contains term > currentTerm, and current state is not follower, convert to follower
		if rf.state != Follower {
			rf.convertToFollower()
		}
	}

	var lastLogIndex, lastLogTerm int
	if len(rf.logs) != 0 {
		lastLog := rf.logs[len(rf.logs)-1]
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	}

	// if candidate's term is smaller than current term,
	// or already voted for the other candidates,
	// or candidate's log is out of date,
	// reply currentTerm, and don't grant vote to this candidate
	if args.Term < rf.currentTerm || -1 != rf.votedFor && args.CandidateId != rf.votedFor ||
		args.LastLogIndex < lastLogIndex || args.LastLogTerm < lastLogTerm {
		// DPrintf("---Reject!---")
		// DPrintf("current term %d, term %d\n", rf.currentTerm, args.Term)
		// DPrintf("voted for %d, me is %d\n", rf.votedFor, rf.me)
		// DPrintf("lastLogIndex %d, receive lastLogIndex %d", lastLogIndex, args.LastLogIndex)
		// DPrintf("lastLogTerm %d, receive lastLogTerm %d", lastLogTerm, args.LastLogTerm)
		// DPrintf("-------------\n")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.resetElectionTimeoutTicker() // reset election timeout ticker
	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

type AppendEntriesArgs struct {
	Term         int      // leader's term
	LeaderId     int      // so followers can redirect clients
	PrevLogIndex int      // index of log entry immediately preceding new ones
	PrevLogTerm  int      // term of prevLogIndex entry
	Entries      []*Entry // log entries to store(empty for heartbeat; may send more than one for efficiently)
	LeaderCommit int      // leader's commit index
}

type AppendEntriesReply struct {
	Term    int  // current Term, for leader to update itself
	Success bool // true if follower contained entry matching preLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// var lastLogIndex, lastLogTerm int
	// if len(rf.logs) != 0 {
	// 	lastLog := rf.logs[len(rf.logs)-1]
	// 	lastLogIndex = lastLog.Index
	// 	lastLogTerm = lastLog.Term
	// }

	// TODO
	// if leader's term is smaller than current term,
	// or log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// reply false
	if args.Term < rf.currentTerm /* || args.PrevLogIndex > lastLogIndex || args.PrevLogTerm > lastLogTerm */ {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		// update currentTerm to leader's term
		rf.currentTerm = args.Term
		// if current state is not follower, and request contains term > currentTerm, convert to follower
		if rf.state != Follower {
			rf.convertToFollower()
		}
	}

	rf.resetElectionTimeoutTicker()

	// append new entries to logs, if request contains
	rf.logs = append(rf.logs, args.Entries...)

	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(args.Entries) != 0 {
			lastNewEntryIndex := args.Entries[len(args.Entries)-1].Index
			rf.commitIndex = Min(rf.commitIndex, lastNewEntryIndex)
		}
	}

	reply.Term = rf.currentTerm
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
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

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
	rf.killAllFunc()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []*Entry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndexs = []int{}
	rf.matchIndex = []int{}

	rf.killAllCtx, rf.killAllFunc = context.WithCancel(context.Background())
	rf.killHeartbeatCtx, rf.killHeartbeatFunc = context.WithCancel(context.Background())

	rf.electionTicker = time.NewTicker(time.Duration(getRandomNumber(150, 300)) * time.Millisecond)
	go rf.electionTimeoutCheck()

	rf.heartbeatTicker = time.NewTicker(10 * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) electionTimeoutCheck() {
	for {
		select {
		case <-rf.electionTicker.C: // when timeout, convert to candicate, start election
			go rf.startElection()
		case <-rf.killAllCtx.Done():
			return
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.convertToCandidate()

	var lastLogIndex, lastLogTerm int
	if len(rf.logs) != 0 {
		lastLog := rf.logs[len(rf.logs)-1]
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	}

	requestArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.mu.Unlock()

	votesCount := 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, requestArgs, &votesCount)
	}

}

func (rf *Raft) sendRequestVote(index int, args *RequestVoteArgs, votesCount *int) {
	reply := &RequestVoteReply{}

	// reply.Term < rf.currentTerm , prevent receipt of overdue votes
	if !rf.peers[index].Call("Raft.RequestVote", args, reply) || reply.Term < rf.currentTerm {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.VoteGranted && rf.state != Leader {
		*votesCount++
		// DPrintf("service %d, got %d vote, peers %d\n", rf.me, *votesCount, len(rf.peers))
		if *votesCount <= len(rf.peers)>>1 {
			return
		}
		// if receives votes from majority of servers, convert from candidate to leader
		rf.convertToLeader()
	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term // update current term
		// if its term is out of date, convert from candidate to follower
		if rf.state != Follower {
			rf.convertToFollower()
			rf.resetElectionTimeoutTicker() // reset election timeout ticker
		}
	}
}

func (rf *Raft) heartbeatTimeoutCheck() {
	// when become a leader, send heartbeat immediately
	go rf.startHeartbeat()

	for {
		select {
		case <-rf.heartbeatTicker.C:
			go rf.startHeartbeat()
		case <-rf.killAllCtx.Done():
			return
		case <-rf.killHeartbeatCtx.Done():
			return
		}
	}
}

func (rf *Raft) startHeartbeat() {
	rf.mu.Lock()

	currentTerm := rf.currentTerm
	prevLogIndex, prevLogTerm := 0, 0
	if len(rf.logs) != 0 {
		lastLog := rf.logs[len(rf.logs)-1]
		prevLogIndex = lastLog.Index
		prevLogTerm = lastLog.Term
	}

	args := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []*Entry{},
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendHeartbeat(i, currentTerm, args)
	}
}

func (rf *Raft) sendHeartbeat(index, currentTerm int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if !rf.peers[index].Call("Raft.AppendEntries", args, reply) || reply.Term < currentTerm {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !reply.Success && reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term // update current term
		// if its term is out of date, convert from leader to follower
		if rf.state != Follower {
			rf.convertToFollower()
			rf.resetElectionTimeoutTicker() // reset election timeout ticker
		}
	}
}
