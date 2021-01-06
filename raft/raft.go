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
	"context"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"github.com/RoxasKing/learn-distributed-system/labgob"
	"github.com/RoxasKing/learn-distributed-system/labrpc"
)

// import "bytes"
// import "../labgob"

const (
	electionTimeoutL         = 150
	electionTimeoutR         = 300
	heartbeatTimeoutDuration = 35 * time.Millisecond
)

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
	CommandTerm  int
	IsLeader     bool
	Snapshot     []byte
	StateChange  bool // if raft state convert from leader to follwer, apply true
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
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
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

	lastIncludedIndex int // snapshot's last included index
	lastIncludedTerm  int // snapshot's last included term

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be commited(initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine(initialized to 0, increases monotonically)

	// volatile state on leaders
	nextIndexs []int // for each server, index of the next log entry to send to that server(initialized to leader last log index+1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)

	applyCh                 chan ApplyMsg                 // send an ApplyMsg when commited new entry
	votesCount              int                           // count votes in election
	state                   State                         // server states (initialize to 0 as follower)
	electionTimeout         *time.Ticker                  // election timeouts
	heartbeatTimeout        *time.Ticker                  // keep followers from starting elections
	electionTimeoutNotifyCh chan struct{}                 // notify start election
	appendEntriesNotifyCh   chan struct{}                 // notify invoke AppendEntries RPC
	requestVoteCh           chan *requestVoteParam        // receive requestVote args
	appendEntriesCh         chan *appendEntriesParam      // receive appendEntries args
	requestVoteReplyCh      chan *requestVoteReplyParam   // receive requestVote reply
	appendEntriesReplyCh    chan *appendEntriesReplyParam // receive appendEntries reply
	startCh                 chan *startParam              // append new command to raft's log
	stateCh                 chan *stateParam              // get currentTerm and whether this server
	killCtx                 context.Context               // receive a message that kills all for select loop
	killFunc                func()                        // methods for killing all for select loops

	logcompactionCh        chan *logcompactionParam        // execute log compaction
	installSnapshotCh      chan *installSnapshotParam      // receive installSnapshot args
	installSnapshotReplyCh chan *installSnapshotReplyParam // receive installSnapshot reply
}

type stateParam struct {
	term     int
	isLeader bool
	waitCh   chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	if rf.killed() {
		return -1, false
	}

	status := &stateParam{waitCh: make(chan struct{}, 1)}
	go func() { rf.stateCh <- status }()

	<-status.waitCh

	term = status.term
	isleader = status.isLeader

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

	rf.persister.SaveRaftState(rf.generateRaftStateData())
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
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var logs []*Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("readPersist falied!\n")
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.logs = logs

	rf.commitIndex = lastIncludedIndex // update to lastIncludedIndex
	rf.lastApplied = lastIncludedIndex // update to lastIncludedIndex
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

type requestVoteParam struct {
	args    *RequestVoteArgs
	replyCh chan *RequestVoteReply
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	replyCh := make(chan *RequestVoteReply, 1)
	rf.requestVoteCh <- &requestVoteParam{args: args, replyCh: replyCh}

	out := <-replyCh

	reply.Term = out.Term
	reply.VoteGranted = out.VoteGranted
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
	Term         int  // current Term, for leader to update itself
	Success      bool // true if follower contained entry matching preLogIndex and prevLogTerm
	PrevLogIndex int  // leader must decrease new enties start index at this+1, not null if reply false
}

type appendEntriesParam struct {
	args    *AppendEntriesArgs
	replyCh chan *AppendEntriesReply
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	replyCh := make(chan *AppendEntriesReply, 1)
	rf.appendEntriesCh <- &appendEntriesParam{args: args, replyCh: replyCh}

	out := <-replyCh

	reply.Term = out.Term
	reply.Success = out.Success
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

type startParam struct {
	command  interface{}
	index    int
	term     int
	isLeader bool
	waitCh   chan struct{}
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
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (2B).

	waitCh := make(chan struct{}, 1)
	param := &startParam{command: command, waitCh: waitCh}
	go func() { rf.startCh <- param }()

	<-waitCh

	index := param.index
	term := param.term
	isLeader := param.isLeader

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
	rf.killFunc()
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

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.logs = []*Entry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndexs = []int{}
	rf.matchIndex = []int{}

	rf.applyCh = applyCh
	rf.votesCount = 0
	rf.state = Follower
	rf.electionTimeout = time.NewTicker(getRandomElectionTimeout())
	rf.heartbeatTimeout = time.NewTicker(heartbeatTimeoutDuration)
	rf.electionTimeoutNotifyCh = make(chan struct{})
	rf.appendEntriesNotifyCh = make(chan struct{})
	rf.requestVoteCh = make(chan *requestVoteParam)
	rf.requestVoteReplyCh = make(chan *requestVoteReplyParam)
	rf.appendEntriesCh = make(chan *appendEntriesParam)
	rf.appendEntriesReplyCh = make(chan *appendEntriesReplyParam)
	rf.stateCh = make(chan *stateParam)
	rf.startCh = make(chan *startParam)
	rf.killCtx, rf.killFunc = context.WithCancel(context.Background())

	rf.logcompactionCh = make(chan *logcompactionParam)
	rf.installSnapshotCh = make(chan *installSnapshotParam)
	rf.installSnapshotReplyCh = make(chan *installSnapshotReplyParam)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.stopHeartbeat()
	go run(rf)
	go electionTimeoutLoop(rf)
	go heartbeatTimeoutLoop(rf)

	return rf
}

func run(rf *Raft) {
	for {
		select {
		case <-rf.electionTimeoutNotifyCh:
			rf.initiatesElection()

		case <-rf.appendEntriesNotifyCh:
			rf.broadcastAppendEntries()

		case param := <-rf.requestVoteCh:
			param.replyCh <- rf.requestVoteHandle(param.args)

		case param := <-rf.appendEntriesCh:
			param.replyCh <- rf.appendEntriesHandle(param.args)

		case param := <-rf.requestVoteReplyCh:
			rf.requestVoteReplyHandle(param.args, param.reply)

		case param := <-rf.appendEntriesReplyCh:
			rf.appendEntriesReplyHandle(param.index, param.args, param.reply)

		case param := <-rf.startCh:
			rf.startHandle(param)

		case param := <-rf.stateCh:
			rf.getStateHandle(param)

		case param := <-rf.logcompactionCh:
			rf.LogCompactionHandle(param)

		case param := <-rf.installSnapshotCh:
			param.replyCh <- rf.installSnapshotHandle(param.args)

		case param := <-rf.installSnapshotReplyCh:
			rf.installSnapshotReplyHandle(param.index, param.args, param.reply)

		case <-rf.killCtx.Done():
			DPrintf("{%d} KILLED!, lastLogIndex:%d, term:%d\n",
				rf.me, rf.lastIncludedIndex+len(rf.logs), rf.currentTerm)
			return
		}
	}
}

func electionTimeoutLoop(rf *Raft) {
	for {
		select {
		case <-rf.electionTimeout.C:
			go func() { rf.electionTimeoutNotifyCh <- struct{}{} }()
		case <-rf.killCtx.Done():
			return
		}
	}
}

func heartbeatTimeoutLoop(rf *Raft) {
	for {
		select {
		case <-rf.heartbeatTimeout.C:
			go func() { rf.appendEntriesNotifyCh <- struct{}{} }()
		case <-rf.killCtx.Done():
			return
		}
	}
}

func getRandomElectionTimeout() time.Duration {
	rand.Seed(makeSeed())
	rand.Seed(makeSeed())
	return time.Duration(electionTimeoutL+rand.Intn(electionTimeoutR-electionTimeoutL+1)) * time.Millisecond
}

func (rf *Raft) stopElection() {
	rf.electionTimeout.Stop()
}

func (rf *Raft) resetElection() {
	rf.electionTimeout.Reset(getRandomElectionTimeout())
}

func (rf *Raft) stopHeartbeat() {
	rf.heartbeatTimeout.Stop()
}

func (rf *Raft) sendHeartbeatImmediately() {
	rf.heartbeatTimeout.Reset(heartbeatTimeoutDuration)
	go func() { rf.appendEntriesNotifyCh <- struct{}{} }()
}

func (rf *Raft) convertToFollower() {
	if rf.state == Leader {
		rf.stopHeartbeat()
	}
	rf.state = Follower // become a follower
	rf.votedFor = -1    // reset when convert to follower
	DPrintf("{%d} convert to follower, term:%d\n", rf.me, rf.currentTerm)

	rf.applyCh <- ApplyMsg{CommandValid: false, StateChange: true}
}

func (rf *Raft) convertToCandidate() {
	rf.state = Candidate // become candidate
	rf.currentTerm++     // increment current term
	rf.votedFor = rf.me  // vote for self
	rf.votesCount = 1    // vote for self
	DPrintf("{%d} convert to candidate, term:%d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader                          // become a leader
	rf.nextIndexs = make([]int, len(rf.peers)) // initialized to leader last log index+1
	rf.matchIndex = make([]int, len(rf.peers)) // initialized to 0
	lastLogIndex := rf.lastIncludedIndex + len(rf.logs)
	for i := range rf.nextIndexs {
		rf.nextIndexs[i] = lastLogIndex + 1
	}
	rf.matchIndex[rf.me] = lastLogIndex
	DPrintf("{%d} convert to leader, term:%d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) getStateHandle(param *stateParam) {
	param.term = rf.currentTerm
	param.isLeader = rf.state == Leader
	param.waitCh <- struct{}{}
}

func (rf *Raft) startHandle(param *startParam) {
	defer func() { param.waitCh <- struct{}{} }()

	param.term = rf.currentTerm
	param.isLeader = rf.state == Leader
	if !param.isLeader {
		param.index = -1
		return
	}

	param.index = rf.lastIncludedIndex + len(rf.logs) + 1

	rf.matchIndex[rf.me] = param.index

	rf.logs = append(rf.logs, &Entry{
		Command: param.command,
		Index:   param.index,
		Term:    param.term,
	})
	rf.persist() // log changed

	rf.sendHeartbeatImmediately()

	DPrintf("{%d} [Entries] , index:%d, term:%d\n", rf.me, param.index, param.term)
}

func (rf *Raft) requestVoteHandle(args *RequestVoteArgs) *RequestVoteReply {
	needPersist := false

	defer func() {
		// remains in follower state as long as it receives valid RPCs from a leader or candidate
		if rf.state == Follower {
			rf.resetElection()
		}
		if needPersist {
			rf.persist()
		}
	}()

	// receive RequestVote RPC request from old term, reply false
	if args.Term < rf.currentTerm {
		DPrintf("Refusal to vote,{%d} Term:%d < {%d} currentTerm:%d\n",
			args.CandidateId, args.Term, rf.me, rf.currentTerm)
		return &RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
	}

	// if RPC request contains term T > currentTerm,
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollower()
		needPersist = true
	}

	// if server has already voted, reply false
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("Refusal to vote,{%d} has already voted to {%d}\n", rf.me, rf.votedFor)
		return &RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
	}

	lastLogIndex := rf.lastIncludedIndex + len(rf.logs)
	lastLogTerm := rf.lastIncludedTerm
	if len(rf.logs) > 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	// if candidate's last log term is smaller, reply false
	if args.LastLogTerm < lastLogTerm {
		DPrintf("candidate:{%d} lastLogTerm:%d server:{%d} lastLogTerm:%d\n",
			args.CandidateId, args.LastLogTerm, rf.me, lastLogTerm)
		return &RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
	}

	// if candidate's log is shorter, reply false
	if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		DPrintf("Refusal to vote,{%d} lastLogIndex:%d < {%d} lastLogIndex:%d\n",
			args.CandidateId, args.LastLogIndex, rf.me, lastLogIndex)
		return &RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
	}

	// remains in follower state as long as it receives valid RPCs from a leader or candidate
	rf.votedFor = args.CandidateId
	needPersist = true

	return &RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}
}

func (rf *Raft) appendEntriesHandle(args *AppendEntriesArgs) *AppendEntriesReply {
	needPersist := false

	defer func() {
		// remains in follower state as long as it receives valid RPCs from a leader or candidate
		if rf.state == Follower {
			rf.resetElection()
		}
		if needPersist {
			rf.persist()
		}
	}()

	// receive AppendEntries RPC request from old term, reply false
	if args.Term < rf.currentTerm {
		return &AppendEntriesReply{Term: rf.currentTerm, Success: false, PrevLogIndex: -1}
	}

	// if RPC request contains term T > currentTerm,
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollower()
		needPersist = true
	}

	// if AppendEntries received from new leader, convert to follower
	if rf.state == Candidate {
		rf.convertToFollower()
		needPersist = true
	}

	lastEntryIndex := args.PrevLogIndex + len(args.Entries)

	// if new entries already present in server's log, ignore it and reply true.
	if lastEntryIndex <= rf.commitIndex {
		return &AppendEntriesReply{Term: rf.currentTerm, Success: true, PrevLogIndex: -1}
	}

	// The following, lastEntryIndex > rf.commitIndex, entries contains new log

	firstLogIndex := rf.lastIncludedIndex + 1
	lastLogIndex := rf.lastIncludedIndex + len(rf.logs)

	// reply false, leader must decrease nextIndex and retry
	if args.PrevLogIndex > lastLogIndex {
		return &AppendEntriesReply{Term: rf.currentTerm, Success: false, PrevLogIndex: lastLogIndex}
	}

	// The following, args.PrevLogIndex <= lastLogIndex

	// if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// reply false, leader must decrease nextIndex and retry
	if 0 <= args.PrevLogIndex-firstLogIndex && args.PrevLogIndex-firstLogIndex < len(rf.logs) &&
		rf.logs[args.PrevLogIndex-firstLogIndex].Term != args.PrevLogTerm {
		index := args.PrevLogIndex - firstLogIndex
		conflictTerm := rf.logs[index].Term
		for index >= 0 && rf.logs[index].Term == conflictTerm {
			index--
		}
		newPrevLogIndex := rf.lastIncludedIndex + index + 1
		rf.logs = rf.logs[:newPrevLogIndex-rf.lastIncludedIndex]
		needPersist = true
		DPrintf("{%d}, Conflict start index: %d\n", rf.me, newPrevLogIndex)
		return &AppendEntriesReply{Term: rf.currentTerm, Success: false, PrevLogIndex: newPrevLogIndex}
	}

	if lastEntryIndex > lastLogIndex {
		lastLogTerm := rf.lastIncludedTerm
		if len(rf.logs) > 0 {
			lastLogTerm = rf.logs[len(rf.logs)-1].Term
		}
		if lastLogIndex == args.PrevLogIndex {
			rf.logs = append(rf.logs, args.Entries...)
		} else if lastLogIndex-(args.PrevLogIndex+1) < len(args.Entries) &&
			args.Entries[lastLogIndex-(args.PrevLogIndex+1)].Term == lastLogTerm {
			rf.logs = append(rf.logs, args.Entries[lastLogIndex-args.PrevLogIndex:]...)
		} else if args.PrevLogIndex >= rf.lastIncludedIndex {
			rf.logs = append(rf.logs[:args.PrevLogIndex-rf.lastIncludedIndex], args.Entries...)
		} else {
			rf.logs = append(rf.logs, args.Entries[rf.lastIncludedIndex-args.PrevLogIndex:]...)
		}
		needPersist = true
	} else if len(args.Entries) > 0 &&
		0 <= args.PrevLogIndex+1-firstLogIndex &&
		args.PrevLogIndex+1-firstLogIndex < len(rf.logs) &&
		args.Entries[0].Term != rf.logs[args.PrevLogIndex+1-firstLogIndex].Term {
		rf.logs = append(rf.logs[:args.PrevLogIndex+1-firstLogIndex], args.Entries...)
		needPersist = true
	}

	// if LeaderCommit > commitIndex , set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, lastEntryIndex)
	}

	if rf.commitIndex > rf.lastApplied {
		rf.sendApplyMessage()
	}

	return &AppendEntriesReply{Term: rf.currentTerm, Success: true, PrevLogIndex: -1}
}

func (rf *Raft) initiatesElection() {
	rf.resetElection()
	rf.convertToCandidate()
	args := rf.makeRequestVoteArgs()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVoteRPC(i, args)
	}
}

func (rf *Raft) makeRequestVoteArgs() *RequestVoteArgs {
	lastLogIndex, lastLogTerm := rf.lastIncludedIndex+len(rf.logs), rf.lastIncludedTerm
	if len(rf.logs) != 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}

type requestVoteReplyParam struct {
	args  *RequestVoteArgs
	reply *RequestVoteReply
}

func (rf *Raft) sendRequestVoteRPC(index int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	if rf.peers[index].Call("Raft.RequestVote", args, reply) {
		rf.requestVoteReplyCh <- &requestVoteReplyParam{args: args, reply: reply}
	}
}

func (rf *Raft) requestVoteReplyHandle(args *RequestVoteArgs, reply *RequestVoteReply) {
	// if reply from old terms, ignore it
	if args.Term != rf.currentTerm {
		return
	}

	// if candidate's term is out of date,
	// convert from candidate to follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term // update currentTerm
		rf.convertToFollower()
		rf.persist()
		rf.resetElection()
		return
	}

	// if vote granted, and current state is not leader, check votes count
	if reply.VoteGranted && rf.state != Leader {
		rf.votesCount++
		// if receives votes from majority of servers, convert from candidate to leader
		if rf.votesCount > len(rf.peers)>>1 {
			rf.stopElection()
			rf.convertToLeader()
			rf.sendHeartbeatImmediately()
		}
		return
	}
}

func (rf *Raft) broadcastAppendEntries() {
	if rf.state != Leader {
		return
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// current log doesn't contain old logs, send a snapshot
		if rf.nextIndexs[i] <= rf.lastIncludedIndex {
			args := rf.makeInstallSnapshotArgs(rf.lastIncludedIndex, rf.lastIncludedTerm, rf.persister.ReadSnapshot())
			go rf.installSnapshotRPC(i, args)
			rf.nextIndexs[i] = rf.lastIncludedIndex + 1
			continue
		}

		args := rf.makeAppendEntriesArgs(i)
		go rf.sendAppendEntriesRPC(i, args)
	}
}

func (rf *Raft) makeAppendEntriesArgs(index int) *AppendEntriesArgs {
	startIndex := rf.lastIncludedIndex + 1
	nextLogIndex := rf.nextIndexs[index]

	prevLogIndex := nextLogIndex - 1
	prevLogTerm := rf.lastIncludedTerm
	if prevLogIndex-startIndex >= 0 {
		prevLogTerm = rf.logs[prevLogIndex-startIndex].Term
	}
	entries := rf.logs[nextLogIndex-startIndex:]

	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

type appendEntriesReplyParam struct {
	index int // server's id
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
}

func (rf *Raft) sendAppendEntriesRPC(index int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if rf.peers[index].Call("Raft.AppendEntries", args, reply) {
		rf.appendEntriesReplyCh <- &appendEntriesReplyParam{index: index, args: args, reply: reply}
	}
}

func (rf *Raft) appendEntriesReplyHandle(index int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// if reply from old terms, ignore it
	if args.Term != rf.currentTerm {
		return
	}

	// if leader's term is out of date,
	// convert from leader to follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term // update currentTerm
		rf.convertToFollower()
		rf.persist()
		rf.resetElection()
		return
	}

	// if success, update matchIndex and nextIndex
	if reply.Success {
		indexOfLastNewEntry := args.PrevLogIndex + len(args.Entries)

		// update server's mathcIndex and nextIndex
		if indexOfLastNewEntry > rf.matchIndex[index] {
			rf.matchIndex[index] = indexOfLastNewEntry
			rf.nextIndexs[index] = rf.matchIndex[index] + 1
		}

		indexes := make([]int, len(rf.matchIndex))
		copy(indexes, rf.matchIndex)
		DPrintf("----matchIndex[]:%v----\n", indexes)
		sort.Ints(indexes)

		// get majorityMatchIndex
		majorityMatchIndex := indexes[(len(indexes)-1)>>1]
		DPrintf("----majorityMatchIndex:%v, commitIndex:%v----\n", majorityMatchIndex, rf.commitIndex)

		if majorityMatchIndex > rf.commitIndex {

			// only commit current term's log
			// if rf.logs[majorityMatchIndex-startIndex].Term != rf.currentTerm {
			// 	return
			// }

			// update commitIndex
			rf.commitIndex = majorityMatchIndex

			// broadcast new commit index
			rf.sendHeartbeatImmediately()
		}

		if rf.commitIndex > rf.lastApplied {
			rf.sendApplyMessage()
		}

		return
	}

	// if follower's doesn't contain an entry at prevLogIndex,
	// decrement nextIndex
	if reply.PrevLogIndex != -1 {
		rf.nextIndexs[index] = reply.PrevLogIndex + 1
	}
}

func (rf *Raft) sendApplyMessage() {
	for _, entry := range rf.logs[rf.lastApplied-rf.lastIncludedIndex : rf.commitIndex-rf.lastIncludedIndex] {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
			CommandTerm:  entry.Term,
			IsLeader:     rf.state == Leader,
		}
		DPrintf("{%d} APPLY index:%v term:%v command:%v\n", rf.me, entry.Index, entry.Term, entry.Command)
	}
	rf.lastApplied = rf.commitIndex
}

type logcompactionParam struct {
	lastIndex int
	lastTerm  int
	data      []byte
}

// generate a snapshot to compress raft's log
func (rf *Raft) LogCompaction(lastIndex, lastTerm int, data []byte) {
	param := &logcompactionParam{lastIndex: lastIndex, lastTerm: lastTerm, data: data}
	go func() { rf.logcompactionCh <- param }()
}

func (rf *Raft) LogCompactionHandle(param *logcompactionParam) {
	lastIncludedIndex, lastIncludedTerm, snapshotData := param.lastIndex, param.lastTerm, param.data

	if lastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	newLogs := []*Entry{}
	if len(rf.logs) > 0 && lastIncludedIndex < rf.lastIncludedIndex+len(rf.logs) {
		DPrintf("newLogs size %d\n", len(rf.logs)-(lastIncludedIndex-rf.lastIncludedIndex))
		newLogs = make([]*Entry, len(rf.logs)-(lastIncludedIndex-rf.lastIncludedIndex))
		copy(newLogs, rf.logs[lastIncludedIndex-rf.lastIncludedIndex:])
	}

	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.logs = newLogs

	rf.persister.SaveStateAndSnapshot(rf.generateRaftStateData(), snapshotData)

	go func() { rf.applyCh <- ApplyMsg{CommandValid: false} }() // notify snapshot is saved

	DPrintf("Server {%d} saved snapshot, lastIncludedIndex = %d\n", rf.me, rf.lastIncludedIndex)
	args := rf.makeInstallSnapshotArgs(lastIncludedIndex, lastIncludedTerm, snapshotData)
	rf.broadcastInstallSnapshot(args)
}

func (rf *Raft) generateRaftStateData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.lastIncludedIndex)
	_ = e.Encode(rf.lastIncludedTerm)
	_ = e.Encode(rf.logs)

	return w.Bytes()
}

func (rf *Raft) makeInstallSnapshotArgs(lastIndex, lastTerm int, data []byte) *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Data:              data,
	}
}

func (rf *Raft) broadcastInstallSnapshot(args *InstallSnapshotArgs) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.installSnapshotRPC(i, args)
	}
}

func (rf *Raft) installSnapshotRPC(index int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	if rf.peers[index].Call("Raft.InstallSnapshot", args, reply) {
		rf.installSnapshotReplyCh <- &installSnapshotReplyParam{index: index, args: args, reply: reply}
	}
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so followers can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	// Offset            int    // byte offset where chunk is positioned in the snapshot file
	// Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // current Term, for leader to update itself
}

type installSnapshotParam struct {
	args    *InstallSnapshotArgs
	replyCh chan *InstallSnapshotReply
}

// Invoked by leader to send chunks of a snapshot to a follwer.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	replyCh := make(chan *InstallSnapshotReply, 1)
	rf.installSnapshotCh <- &installSnapshotParam{args: args, replyCh: replyCh}

	out := <-replyCh

	reply.Term = out.Term
}

func (rf *Raft) installSnapshotHandle(args *InstallSnapshotArgs) *InstallSnapshotReply {
	defer func() {
		// remains in follower state as long as it receives valid RPCs from a leader or candidate
		if rf.state == Follower {
			rf.resetElection()
		}
	}()

	if args.Term < rf.currentTerm {
		return &InstallSnapshotReply{Term: rf.currentTerm}
	}

	rf.currentTerm = args.Term

	if rf.state != Follower {
		rf.convertToFollower()
	}

	// if current snapshot is newer than leader, return immediately
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return &InstallSnapshotReply{Term: rf.currentTerm}
	}

	// discard log entries
	newLogs := []*Entry{}
	if len(rf.logs) > 0 && args.LastIncludedIndex < rf.lastIncludedIndex+len(rf.logs) {
		newLogs = make([]*Entry, len(rf.logs)-(args.LastIncludedIndex-rf.lastIncludedIndex))
		copy(newLogs, rf.logs[args.LastIncludedIndex-rf.lastIncludedIndex:])
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.logs = newLogs

	rf.persister.SaveStateAndSnapshot(rf.generateRaftStateData(), args.Data)
	DPrintf("Server {%d} saved snapshot, lastIncludedIndex = %d\n", rf.me, rf.lastIncludedIndex)

	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied = rf.commitIndex
	}

	rf.sendSnapshotMsg(args.Data)

	return &InstallSnapshotReply{Term: rf.currentTerm}
}

func (rf *Raft) sendSnapshotMsg(data []byte) {
	rf.applyCh <- ApplyMsg{
		CommandValid: false,
		Snapshot:     data,
	}
}

type installSnapshotReplyParam struct {
	index int
	args  *InstallSnapshotArgs
	reply *InstallSnapshotReply
}

func (rf *Raft) installSnapshotReplyHandle(index int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// if reply from old terms, ignore it
	if args.Term != rf.currentTerm {
		return
	}

	// if leader's term is out of date,
	// convert from leader to follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term // update currentTerm
		rf.convertToFollower()
		rf.persist()
		rf.resetElection()
		return
	}

	if args.LastIncludedIndex >= rf.matchIndex[index] {
		rf.matchIndex[index] = args.LastIncludedIndex
		rf.nextIndexs[index] = args.LastIncludedIndex + 1
	}
}
