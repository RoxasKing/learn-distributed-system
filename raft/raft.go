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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoxasKing/learn-distributed-system/labrpc"
)

// import "bytes"
// import "../labgob"

const (
	electionTimeoutL         = 350
	electionTimeoutR         = 700
	heartbeatTimeoutDuration = 50 * time.Millisecond
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

	applyCh chan ApplyMsg // send an ApplyMsg when commited new entry

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

	state            State           // server states: follower, candidate or leader(initialize to 0 as follower)
	electionTimeout  *time.Ticker    // election timeouts are chosen randomly from a fixed interval(150~300ms)
	heartbeatTimeout *time.Ticker    // keep followers from starting elections
	killCtx          context.Context // receive cancel signal
	killFunc         func()          // cancel all goroutines

	commitCh chan int
	startCh  chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.safeOperation(func() {
		term = rf.currentTerm
		isleader = rf.state == Leader
	})

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

	// if candidate's term is out of date, reply false
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if candidate's term is newer than server's, convert to follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		if rf.state != Follower {
			rf.convertToFollower()
			rf.resetElectionTimeoutDetection()
		}
	}

	// if server has already voted, reply false
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// get server's last log's index and term
	lastLogIndex, lastLogTerm := 0, 0
	if len(rf.logs) > 0 {
		lastLog := rf.logs[len(rf.logs)-1]
		lastLogIndex, lastLogTerm = lastLog.Index, lastLog.Term
	}

	// if candidate's log is out of date, reply false
	if lastLogTerm > args.LastLogTerm || lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

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

	// if leader's term is out of date, reply false
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if leader's term is newer than server's, update currentTerm, and convert to follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		if rf.state != Follower {
			rf.convertToFollower()
		}
	}

	rf.resetElectionTimeoutDetection()

	latestEntryIndex := args.PrevLogIndex + len(args.Entries)

	// if new entries already present in server's log, ignores and reply true.
	if rf.commitIndex >= latestEntryIndex {
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}

	lastLogIndex := 0
	if len(rf.logs) > 0 {
		lastLogIndex = rf.logs[len(rf.logs)-1].Index
	}

	// if lastLogIndex < prevLogIndex, reply false, leader must decrement nextIndex and retry
	if lastLogIndex < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if lastLogIndex > prevLogIndex, check if received duplicate entries
	if lastLogIndex >= args.PrevLogIndex {
		logIndex := len(rf.logs) - 1
		entryIndex := len(args.Entries) - 1
		if lastLogIndex > latestEntryIndex {
			for logIndex >= 0 && rf.logs[logIndex].Index > latestEntryIndex {
				logIndex--
			}
		} else {
			for entryIndex >= 0 && args.Entries[entryIndex].Index > lastLogIndex {
				entryIndex--
			}
		}
		if logIndex >= 0 && entryIndex >= 0 && rf.logs[logIndex].Term == args.Entries[entryIndex].Term {
			// remove duplicate entries
			if lastLogIndex < latestEntryIndex {
				rf.logs = append(rf.logs, args.Entries[entryIndex+1:]...)
			}
		} else {
			// delete conflict entries
			for logIndex >= 0 && rf.logs[logIndex].Index > args.PrevLogIndex {
				logIndex--
			}
			// if server's log dosen't contain an entry ait prevLogIndex,
			// reply false, leader must decrement nextIndex and retry
			if logIndex >= 0 && rf.logs[logIndex].Term != args.PrevLogTerm {
				rf.logs = rf.logs[:logIndex]
				reply.Term = rf.currentTerm
				reply.Success = false
				return
			}
			// rewrite new entries
			rf.logs = append(rf.logs[:logIndex+1], args.Entries...)
		}
	}

	// if commitIndex < leaderCommit , set commitIndex = min(leaderCommit, index of last new entry)
	if rf.commitIndex < args.LeaderCommit {
		newCommitIndex := Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		base := rf.logs[0].Index
		entries := make([]*Entry, newCommitIndex-rf.commitIndex)
		copy(entries, rf.logs[rf.commitIndex-base+1:newCommitIndex-base+1])
		go func() {
			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				DPrintf("server:%v index:%v term:%v\n", rf.me, entry.Index, entry.Term)
			}
		}()
		rf.commitIndex = newCommitIndex

		// TODO
		if rf.commitIndex > rf.lastApplied {
			rf.persist() // Updated on stable storage before responding to RPCs
			rf.lastApplied = rf.commitIndex
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
	// term := -1
	// isLeader := true

	// Your code here (2B).

	rf.startCh <- struct{}{}
	defer func() { <-rf.startCh }()

	term, isLeader := rf.GetState()

	if !isLeader || rf.killed() {
		return -1, term, isLeader
	}

	rf.safeOperation(func() {
		if len(rf.logs) > 0 {
			index = rf.logs[len(rf.logs)-1].Index + 1
		}
		if index < 0 {
			index = 1
		}
		entry := &Entry{
			Command: command,
			Index:   index,
			Term:    term,
		}
		rf.logs = append(rf.logs, entry)
	})

	success := make([]bool, len(rf.peers))

	sendAppendEntriesRPC := func() {
		rf.safeOperation(func() {
			for i := range rf.matchIndex {
				if !success[i] && rf.matchIndex[i] >= index {
					success[i] = true
				}
			}
		})
		for i := range rf.peers {
			if i == rf.me || success[i] {
				continue
			}
			go rf.sendAppendEntriesRPC(i)
		}
	}

	sendAppendEntriesRPC()

	failed := time.NewTicker(50 * time.Millisecond)
	retry := time.NewTicker(5 * time.Millisecond)

LOOP:
	for {
		select {
		case <-failed.C:
			break LOOP
		case <-retry.C:
			if rf.killed() {
				return -1, term, isLeader
			}
			term, isLeader = rf.GetState()
			if !isLeader {
				return -1, term, isLeader
			}
			sendAppendEntriesRPC()
		case latestCommit := <-rf.commitCh:
			if latestCommit >= index {
				DPrintf("commit index %d\n", index)
				break LOOP
			}
		}
	}

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

	rf.applyCh = applyCh

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1 // initial state is -1
	rf.logs = []*Entry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndexs = []int{}
	rf.matchIndex = []int{}

	rf.killCtx, rf.killFunc = context.WithCancel(context.Background())

	rf.electionTimeout = time.NewTicker(getRandomElectionTimeout())

	rf.heartbeatTimeout = time.NewTicker(heartbeatTimeoutDuration)
	rf.stopHeartbeatTimeoutDetection()

	go rf.broadcast()

	rf.commitCh = make(chan int, 1)
	rf.startCh = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) broadcast() {
	for {
		select {
		case <-rf.electionTimeout.C:
			// when timeout, convert to candicate, and start election
			rf.safeOperation(func() {
				rf.convertToCandidate()
			})
			go rf.broatcastRequestVoteRPC()
		case <-rf.heartbeatTimeout.C:
			// when convert to leader, send heartbeat RPC per 10ms
			go rf.broatcastAppendEntriesRPC()
		case <-rf.killCtx.Done():
			return
		}
	}
}

func (rf *Raft) broatcastRequestVoteRPC() {
	votesCount := 1 // vote for self
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVoteRPC(i, &votesCount)
	}
}

func (rf *Raft) broatcastAppendEntriesRPC() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesRPC(i)
	}
}

func (rf *Raft) sendRequestVoteRPC(index int, votesCount *int) {
	args := &RequestVoteArgs{}
	reply := &RequestVoteReply{}
	rf.safeOperation(func() {
		// if the current state has expired
		if rf.state != Candidate {
			return
		}

		lastLogIndex, lastLogTerm := 0, 0
		if len(rf.logs) != 0 {
			lastLog := rf.logs[len(rf.logs)-1]
			lastLogIndex = lastLog.Index
			lastLogTerm = lastLog.Term
		}

		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = lastLogIndex
		args.LastLogTerm = lastLogTerm
	})

	if !rf.peers[index].Call("Raft.RequestVote", args, reply) {
		return
	}

	rf.safeOperation(func() {
		// if reply's term is out of date, ignore it
		if reply.Term < rf.currentTerm {
			return
		}
		// if vote granted, check votes count
		if reply.VoteGranted {
			if rf.state != Leader {
				*votesCount++
				// if receives votes from majority of servers, convert from candidate to leader
				if *votesCount > len(rf.peers)>>1 {
					rf.convertToLeader()
					go rf.broatcastAppendEntriesRPC() // send heartbeat immediately
				}
			}
			return
		}
		// if server's term is out of date, convert from candidate to follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term // update current term
			if rf.state != Follower {
				rf.convertToFollower()
				rf.resetElectionTimeoutDetection()
			}
		}
	})
}

func (rf *Raft) sendAppendEntriesRPC(index int) {
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}
	rf.safeOperation(func() {
		// if the current state has expired
		if rf.state != Leader {
			return
		}

		nextLogIndex := rf.nextIndexs[index]
		prevLogIndex := nextLogIndex - 1
		prevLogTerm := 0
		entries := []*Entry{}
		if len(rf.logs) > 0 {
			offset := rf.logs[0].Index
			if prevLogIndex-offset >= 0 {
				prevLogTerm = rf.logs[prevLogIndex-offset].Term
			}
			entries = append(entries, rf.logs[nextLogIndex-offset:]...)
		}

		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = prevLogTerm
		args.Entries = entries
		args.LeaderCommit = rf.commitIndex
	})

	if !rf.peers[index].Call("Raft.AppendEntries", args, reply) {
		return
	}

	rf.safeOperation(func() {
		// if reply's term is out of date, or reply is success, ignore it
		if reply.Term < rf.currentTerm {
			return
		}
		// if success, update matchIndex and nextIndex
		if reply.Success {
			lastIndex := args.PrevLogIndex + len(args.Entries)
			// entries has already commited
			if lastIndex <= rf.matchIndex[index] {
				return
			}

			// update server's mathcIndex and nextIndex
			rf.matchIndex[index] = lastIndex
			rf.nextIndexs[index] = rf.matchIndex[index] + 1

			// get latest commit index
			indexes := make([]int, len(rf.matchIndex))
			copy(indexes, rf.matchIndex)
			sort.Ints(indexes)
			latestCommit := indexes[len(indexes)-len(indexes)>>1]

			// if must update commitIndex
			if latestCommit > rf.commitIndex {
				l, r := 0, 0
				if rf.commitIndex > 0 {
					base := rf.logs[0].Index
					l = rf.commitIndex - base + 1
				}
				r = l + latestCommit - rf.commitIndex
				entries := rf.logs[l:r]

				// update commitIndex
				rf.commitIndex = latestCommit

				go func() {
					rf.commitCh <- latestCommit
				}()

				// send applyCh
				go func() {
					for _, entry := range entries {
						rf.applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      entry.Command,
							CommandIndex: entry.Index,
						}
						DPrintf("server:%v index:%v term:%v\n", rf.me, entry.Index, entry.Term)
					}
				}()
			}
			return
		}
		// if leader's term is out of date,
		// convert from leader to follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term // update current term
			if rf.state != Follower {
				rf.convertToFollower()
				rf.resetElectionTimeoutDetection()
			}
			return
		}
		// if follower's log is out of date, or conflict with leader's,
		// and reply's term is same as the request's,
		// decrease this server's nextIndex
		if reply.Term == args.Term {
			rf.nextIndexs[index]--
		}
	})
}

func (rf *Raft) convertToFollower() {
	rf.state = Follower                // become a follower
	rf.votedFor = -1                   // reset when convert to follower
	rf.stopHeartbeatTimeoutDetection() // stop heartbeat detection if current state is leader
	DPrintf("server %d become follower, term is %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) convertToCandidate() {
	rf.state = Candidate // become candidate
	rf.currentTerm++     // increment current term
	rf.votedFor = rf.me  // vote for self
	DPrintf("server %d become candidate, term is %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader                          // become a leader
	rf.stopElectionTimeoutDetection()          // stop election timeout detection
	rf.resetHeartbeatTimeoutDetection()        // activate heartbeat detection
	rf.matchIndex = make([]int, len(rf.peers)) // initialized to 0
	rf.nextIndexs = make([]int, len(rf.peers)) // initialized to leader last log index+1
	lastLogIndex, logsLen := 0, len(rf.logs)
	if logsLen > 0 {
		lastLogIndex = rf.logs[logsLen-1].Index
	}
	for i := range rf.nextIndexs {
		rf.nextIndexs[i] = lastLogIndex + 1
	}
	DPrintf("server %d become leader, term is %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) resetElectionTimeoutDetection() {
	rf.electionTimeout.Reset(getRandomElectionTimeout())
}

func (rf *Raft) stopElectionTimeoutDetection() {
	rf.electionTimeout.Stop()
}

func (rf *Raft) resetHeartbeatTimeoutDetection() {
	rf.heartbeatTimeout.Reset(heartbeatTimeoutDuration)
}

func (rf *Raft) stopHeartbeatTimeoutDetection() {
	rf.heartbeatTimeout.Stop()
}

func getRandomElectionTimeout() time.Duration {
	rand.Seed(makeSeed())
	return time.Duration(electionTimeoutL+rand.Intn(electionTimeoutR-electionTimeoutL+1)) * time.Millisecond
}

func (rf *Raft) safeOperation(f func()) {
	rf.mu.Lock()
	f()
	rf.mu.Unlock()
}
