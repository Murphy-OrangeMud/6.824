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
	"sync"
	"sync/atomic"
	"time"
	"fmt"
	"../labrpc"
)

// import "fmt"

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

type Log struct {
	LogTerm   int
	Index     int
	Command   interface{}
	Committed bool
}

const (
	Follower  = 1
	Candidate = 2
	Leader    = 3
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
	currentTerm       int
	votedFor          int
	currentState      int
	currentVotes      int
	electionTimeout   int //ms
	lastHeartbeatTime time.Time
	currentLeaderID   int

	logs []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	ApplyCh chan ApplyMsg
}

// determine if rf is at least the same or more up to date than rf2
func (rf *Raft) MoreUpdate(rf2 *Raft) bool {
	if rf.logs[len(rf.logs)-1].LogTerm > rf2.logs[len(rf2.logs)-1].LogTerm {
		return true
	} else if rf.logs[len(rf.logs)-1].LogTerm == rf2.logs[len(rf2.logs)-1].LogTerm {
		if len(rf.logs) >= len(rf2.logs) {
			return true
		}
	}
	return false
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	
	if rf.currentState == Leader {
		isleader = true
	} else {
		isleader = false
	}

	rf.mu.Unlock()

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
	RequestCandidateTerm int
	RequestCandidateID   int
	LastLogIndex         int
	LastLogTerm          int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	PeerTerm    int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// fmt.Printf("server %v is now requesting vote from server %v\n", args.RequestCandidateID, rf.me)
	// Your code here (2A, 2B).
	if args.RequestCandidateTerm < rf.currentTerm {
		reply.PeerTerm = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if rf.votedFor == -1 || rf.votedFor == args.RequestCandidateID {
			if (len(rf.logs) == 0 && args.LastLogIndex + 1 == 0) || 
				rf.logs[len(rf.logs)-1].LogTerm > args.LastLogTerm ||
				(rf.logs[len(rf.logs)-1].LogTerm == args.LastLogTerm &&
					len(rf.logs) >= args.LastLogIndex+1) {
				reply.VoteGranted = true
				rf.votedFor = args.RequestCandidateID
				/*
				if (rf.votedFor != -1) {
					fmt.Printf("server %v voted for server %v\n", rf.me, rf.votedFor)
				}
				*/
				if args.RequestCandidateTerm > rf.currentTerm {
					rf.currentState = Follower
					rf.currentTerm = args.RequestCandidateTerm
				}
			}
		}
	}
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


type AppendEntriesArgs struct {
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	entries           []Log
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// TODO: to be revised for multi-thread follower
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else if args.PrevLogIndex == -1 {
		// heartbeat
		reply.Success = true
		reply.Term = rf.currentTerm
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
		}
		rf.currentLeaderID = args.LeaderID
		if rf.currentState > Follower {
			rf.currentState = Follower
		}
	} else if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].LogTerm != args.PrevLogTerm {
		rf.logs = rf.logs[:args.PrevLogIndex]
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		rf.logs = append(rf.logs, args.entries[args.PrevLogIndex + 1])
		reply.Success = true
		rf.lastHeartbeatTime = time.Now()
	}
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommitIndex, len(rf.logs)-1)
	}
}

func (rf *Raft) sendAppendEntries(serverID int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[serverID].Call("Raft.AppendEntries", args, reply)
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
	
	var isLeader bool
	if rf.currentState == Leader {
		isLeader = true
	} else {
		isLeader = false
	}

	// Your code here (2B).

	if isLeader {
		fmt.Printf("Yeah, I'm leader")
		term = rf.currentTerm
		index = len(rf.logs)

		go rf.commitCommand(command)
	}

	// not guaranteed to be committed
	return index, term, isLeader
}


func (rf *Raft) canCommit() {
	for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {
		successCnt := 1
		if rf.logs[i].LogTerm != rf.currentTerm {
			continue
		}
		for j, _ := range rf.peers {
			if rf.matchIndex[j] >= i {
				successCnt++
			}
		}
		if successCnt > len(rf.peers)/2 {
			rf.commitIndex = i
			break
		}
	}
}

func (rf *Raft) commitCommand(command interface{}) {
	rf.mu.Lock()
	rf.logs = append(rf.logs, Log{
		LogTerm:   rf.currentTerm,
		Index:     len(rf.logs),
		Command:   command,
		Committed: false})
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		rf.canCommit()
		rf.mu.Unlock()

		args := AppendEntriesArgs{
			Term:              rf.currentTerm,
			LeaderID:          rf.me,
			PrevLogIndex:      len(rf.logs) - 1,
			PrevLogTerm:       rf.logs[len(rf.logs) - 1].LogTerm,
			entries:           rf.logs,
			LeaderCommitIndex: rf.commitIndex,
		}
		reply := AppendEntriesReply{
			Term:    -1,
			Success: false,
		}

		for {
			ok := rf.sendAppendEntries(rf.me, &args, &reply)
			if ok {
				if reply.Success {
					rf.mu.Lock()
					rf.nextIndex[i] = args.PrevLogIndex + 2
					rf.matchIndex[i] = args.PrevLogIndex + 1
					rf.mu.Unlock()
					break
				} else {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						// definitely not committed
						rf.currentState = Follower
						rf.currentTerm = reply.Term
						rf.mu.Unlock()
						break
					} else {
						args.PrevLogIndex--
						args.PrevLogTerm = rf.logs[args.PrevLogIndex].LogTerm
						args.LeaderCommitIndex = rf.commitIndex
						rf.mu.Unlock()
						continue
					}
				}
			} else {
				// loop indefinitely
			}
		}
	}
}

func (rf *Raft) tryReplicate(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	for rf.sendAppendEntries(i, args, reply) == false {
	}
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
	rf.electionTimeout = rand.Int()%300 + 300
	rf.currentTerm = 0
	rf.currentState = Follower
	rf.votedFor = -1
	rf.currentVotes = 0
	rf.currentLeaderID = -1

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.matchIndex = []int{}
	rf.nextIndex = []int{}

	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex = append(rf.matchIndex, -1)
		rf.nextIndex = append(rf.nextIndex, 0)
	}

	rf.logs = []Log{}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}

func (rf *Raft) run() {
	rf.lastHeartbeatTime = Max(rf.lastHeartbeatTime, time.Now())
	for {
		currentTime := time.Now()
		// timeout
		if rf.currentState == Follower && int(currentTime.Sub(rf.lastHeartbeatTime).Milliseconds()) > rf.electionTimeout {
			rf.lastHeartbeatTime = time.Now()
			rf.elect()
			fmt.Printf("Term %v: server %v ended election with state %v\n", rf.currentTerm, rf.me, rf.currentState)
			if rf.currentState == Leader {
				rf.leader()
			}
		}
		rf.applyCommit()
	}
}

func (rf *Raft) applyCommit() {
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.ApplyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,}
		}
		rf.lastApplied = rf.commitIndex
	}
}


func (rf *Raft) elect() {
	fmt.Printf("server %v begins election...\n", rf.me)
	rf.currentState = Candidate
	rf.currentVotes = 1
	rf.currentTerm++
	for rf.currentState == Candidate && int(time.Now().Sub(rf.lastHeartbeatTime).Milliseconds()) < rf.electionTimeout {
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			reply := RequestVoteReply{
				PeerTerm:    -1,
				VoteGranted: false,
			}

			args := RequestVoteArgs{
				RequestCandidateTerm: rf.currentTerm,
				RequestCandidateID:   rf.me,
				LastLogIndex: len(rf.logs) - 1, 
			}
			if len(rf.logs) == 0 {
				args.LastLogTerm = -1
			} else {
				args.LastLogTerm = rf.logs[args.LastLogIndex].LogTerm
			}

			if rf.sendRequestVote(i, &args, &reply) == false {
				continue // to be revised in fault tolerance
			}

			if reply.VoteGranted == true {
				fmt.Printf("server %v now has %v votes\n", rf.me, rf.currentVotes)
				rf.currentVotes++
				if rf.currentVotes > len(rf.peers)/2 {
					rf.currentState = Leader
					return
				}
			} else {
				if reply.PeerTerm > rf.currentTerm {
					rf.currentState = Follower
					rf.currentTerm = reply.PeerTerm
					return
				}
			}
		}
	}
	rf.currentState = Follower
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	if rf.currentState != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		args := AppendEntriesArgs{
			Term:              rf.currentTerm, // here we need to lock
			LeaderID:          rf.me,
			PrevLogIndex:      -1,
			PrevLogTerm:       -1,
			entries:           rf.logs,
			LeaderCommitIndex: rf.commitIndex,
		}
		rf.mu.Unlock()
		reply := AppendEntriesReply{
			Term:    -1,
			Success: false,
		}

		if rf.sendAppendEntries(i, &args, &reply) == true {
			if reply.Success == true {
				continue
			} else {
				if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.currentState = Follower
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
					return
				}
			}
		}
	}
}

func (rf *Raft) leader() {
	rf.mu.Lock()
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}
	for i, _ := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
	for rf.currentState == Leader {
		// heartbeats
		go rf.sendHeartbeat()

		rf.mu.Lock()
		rf.canCommit()
		rf.applyCommit()
		rf.mu.Unlock()

		go rf.updateCommit()

		time.Sleep(2e8)
	}
	rf.GetState()
}

func (rf *Raft) updateCommit() {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
	
		rf.mu.Lock()
		if rf.nextIndex[i] >= len(rf.logs) {
			rf.mu.Unlock()
			continue
		}
		
		args := AppendEntriesArgs{
			Term:              rf.currentTerm,
			LeaderID:          rf.me,
			PrevLogIndex:      rf.nextIndex[i] - 1,
			PrevLogTerm:       rf.logs[rf.nextIndex[i]-1].LogTerm,
			entries:           rf.logs,
			LeaderCommitIndex: rf.commitIndex,
		}
		rf.mu.Unlock()

		reply := AppendEntriesReply{
			Term:    -1,
			Success: false,
		}

		for {
			if rf.sendAppendEntries(i, &args, &reply) == true {
				if reply.Success {
					rf.mu.Lock()
					rf.nextIndex[i] = args.PrevLogIndex + 2
					rf.matchIndex[i] = args.PrevLogIndex + 1
					rf.mu.Unlock()
				} else {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentState = Follower
						rf.currentTerm = reply.Term
					} else {
						rf.nextIndex[i] = args.PrevLogIndex
						args.PrevLogIndex--
						args.PrevLogTerm = rf.logs[args.PrevLogIndex].LogTerm
						args.LeaderCommitIndex = rf.commitIndex
						continue
					}
					rf.mu.Unlock()
				}
			} else {
				// fault tolerance
			}
		}
	}
}

func Max(t1 time.Time, t2 time.Time) time.Time {
	if t1.Unix() > t2.Unix() {
		return t1
	}
	return t2
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
