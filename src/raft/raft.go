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

import "math/rand"
import "time"

import "bytes"
import "../labgob"

import "fmt"

type State int

const (
	Follower = 1
	Candidate = 2
	Leader = 3
)

type Log struct {
	Command interface{}
	Index int
	Term int
}


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
	// state a Raft server must maintain
	state int
	currentTerm int
	logs []Log
	getVotes int
	voteCandidate int
	electionTimeout int
	currentTime int
	applyCh chan ApplyMsg

	// for leader
	nextIndex []int
	heartbeatInterval int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logs)
	e.Encode(rf.voteCandidate)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	var currentTerm int
	var logs []Log
	var voteCandidate int

	if d.Decode(&currentTerm) != nil {
		fmt.Print("Decode failed: currentTerm")
	} else {
		rf.currentTerm = currentTerm
	}

	if d.Decode(&logs) != nil {
		fmt.Print("Decode failed: logs")
	} else {
		rf.logs = logs
	}

	if d.Decode(&voteCandidate) != nil {
		fmt.Print("Decode failed: voteCandidate")
	} else {
		rf.voteCandidate = voteCandidate
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CurrentTerm int
	Logs []Log
	ID int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	ReturnValue int
	CurrentTerm int
}

//
// example RequestVote RPC handler.
//
// rf: callee, requestVoteArgs: caller
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("Term %v: server %v is now requesting vote from server %v with Term %v...\n", args.RequestCandidateTerm, args.RequestCandidateID, rf.me, rf.currentTerm)
	// Your code here (2A, 2B).
	if args.CurrentTerm < rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		reply.ReturnValue = -1
		return
	} else if rf.state == Leader && rf.currentTerm == args.CurrentTerm {
		reply.CurrentTerm = -1
		reply.ReturnValue = -4
	} else if len(args.Logs) > 0 && len(rf.logs) > 0 && args.Logs[len(args.Logs) - 1].Term < rf.logs[len(rf.logs) - 1].Term {
		// not so update
		reply.CurrentTerm = -1
		reply.ReturnValue = -2
		return
	} else if len(args.Logs) > 0 && len(rf.logs) > 0 && args.Logs[len(args.Logs) - 1].Term == rf.logs[len(rf.logs) - 1].Term && len(args.Logs) < len(rf.logs) {
		// not so update
		reply.CurrentTerm = -1
		reply.ReturnValue = -2
		return
	} else if rf.voteCandidate != args.ID && rf.voteCandidate != -1 {
		reply.CurrentTerm = -1
		reply.ReturnValue = -3
		return
	} else {
		rf.voteCandidate = args.ID
		reply.ReturnValue = 1
		return
	}
}

type AppendEntriesArgs struct {
	CurrentTerm int
	Logs []Log
	ID int
	NextIndex int
}

type AppendEntriesReply struct {
	ReturnValue int
	CurrentTerm int
	NextIndex int
}

// rf: callee args: leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.CurrentTerm = -1

	if args.CurrentTerm < rf.currentTerm {
		reply.ReturnValue = -1
		reply.CurrentTerm = rf.currentTerm
	} else {
		// rf.mu.Lock()
		rf.currentTime = time.Now().Nanosecond()
		if rf.currentTerm < args.CurrentTerm {
			rf.currentTerm = args.CurrentTerm
		}
		if rf.state == Candidate {
			rf.state = Follower
		}

		if args.NextIndex > 0 && args.NextIndex <= len(rf.logs) {
			if rf.logs[args.NextIndex - 1] != args.Logs[args.NextIndex - 1] {
				reply.NextIndex = args.NextIndex - 1
				reply.ReturnValue = -1
			} else {
				rf.logs = rf.logs[:args.NextIndex]
				for i, log := range args.Logs {
					if i >= args.NextIndex {
						rf.logs = append(rf.logs, log)
					}
				}
				reply.ReturnValue = 1
			}
		} else if args.NextIndex > len(rf.logs) {
			args.NextIndex = len(rf.logs)
			for i, log := range args.Logs {
				if i >= args.NextIndex {
					rf.logs = append(rf.logs, log)
				}
			}
			reply.ReturnValue = 1
		} else {
			// args.NextIndex == 0
			rf.logs = []Log{}
			if len(args.Logs) > 0 {
				rf.logs = append(rf.logs, args.Logs[0])
			}
			reply.ReturnValue = 1
		}
		//rf.mu.Unlock()
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

	var isLeader bool
	if rf.currentState == Leader {
		isLeader = true
	} else {
		isLeader = false
	}

	// Your code here (2B).

	isLeader = (rf.state == Leader)
	term = rf.currentTerm
	index = len(rf.logs)

	if isLeader == false {
		return index, term, isLeader
	} else {
		go rf.commit(Log {
			command,
			index,
			term,
		})
		return index, term, isLeader
	}
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
			PrevLogTerm:       rf.logs[len(rf.logs)-1].LogTerm,
			Entries:           rf.logs,
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

func (rf *Raft) commit(log Log) bool {
	for {
		rf.logs = append(rf.logs, log)
		cnt := 0
		for i, _ := range rf.peers {
			// important!!!
			if i == rf.me {
				continue
			}
			args := AppendEntriesArgs {
				rf.currentTerm,
				rf.logs,
				rf.me,
				rf.nextIndex[i],
			}
			reply := AppendEntriesReply {}
			ok := rf.sendAppendEntries(i, &args, &reply)
			if ok == false {
				continue
			} else if reply.ReturnValue == -1 {
				if reply.CurrentTerm != -1 {
					if cnt > len(rf.peers) / 2 {
						// 
					}
				}
				rf.nextIndex[i] = reply.NextIndex
				continue
			} else {
				cnt++
			}
		}
		if cnt > len(rf.peers) / 2 {
			applyMsg := ApplyMsg {
				true, 
				log.Command,
				// to be revised
				len(rf.logs),
			}
			rf.applyCh <- applyMsg
		}
	}
}

// daemon thread
func (rf *Raft) run() {
	rf.currentTime = time.Now().Nanosecond()
	for {
		time.Sleep(1e6)
		if rf.state == Follower && time.Now().Nanosecond() - rf.currentTime >= rf.electionTimeout * 1e6 {
			rf.elect()
		}
		if rf.state == Leader && time.Now().Nanosecond() - rf.currentTime >= rf.heartbeatInterval * 1e6 {
			for i, _ := range rf.peers {
				// important!!!
				if i == rf.me {
					continue
				}
				args := AppendEntriesArgs {
					rf.currentTerm, 
					rf.logs,
					rf.me,
					rf.nextIndex[i],
				}
				reply := AppendEntriesReply {}
				ok := rf.sendAppendEntries(i, &args, &reply)
				
				if ok == false {
					continue
				}
				if reply.ReturnValue == -1 && reply.CurrentTerm > rf.currentTerm {
					rf.mu.Lock()
					rf.state = Follower
					rf.currentTerm = reply.CurrentTerm
					rf.mu.Unlock()
					break
				}

				if reply.ReturnValue == -1 {
					rf.nextIndex[i] = reply.NextIndex
				} else {
					rf.nextIndex[i] = len(rf.logs)
				}
			}
		}
	}
}

func (rf *Raft) elect() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = Candidate
	rf.getVotes = 1
	rf.voteCandidate = rf.me
	rf.mu.Unlock()

	currentTime := time.Now().Nanosecond()

	for {
		if time.Now().Nanosecond() - currentTime >= rf.electionTimeout * 1e6 {
			rf.mu.Lock()
			rf.state = Follower
			rf.voteCandidate = -1
			rf.getVotes = 0
			rf.mu.Unlock()
			return
		}

		for i, _ := range rf.peers {
			// important!!!
			if i == rf.me {
				continue
			}

			args := RequestVoteArgs {
				rf.currentTerm,
				rf.logs,
				rf.me,
			}
			reply := RequestVoteReply {}
			ok := rf.sendRequestVote(i, &args, &reply)
	
			if ok == false {
				continue
			}
			
			if reply.ReturnValue == 1 {
				rf.getVotes++
			} else if reply.CurrentTerm != -1 {
				rf.mu.Lock()
				rf.state = Follower
				rf.voteCandidate = -1
				rf.getVotes = 0
				rf.currentTerm = reply.CurrentTerm
				rf.mu.Unlock()
				return
			} else if reply.ReturnValue == -4 {
				rf.mu.Lock()
				rf.state = Follower
				rf.voteCandidate = -1
				rf.getVotes = 0
				rf.mu.Unlock()
				return
			}

			if rf.getVotes > len(rf.peers) / 2 {
				rf.mu.Lock()
				rf.state = Leader
				rf.mu.Unlock()
				return
			}
		}

		if rf.state != Candidate {
			return
		}
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
	rf.mu.Lock()
	rf.logs = []Log{}
	rf.currentTerm = 0
	rf.voteCandidate = -1
	rf.state = Follower
	rf.nextIndex = []int{0, 0, 0, 0, 0}
	rf.getVotes = 0
	rf.heartbeatInterval = 100 // ms
	rf.applyCh = applyCh
	rf.mu.Unlock()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.electionTimeout = r.Intn(200) + 300 //ms

	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())

	go rf.run()

	return rf
}

func (rf *Raft) run() {
	rf.lastHeartbeatTime = Max(rf.lastHeartbeatTime, time.Now())
	for {
		currentTime := time.Now()
		// timeout
		if rf.currentState == Follower && int(currentTime.Sub(rf.lastHeartbeatTime).Milliseconds()) > rf.electionTimeout {
			fmt.Println(rf.me, int(currentTime.Sub(rf.lastHeartbeatTime).Milliseconds()), rf.electionTimeout)
			rf.lastHeartbeatTime = time.Now()
			rf.elect()
			fmt.Printf("Term %v: server %v ended election with state %v\n", rf.currentTerm, rf.me, rf.currentState)
			rf.lastHeartbeatTime = time.Now()
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
				CommandIndex: i}
		}
		rf.lastApplied = rf.commitIndex
	}
}

func (rf *Raft) elect() {
	fmt.Printf("Term %v: server %v begins election...\n", rf.currentTerm + 1, rf.me)
	rf.currentState = Candidate
	rf.currentVotes = 1
	rf.votedFor = rf.me
	rf.currentTerm++
	for rf.currentState == Candidate && int(time.Now().Sub(rf.lastHeartbeatTime).Milliseconds()) < rf.electionTimeout {
		failedVote := 0
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
				LastLogIndex:         len(rf.logs) - 1,
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
				rf.currentVotes++
				fmt.Printf("server %v now has %v votes\n", rf.me, rf.currentVotes)
				if rf.currentVotes > len(rf.peers)/2 {
					rf.currentState = Leader
					rf.votedFor = -1
					return
				}
			} else {
				if reply.PeerTerm > rf.currentTerm {
					fmt.Printf("server %v has term %v but server %v has term %v, updating...\n", rf.me, rf.currentTerm, i, reply.PeerTerm)
					rf.currentState = Follower
					rf.votedFor = -1
					rf.currentTerm = reply.PeerTerm
					return
				}
				failedVote++
				if failedVote >= len(rf.peers)/2 {
					rf.currentState = Follower
					rf.votedFor = -1
					return
				}
			}
		}
	}
	rf.votedFor = -1
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
			Entries:           rf.logs,
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
		rf.sendHeartbeat()

		rf.mu.Lock()
		rf.canCommit()
		rf.applyCommit()
		rf.mu.Unlock()

		go rf.updateCommit()

		time.Sleep(1e8)
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
			Entries:           rf.logs,
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
