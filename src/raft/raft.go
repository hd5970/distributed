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

import "sync"
import (
	"labrpc"
	"time"
	"log"
	"math/rand"
	"fmt"
	"sync/atomic"
)

const (
	HeartbeatTimeout = 2 * 100
	HeartbeatCycle   = 150
)

const (
	Leader    = 2
	Candidate = 1
	Follower  = 0
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type AppendLogEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendLogEntriesReply struct {
	Term    int
	Success bool
}

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int
	votedFor          int
	log               []LogEntry
	commitIndex       int
	lastApplied       int
	role              int
	leader            int
	leaderPack        leaderPack
	lastHeartbeatTime int64
}

type leaderPack struct {
	nextIndex  []int
	matchIndex []int
}

func randomElectionTimeout() time.Duration {
	random := rand.Intn(150) + 150
	return time.Duration(random) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.role == Leader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	LastLogTerm  int
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

func (rf *Raft) getLastLogIndex() int {
	var max int
	length := len(rf.log)
	if len(rf.log) > 0 {
		max = rf.log[length-1].Index
	}
	// in case of nil log
	if max < rf.commitIndex {
		max = rf.commitIndex
	}
	return max
}

func (rf *Raft) getLastLogTerm() int {
	length := len(rf.log)
	if length > 0 {
		return rf.log[len(rf.log)-1].Term
	}
	return rf.currentTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.voteDebug(args, reply)
	rf.lastHeartbeatTime = time.Now().UnixNano()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term == rf.currentTerm {
		if rf.role != Follower {
			reply.VoteGranted = false
			return
		}
	} else {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}
	if rf.votedFor == -1 {
		if args.LastLogIndex < rf.getLastLogIndex() {
			reply.VoteGranted = false
			return
		}
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.role = Follower
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) voteDebug(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Unlock()
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

func (rf *Raft) AppendEntries(request *AppendLogEntriesRequest, reply *AppendLogEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartbeatTime = time.Now().UnixNano()
	if rf.currentTerm < request.Term {
		log.Println("Fail from leader  im ", rf.me)
		rf.role = Follower
		rf.currentTerm = request.Term
	}
	reply.Term = rf.currentTerm

	if request.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if rf.getLastLogTerm() != request.PrevLogTerm || rf.getLastLogIndex() != request.PrevLogIndex {
		reply.Success = false
		return
	}

	// in case of heartbeat
	if !(len(request.Entries) > 0) {
		reply.Success = true
		return
	}

	var firstDiff = -1
	for i := len(rf.log) - 1; i >= 0; i-- {
		// ensure request.Entries is sorted
		if rf.log[i].Index == request.Entries[0].Index {
			if rf.log[i].Term != request.Entries[0].Term {
				firstDiff = i
			} else {
				break
			}
		} else if rf.log[i].Index < request.Entries[0].Index {
			break
		}
	}
	if firstDiff != -1 {
		rf.log = rf.log[:firstDiff]
	}
	rf.log = append(rf.log, request.Entries...)
	if request.LeaderCommit > rf.commitIndex {
		if request.LeaderCommit > request.Entries[0].Index {
			rf.commitIndex = request.Entries[0].Index
		} else {
			rf.commitIndex = request.LeaderCommit
		}
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, request *AppendLogEntriesRequest, reply *AppendLogEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", request, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) needElection() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == Leader {
		return false
	}
	delta := time.Duration(time.Now().UnixNano() - rf.lastHeartbeatTime)
	random := randomElectionTimeout()
	need := delta > random
	return need
}

func (rf *Raft) startElection() {
	// gonna start an election
	rf.mu.Lock()
	if rf.role != Candidate {
		return
	}
	fmt.Printf("Gonna start an election me:%d\n", rf.me)
	rf.currentTerm ++
	rf.votedFor = rf.me
	args := RequestVoteArgs{
		CandidateId:  rf.me,
		LastLogTerm:  rf.currentTerm - 1,
		LastLogIndex: rf.getLastLogIndex(),
		Term:         rf.currentTerm }
	rf.mu.Unlock()
	var ballotCount int32 = 1
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(rf.peers) - 1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			defer waitGroup.Done()

			reply := RequestVoteReply{}
			ret := rf.sendRequestVote(i, &args, &reply)
			if !ret {
				return
			}
			if reply.Term > rf.currentTerm {
				return
			}
			if reply.VoteGranted {
				atomic.AddInt32(&ballotCount, 1)
			}
		}(i)

	}

	var done bool
	go func() {
		for !done {
			if ballotCount > int32(len(rf.peers)/2) {
				if rf.role == Candidate {
					rf.role = Leader
					go rf.leadTheCluster()
				}
				return
			}
		}
	}()
	waitGroup.Wait()
	done = true
	if rf.role == Candidate {
		rf.votedFor = -1
		fmt.Printf("Start election failed me:%d\n", rf.me)
		time.Sleep(randomElectionTimeout())
		rf.startElection()
	}
}

func (rf *Raft) leadTheCluster() {
	fmt.Println("Now i am gonna lead the cluster, my id:", rf.me)
	rf.AppendLogEntriesToFollowers()
	ticker := time.NewTicker(time.Millisecond * HeartbeatCycle)
	for rf.role == Leader {
		select {
		case <-ticker.C:
			rf.AppendLogEntriesToFollowers()
		}

	}
}

func (rf *Raft) AppendLogEntriesToFollowers() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			request := AppendLogEntriesRequest{Term: rf.currentTerm, LeaderId: rf.me}
			reply := AppendLogEntriesReply{}
			if rf.role == Leader {
				ret := rf.sendAppendEntries(i, &request, &reply)
				if rf.role != Leader {
					return
				}
				fmt.Printf("Role:%d Me:%d current term: %d reply server:%d rpc status:%t reply status: %t reply term:%d\n", rf.role, rf.me, rf.currentTerm, i, ret, reply.Success, reply.Term)
				if !reply.Success && ret {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						fmt.Printf("I am not the leader any more me:%d\n", rf.me)
						rf.role = Follower
					}
				}
			}
		}(i)

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
	rf.votedFor = -1
	rf.role = Follower
	rf.lastHeartbeatTime = time.Now().UnixNano()
	// Your initialization code here (2A, 2B, 2C).
	go func() {
		heartbeatTimeoutTicker := time.NewTicker(time.Millisecond * 10)
		for rf.role == Follower {
			select {
			case <-heartbeatTimeoutTicker.C:
				if rf.needElection() {
					rf.role = Candidate
					rf.startElection()
				}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
