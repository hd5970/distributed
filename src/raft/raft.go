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
	"sync/atomic"
	"math/rand"
)

const (
	HeartbeatTimeout = 2 * 100
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

type RpcLogEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type RpcLogEntriesReply struct {
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
	isLeader          bool
	leader            int
	leaderPack        leaderPack
	lastHeartbeatTime int64
}

type leaderPack struct {
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	return term, isLeader
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < reply.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == 0 {
		if args.LastLogIndex < rf.log[len(rf.log)-1].Index {
			reply.VoteGranted = false
			return
		}
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
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

func (rf *Raft) AppendEntries(request *RpcLogEntriesRequest, reply *RpcLogEntriesReply) {
	// todo refresh election time
	rf.updateOnAppendEntries(request.Term)
	reply.Term = rf.currentTerm
	if request.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if rf.log[len(rf.log)-1].Term != request.PrevLogTerm || rf.log[len(rf.log)-1].Index != request.PrevLogIndex {
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
	length := len(rf.log)
	rf.log[length-1:length-1+len(request.Entries)] = request.Entries
	if request.LeaderCommit > rf.commitIndex {
		if request.LeaderCommit > request.Entries[0].Index {
			rf.commitIndex = request.Entries[0].Index
		} else {
			rf.commitIndex = request.LeaderCommit
		}
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, request *RpcLogEntriesRequest, reply *RpcLogEntriesReply) bool {
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
	return time.Now().UnixNano()-rf.lastHeartbeatTime > int64(time.Millisecond/time.Nanosecond*HeartbeatTimeout)
}

func (rf *Raft) updateOnAppendEntries(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartbeatTime = time.Now().UnixNano()
	if rf.role == Candidate && rf.currentTerm <= term {
		rf.role =
	}

}

func (rf *Raft) startElection() {
	// gonna start an election
	if rf.role != Candidate {
		return
	}
	rf.currentTerm ++
	rf.votedFor = rf.me
	var ballotCount int32
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(rf.peers) - 1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func() {
			args := RequestVoteArgs{
				CandidateId:  rf.me,
				LastLogTerm:  rf.currentTerm - 1,
				LastLogIndex: rf.log[len(rf.log)-1].Index,
				Term:         rf.currentTerm }
			reply := RequestVoteReply{}
			ret := rf.sendRequestVote(i, &args, &reply)
			if !ret {
				log.Println("Invalid reponse from server:", i)
				return
			}
			if reply.Term > rf.currentTerm {
				log.Printf("Backward term, current:%d reply:%d\n", rf.currentTerm, reply.Term)
				return
			}
			if reply.VoteGranted {
				atomic.AddInt32(&ballotCount, 1)
			}
			waitGroup.Done()
		}()

	}
	waitGroup.Wait()
	if ballotCount > int32(len(rf.peers)/2) {
		log.Println("Become the leader")
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.isLeader = true
		rf.role = Leader
		return
	}
	random := rand.Intn(300) + 300
	time.Sleep(time.Millisecond * time.Duration(random))
	rf.startElection()
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
	go func() {
		heartbeatTimeoutTicker := time.NewTicker(time.Millisecond * HeartbeatTimeout / 2)
		for rf.isLeader == false {
			select {
			case <-heartbeatTimeoutTicker.C:
				needElection := rf.needElection()
				if needElection {
					log.Println("Gonna start an election")
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case /*msg := */ <-applyCh:
			case <-time.After(time.Microsecond * HeartbeatTimeout):


			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) findLastLogIndex() (max int) {
	for i := len(rf.log); i > 0; i-- {
		if rf.log[i-1].Index > max {
			max = rf.log[i-1].Index
		}
	}
	// in case of nil log
	if max < rf.commitIndex {
		max = rf.commitIndex
	}
	return
}
