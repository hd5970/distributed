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
	"math/rand"
	"fmt"
	"log"
)

// import "bytes"
// import "encoding/gob"

const (
	// Role
	Leader    = 2
	Candidate = 1
	Follower  = 0
)

const (
	NotVoted            = -1
	HeartBeatCycle      = 500 // heart beat duration in milliseconds
	ElectionTimeoutBase = 800
	RecvChanBufferSize  = 200
	//NullInt             = -1
)

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

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type updateRoleOrTerm struct {
	role int
	term int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm int
	votedFor    int
	log         []LogEntry
	role        int
	memberCount int

	commitIndex     int
	lastApplied     int
	ifPrevRpcReturn []bool
	everLeader      bool // indicates that i am the leader prev time

	// leader should know
	nextIndex  []int
	matchIndex []int

	recvLogEventChan chan bool
	heartbeatChan    chan bool
	changeRoleChan   chan updateRoleOrTerm
	roleChangedChan  chan bool
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return 0, 0, false
	}
	index := rf.getLastLogIndex() + 1
	l := LogEntry{
		Index:   index,
		Term:    rf.currentTerm,
		Command: command}
	rf.log = append(rf.log, l)
	rf.recvLogEventChan <- true
	return index, int(rf.currentTerm), true
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

func (rf *Raft) GetState() (int, bool) {
	return int(rf.currentTerm), rf.role == Leader
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

func (rf *Raft) getRelativeLog(index int) LogEntry {
	// 如果rf.log被持久化了 那么rf.log这个slice里面的index就不是真正的log.index了,所以需要保持一个变量存储了被持久化的log的最大序号
	if len(rf.log) == 0 {
		return LogEntry{}
	}
	return rf.log[index]
}

func (rf *Raft) getRelativeLogEntries(from int) []LogEntry {
	if len(rf.log) == 0 {
		return []LogEntry{}
	}
	return rf.log[from:]
}

func (rf *Raft) getLastLogIndex() int {
	// this should run with lock
	var max int
	length := len(rf.log)
	if length > 0 {
		max = rf.log[length-1].Index
	}
	// in case of null log
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

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	return time.Duration(rand.Int63n(int64(HeartBeatCycle))+ElectionTimeoutBase) * time.Millisecond
}

//handler
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.debug(args.CandidateId, "recv vote from term:%d", args.Term)
	defer func() {
		rf.debug(args.CandidateId, "vote request from remote return vote for:%t", reply.VoteGranted)
	}()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.changeRoleChan <- updateRoleOrTerm{term: args.Term, role: Follower}
		time.Sleep(time.Millisecond * 20)
		rf.debug(args.CandidateId, "Found request vote term greater than me, term:%d", args.Term)
	}
	reply.Term = rf.currentTerm

	rf.heartbeatChan <- true
	if rf.votedFor == NotVoted {
		if rf.getLastLogIndex() <= args.LastLogIndex {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			return
		}
	}
	reply.VoteGranted = false
}

// handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.debug(args.LeaderId, "Append log leader term greater than me current term:%d args term:%d",
			rf.currentTerm, args.Term)
		rf.changeRoleChan <- updateRoleOrTerm{term: args.Term, role: Follower}
		time.Sleep(time.Millisecond * 20)
	} else {
		if rf.role == Candidate {
			rf.changeRoleChan <- updateRoleOrTerm{term: args.Term, role: Follower}
			time.Sleep(time.Millisecond * 20)
		}
	}
	reply.Term = rf.currentTerm
	rf.heartbeatChan <- true

	if len(args.Entries) == 0 {
		reply.Success = true
		return
	}

	if rf.log[rf.getLastLogIndex()].Term != args.PrevLogTerm {
		return
	}

	var firstDiff = -1
	for i := len(rf.log) - 1; i >= 0; i-- {
		// ensure request.Entries is sorted
		if rf.log[i].Index == args.Entries[0].Index {
			if rf.log[i].Term != args.Entries[0].Term {
				firstDiff = i
			} else {
				break
			}
		} else if rf.log[i].Index < args.Entries[0].Index {
			break
		}
	}
	if firstDiff != -1 {
		rf.log = rf.log[:firstDiff]
	}
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > args.Entries[0].Index {
			rf.commitIndex = args.Entries[0].Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	reply.Success = true

}

func (rf *Raft) startElection() {
	rf.debug(rf.me, "Gonna start an election")

	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm + 1,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm()}
	rf.mu.Unlock()
	rf.changeRoleChan <- updateRoleOrTerm{term: rf.currentTerm + 1, role: Candidate}
	time.Sleep(time.Millisecond * 20)
	rf.votedFor = rf.me
	rf.debug(rf.me, "launch a election term:%d ", rf.currentTerm)
	var ballotCount int = 1
	wait := sync.WaitGroup{}
	wait.Add(rf.memberCount - 1)
	var done = make(chan bool, rf.memberCount)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			defer wait.Done()
			reply := &RequestVoteReply{}
			rf.debug(rf.me, "send vote request to %d term:%d", i, args.Term)
			ret := rf.sendRequestVote(i, args, reply)
			rf.debug(rf.me,
				"vote request returns remote:%d rpc:%t reply term:%d vote:%t args.term:%d", i, ret,
				reply.Term, reply.VoteGranted, args.Term)
			if !ret {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.changeRoleChan <- updateRoleOrTerm{term: args.Term, role: Follower}
				time.Sleep(time.Millisecond * 20)
				return
			}
			if reply.VoteGranted {
				rf.mu.Lock()
				ballotCount ++
				if ballotCount > rf.memberCount/2 && rf.role == Candidate /*&& rf.currentTerm == args.Term*/ {
					rf.changeRoleChan <- updateRoleOrTerm{term: rf.currentTerm, role: Leader}
					time.Sleep(time.Millisecond * 20)
					done <- true
					rf.debug(rf.me, "win the election term:%d ballot:%d",
						rf.currentTerm, ballotCount)
				}
				rf.mu.Unlock()
			}
		}(i)
	}
	go func() {
		wait.Wait()
		done <- false
	}()
	ret := <-done
	if ret {
		return
	}
	if rf.role == Candidate {
		rf.debug(rf.me, "lose the election term:%d ballot:%d", rf.currentTerm, ballotCount)
		time.Sleep(rf.randomElectionTimeout())
	}
}

func (rf *Raft) lead() {
	select {
	case <-time.After(time.Millisecond * HeartBeatCycle):
		rf.broadcastLogEntries()
	case <-rf.recvLogEventChan:
		rf.broadcastLogEntries()
	}
}

func (rf *Raft) broadcastLogEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.matchIndex[i],
				LeaderCommit: rf.commitIndex,
				Entries:      rf.getRelativeLogEntries(rf.nextIndex[i]),
				PrevLogTerm:  rf.getRelativeLog(rf.matchIndex[i]).Term}
			rf.mu.Unlock()
			reply := &AppendEntriesReply{}
			if rf.role != Leader || rf.currentTerm != args.Term {
				return
			}
			rf.debug(rf.me, "Gonna send append entries to remote:%d current term:%d", i, rf.currentTerm)
			ret := rf.sendAppendEntries(i, args, reply)
			rf.debug(rf.me, "Append entries to remote:%d rpc: %t current term:%d ,reply.term:%d success:%t",
				i, ret, rf.currentTerm, reply.Term, reply.Success)
			if ret {
				if reply.Term > rf.currentTerm {
					rf.debug(rf.me,
						"found greater term from append log remote:%d, "+
							"current term:%d reply term:%d", i, rf.currentTerm, reply.Term)
					rf.changeRoleChan <- updateRoleOrTerm{role: Leader, term: reply.Term}
					time.Sleep(time.Millisecond * 20)
					return
				}
				if rf.role != Leader {
					return
				}
				if reply.Success {
					rf.nextIndex[i] += len(args.Entries)
					rf.matchIndex[i] = rf.nextIndex[i] - 1
				} else {
					rf.nextIndex[i] -= 1
				}
			}
		}(i)
	}
}

func (rf *Raft) loop() {
	for {
		if rf.role == Follower {
			select {
			case <-rf.heartbeatChan:
			case <-time.After(rf.randomElectionTimeout()):
				rf.changeRoleChan <- updateRoleOrTerm{term: rf.currentTerm + 1, role: Candidate }
				time.Sleep(time.Millisecond * 20)
			}
		} else if rf.role == Candidate {
			rf.startElection()
		} else if rf.role == Leader {
			if rf.everLeader {
				rf.lead()
			} else {
				rf.nextIndex = make([]int, rf.memberCount)
				rf.matchIndex = make([]int, rf.memberCount)
				rf.ifPrevRpcReturn = make([]bool, rf.memberCount)
				for i := range rf.matchIndex {
					rf.nextIndex[i] = rf.commitIndex + 1
					rf.ifPrevRpcReturn[i] = true
					rf.matchIndex[i] = rf.commitIndex
				}
				rf.everLeader = true
				rf.broadcastLogEntries()
			}
		}
	}
}

func (rf *Raft) updateRoleOrTerm() {
	for {
		pack := <-rf.changeRoleChan
		//rf.debug(rf.me,
		//	"Change role pack term:%d current term:%d pack role:%d current role:%d",
		//	pack.term, rf.currentTerm, pack.role, rf.role)
		if pack.term < rf.currentTerm {
			//rf.debug(rf.me,
			//	"Change role term lower than me, pack term:%d current term:%d pack role:%d current role:%d",
			//	pack.term, rf.currentTerm, pack.role, rf.role)
			continue
		}
		if rf.role != Leader && pack.role == Leader {
			rf.everLeader = false
		}
		rf.role = pack.role
		if pack.term > rf.currentTerm {
			rf.currentTerm = pack.term
			rf.votedFor = NotVoted
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
	//runtime.GOMAXPROCS(8)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = NotVoted
	rf.role = Follower
	rf.recvLogEventChan = make(chan bool)
	rf.log = []LogEntry{}
	rf.commitIndex = 0
	rf.memberCount = len(rf.peers)
	rf.currentTerm = 0
	rf.changeRoleChan = make(chan updateRoleOrTerm)
	rf.heartbeatChan = make(chan bool)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.updateRoleOrTerm()
	go rf.loop()
	return rf
}

func (rf *Raft) debug(sender int, pattern string, args ...interface{}) {
	var role string
	switch rf.role {
	case Leader:
		role = "Leader"
	case Candidate:
		role = "Candidate"
	case Follower:
		role = "Follower"
	}
	prefix := fmt.Sprintf("Sender: %d Me:%d Role:%s TERM:%d TIME:%d ", sender, rf.me, role,
		rf.currentTerm, time.Now().UnixNano()/int64(time.Millisecond))
	log.Println(prefix + fmt.Sprintf(pattern, args...))
}
