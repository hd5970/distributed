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
	"sync/atomic"
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
	HeartBeatCycle      = 110 // heart beat duration in milliseconds
	ElectionTimeoutBase = 300
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
	Index int
	Term  int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm       int
	votedFor          int
	log               []LogEntry
	role              int
	lastHeartBeatUnix int64

	commitIndex int
	lastApplied int

	// leader should know
	nextIndex  []int
	matchIndex []int
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

func (rf *Raft) GetState() (int, bool) {
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

//handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	var reason string
	if rf.shouldAvoidElection() || args.Term < rf.currentTerm {
		if args.Term < rf.currentTerm {
			reason = "candidate term lower than me"
		} else {
			reason = "Should not election"
		}
		fmt.Printf("Recv vote from %d me %d vote:%t reason:%s\n",
			args.CandidateId, rf.me, reply.VoteGranted, reason)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateLastHeartBeat()
	rf.updateTermToGreater(args.Term)
	reply.Term = rf.currentTerm
	if rf.votedFor == NotVoted {
		rf.role = Follower
		if args.LastLogIndex >= rf.getLastLogIndex() {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			return
		} else {
			fmt.Printf("Recv vote from %d me %d vote:%t reason:%s\n",
				args.CandidateId, rf.me, reply.VoteGranted, "index lower than me")
		}
	}
	reason = fmt.Sprintf("Voted for is not null voted for:%d args term:%d prev term: %d", rf.votedFor, args.Term, reply.Term)
	fmt.Printf("Recv vote from %d me %d vote:%t reason:%s\n",
		args.CandidateId, rf.me, reply.VoteGranted, reason)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

// handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.role = Follower
		rf.updateTermToGreater(args.Term)
	}
	rf.updateLastHeartBeat()
	reply.Term = rf.currentTerm

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) needLaunchElection() bool {
	delta := time.Now().UnixNano() - rf.lastHeartBeatUnix
	return delta > int64(rf.randomElectionTimeout())
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	return time.Duration(rand.Int63n(int64(HeartBeatCycle))+ElectionTimeoutBase) * time.Millisecond
}

func (rf *Raft) launchElection() {
	rf.mu.Lock()
	// in case of fall back to follower
	if rf.role != Candidate {
		return
	}

	rf.currentTerm ++
	rf.votedFor = rf.me
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm()}
	rf.mu.Unlock()
	fmt.Printf("Launch a election me:%d term:%d\n", rf.me, rf.currentTerm)
	var ballotCount int32 = 1
	memberCount := len(rf.peers)
	var done = make(chan bool, memberCount)
	var electionDone = make(chan bool)
	wait := sync.WaitGroup{}
	wait.Add(memberCount - 1)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			defer func() {
				wait.Done()
			}()
			reply := &RequestVoteReply{}
			fmt.Printf("Me:%d send vote to %d\n", rf.me, i)
			ret := rf.sendRequestVote(i, args, reply)
			fmt.Printf("Me:%d Remote:%d rpc:%t Reply term:%d vote:%t\n", rf.me, i, ret, reply.Term, reply.VoteGranted)
			if ret {
				if reply.VoteGranted {
					atomic.AddInt32(&ballotCount, 1)
				} else if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.role = Follower
					rf.updateTermToGreater(reply.Term)
					rf.mu.Unlock()
					done <- true
				}
			}
		}(i)
	}
	go func() {
		for {
			select {
			case <-done:
				if ballotCount > int32(memberCount/2) {
					rf.mu.Lock()
					if rf.role == Candidate {
						rf.role = Leader
						go rf.lead()
					}
					rf.mu.Unlock()
				}
				electionDone <- true
				return
			case <-time.After(time.Millisecond * 1):
				if ballotCount > int32(memberCount/2) {
					rf.mu.Lock()
					if rf.role == Candidate {
						rf.role = Leader
						go rf.lead()
					}
					rf.mu.Unlock()
					electionDone <- true
					return
				}
			}
		}
	}()
	fmt.Println("All fork me ", rf.me)
	wait.Wait()
	fmt.Printf("All returns me:%d ballot:%d\n ", rf.me, ballotCount)
	done <- true
	<-electionDone
	if rf.role == Candidate {
		fmt.Printf("Lose the election me:%d term:%d ballot:%d\n", rf.me, rf.currentTerm, ballotCount)
		time.Sleep(rf.randomElectionTimeout())
		rf.launchElection()
	}
}

func (rf *Raft) lead() {
	fmt.Printf("Now i am gonna lead the cluster me:%d term:%d\n", rf.me, rf.currentTerm)
	rf.appendEntriesToFollowers()
	ticker := time.NewTicker(time.Millisecond * HeartBeatCycle)
	for rf.role == Leader {
		select {
		case <-ticker.C:
			rf.appendEntriesToFollowers()
		}
	}
}

func (rf *Raft) appendEntriesToFollowers() {
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.getLastLogIndex(),
		PrevLogTerm:  rf.getLastLogTerm()}
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &AppendEntriesReply{}
			ret := rf.sendAppendEntries(i, args, reply)
			if ret {
				if reply.Term > rf.currentTerm {
					fmt.Printf("Found greater term from append log me:%d remote:%d, current term:%d reply term:%d\n",
						rf.me, i, rf.currentTerm, reply.Term)
					rf.mu.Lock()
					rf.role = Follower
					rf.updateTermToGreater(reply.Term)
					rf.mu.Unlock()
				}
			}

		}(i)
	}
}

func (rf *Raft) shouldAvoidElection() bool {
	return time.Now().UnixNano()-rf.lastHeartBeatUnix < int64(time.Millisecond*HeartBeatCycle)
}

func (rf *Raft) updateLastHeartBeat() {
	rf.lastHeartBeatUnix = time.Now().UnixNano()
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

func (rf *Raft) updateTermToGreater(term int) {
	if term == rf.currentTerm {
		return
	}
	rf.currentTerm = term
	rf.votedFor = NotVoted
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
	rf.votedFor = NotVoted
	rf.role = Follower
	rf.lastHeartBeatUnix = time.Now().UnixNano()

	go func() {
		for {
			if rf.role == Follower {
				if rf.needLaunchElection() {
					rf.role = Candidate
					rf.launchElection()
				}
			}
			time.Sleep(time.Millisecond * 10)
		}

	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
