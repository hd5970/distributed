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
	"runtime"
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
	HeartBeatCycle      = 150 // heart beat duration in milliseconds
	ElectionTimeoutBase = 180
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

	recvLogEventChan chan bool
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
	return index, rf.currentTerm, true
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
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	var reason string
	rf.mu.Lock()
	defer func() {
		rf.debug(args.CandidateId, rf.me, "recv vote from %d vote:%t reason:%s",
			args.CandidateId, reply.VoteGranted, reason)
		rf.mu.Unlock()
	}()
	reply.Term = rf.currentTerm
	//if rf.shouldAvoidElection() || args.Term < rf.currentTerm {
	if args.Term < rf.currentTerm {
		if args.Term < rf.currentTerm {
			reason = "Candidate term lower than me"
		} else {
			reason = "Should not election"
		}
		return
	}
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
			reason = "Log index lower than me"
		}
	}
	reason = fmt.Sprintf("Voted for is not null voted for:%d args term:%d prev term: %d", rf.votedFor, args.Term, reply.Term)
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
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
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.debug(args.LeaderId, rf.me, "append entry lower than me args term:%d my term:%d",
			args.Term, rf.currentTerm)
		return
	} else if args.Term >= rf.currentTerm {
		rf.role = Follower
		rf.updateTermToGreater(args.Term)
	}
	rf.updateLastHeartBeat()

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

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
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

	rf.updateTermToGreater(rf.currentTerm + 1)
	rf.debug(rf.me, rf.me, "term has been upgrade to:%d", rf.currentTerm)
	rf.votedFor = rf.me
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm()}
	rf.mu.Unlock()
	rf.debug(rf.me, rf.me, "launch a election term:%d ", rf.currentTerm)
	var ballotCount int = 1
	memberCount := len(rf.peers)
	wait := sync.WaitGroup{}
	wait.Add(memberCount - 1)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			defer wait.Done()
			reply := &RequestVoteReply{}
			rf.debug(rf.me, rf.me, "send vote request to %d term:%d", i, args.Term)
			ret := rf.sendRequestVote(i, args, reply)
			rf.debug(rf.me, rf.me,
				"vote request returns remote:%d rpc:%t reply term:%d vote:%t args.term:%d", i, ret,
				reply.Term, reply.VoteGranted, args.Term)
			if ret {
				if reply.VoteGranted {
					rf.mu.Lock()
					ballotCount ++
					if ballotCount > memberCount/2 {
						if rf.role == Candidate {
							rf.role = Leader
							rf.debug(rf.me, rf.me, "win the election term:%d ballot:%d",
								rf.currentTerm, ballotCount)
							go rf.lead()
						}
					}
					rf.mu.Unlock()
				} else if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.role = Follower
					rf.updateTermToGreater(reply.Term)
					rf.mu.Unlock()
				}
			}
		}(i)
	}
	wait.Wait()
	if rf.role == Candidate {
		rf.debug(rf.me, rf.me, "lose the election term:%d ballot:%d", rf.currentTerm, ballotCount)
		time.Sleep(rf.randomElectionTimeout())
		rf.launchElection()
	}
}

func (rf *Raft) lead() {
	rf.debug(rf.me, rf.me,
		"i am gonna lead the cluster term:%d", rf.currentTerm)
	// init leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = rf.commitIndex + 1
	}
	rf.appendEntriesToFollowers()
	for {
		if rf.role == Leader {
			select {
			case <-time.After(time.Millisecond * HeartBeatCycle):
				if rf.role == Leader {
					rf.appendEntriesToFollowers()
				}
			case <-rf.recvLogEventChan:
				if rf.role == Leader {
					rf.appendEntriesToFollowers()
				}
			}

		} else {
			break
		}
	}
}

func (rf *Raft) appendEntriesToFollowers() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			if rf.matchIndex[i] != rf.nextIndex[i]-1 && rf.matchIndex[i] != -1 {
				// 已经发送过了,但是对方没有返回成功, 所以上一次请求应该还在重试,这次miss
				rf.debug(rf.me, rf.me, "fail to send log, prev not returns remote:%d "+
					"matchIndex:%d nextIndex:%d leader lastIndex:%d", i, rf.matchIndex[i],
					rf.nextIndex[i], rf.getLastLogIndex())
				return
			}
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.matchIndex[i],
				LeaderCommit: rf.commitIndex,
				Entries:      rf.getRelativeLogEntries(rf.nextIndex[i]),
				PrevLogTerm:  rf.getRelativeLog(rf.matchIndex[i]).Term}
			rf.nextIndex[i] = rf.nextIndex[i] + len(args.Entries)
			rf.mu.Unlock()
			//rf.debug(rf.me, rf.me, "append entry arg args term:%d remote:%d current term:%d ",
			//	args.Term, i, rf.currentTerm)
			reply := &AppendEntriesReply{}
			if rf.role == Leader {
				ret := rf.sendAppendEntries(i, args, reply)
				if ret {
					if rf.role != Leader {
						return
					}
					if reply.Term > rf.currentTerm {
						//rf.debug(rf.me, rf.me,
						//	"found greater term from append log remote:%d, "+
						//		"current term:%d reply term:%d", i, rf.currentTerm, reply.Term)
						rf.mu.Lock()
						rf.role = Follower
						rf.updateTermToGreater(reply.Term)
						rf.mu.Unlock()
						return
					}
					if reply.Success == true {
						//rf.debug(rf.me, rf.me, "successfully send log to remote:%d term:%d reply.Term:%d ",
						//	i, rf.currentTerm, reply.Term)
						//rf.mu.Lock()
						if len(args.Entries) > 0 {
							rf.matchIndex[i] = args.Entries[len(args.Entries)-1].Index
							rf.nextIndex[i] = rf.matchIndex[i] + 1
						} else if rf.matchIndex[i] == -1 {
							rf.matchIndex[i] = 0
						}

						//rf.mu.Unlock()
					} else {
						// gonna retry with lower index
						//rf.debug(rf.me, rf.me, "append log return false remote:%d "+
						//	"gonna minus next index:%d", i, rf.nextIndex[i])
						//rf.mu.Lock()
						rf.nextIndex[i] = rf.nextIndex[i] - 1
						//rf.mu.Unlock()
						//goto Retry
					}
				} else {
					// 远端没有返回结果 rpc失败
					//rf.debug(rf.me, rf.me, "append log rpc false, remote:%d", i)
					//goto Retry
				}
			}

		}(i)
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
	} else if term < rf.currentTerm {
		panic("fuck")
	}
	rf.debug(rf.me, rf.me, "current term :%d has been set to:%d", rf.currentTerm, term)
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
	runtime.GOMAXPROCS(8)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = NotVoted
	rf.role = Follower
	rf.lastHeartBeatUnix = time.Now().UnixNano()
	rf.recvLogEventChan = make(chan bool, RecvChanBufferSize)
	rf.log = []LogEntry{}
	rf.commitIndex = 0
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

func (rf *Raft) debug(sender int, me int, pattern string, args ...interface{}) {
	prefix := fmt.Sprintf("Sender: %d Me:%d Role:%d TERM:%d TIME:%d ", sender, me, rf.role, rf.currentTerm, time.Now().UnixNano()/int64(time.Millisecond))
	println(prefix + fmt.Sprintf(pattern, args...))
}
