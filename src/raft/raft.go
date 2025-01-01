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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

const tickInterval = 50 * time.Millisecond
const basePatience = 15

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type entrie struct {
	Id   int
	Term int
	V    interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []entrie
	status      int

	//Volatile state on all servers:
	commitIndex int
	lastApplied int
	patience    int

	//Volatile state on leaders:
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.status == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if term, ok := rf.validTerm(args.Term); !ok {
		reply.Term = term
		reply.VoteGranted = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index, term := 0, 0
	if len(rf.log) > 0 {
		index, term = rf.log[len(rf.log)-1].Id, rf.log[len(rf.log)-1].Term
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && ((args.LastLogTerm > term) || (args.LastLogTerm == term && args.LastLogIndex >= index)) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.patience = newPatience()
		DPrintf("term %v, %v agree %v to win\n", rf.currentTerm, rf.me, args.CandidateId)
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, ch chan bool) {
	DPrintf("%v sendRequestVote to %v start\n", rf.me, server)
	defer DPrintf("%v sendRequestVote to %v end\n", rf.me, server)
	reply := &RequestVoteReply{}
	_ = rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.validTerm(reply.Term)
	ch <- reply.VoteGranted
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entrie
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if term, ok := rf.validTerm(args.Term); !ok {
		reply.Term = term
		reply.Success = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	p := -1
	if args.PrevLogIndex > 0 {
		for p = range rf.log {
			if rf.log[p].Id == args.PrevLogIndex && rf.log[p].Term == args.Term {
				break
			}
		}
		if p == -1 {
			reply.Success = false
			return
		}
	}

	for i := range args.Entries {
		p += 1
		if p >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i])
			continue
		}
		if args.Entries[i].Id != rf.log[p].Id || args.Entries[i].Term != rf.log[p].Term {
			rf.log[p] = args.Entries[i]
			rf.log = rf.log[:p+1]
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.log[p].Id < rf.commitIndex {
			rf.commitIndex = rf.log[p].Id
		}
		//rf.commit()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%v sendAppendEntries to %v start\n", rf.me, server)
	defer DPrintf("%v sendAppendEntries to %v end\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.validTerm(reply.Term)
	if ok {
		rf.nextIndex[server] -= 1
		//resend by id-1
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	lastIndex := 0
	if len(rf.log) > 0 {
		lastIndex = rf.log[len(rf.log)-1].Id
	}
	if rf.status != LEADER {
		term := rf.currentTerm
		rf.mu.Unlock()
		return lastIndex + 1, term, false
	}
	newEntrie := entrie{lastIndex + 1, rf.currentTerm, command}
	rf.log = append(rf.log, newEntrie)
	rf.mu.Unlock()
	rf.startApply()
	return newEntrie.Id, newEntrie.Term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()
		switch status {
		case LEADER:
			rf.heatBeat()
		case FOLLOWER, CANDIDATE:
			if rf.patienceDown() {
				go rf.startElection()
			}
		}
		time.Sleep(tickInterval)
	}
}

func newPatience() int {
	return basePatience + rand.Intn(basePatience+1)
}

func (rf *Raft) patienceDown() bool {
	rf.patience -= 1
	if rf.patience == 0 {
		rf.status = CANDIDATE
		return true
	}
	return false
}

func (rf *Raft) heatBeat() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) startElection() {
	DPrintf("I'm %v, startElection\n", rf.me)
	defer DPrintf("I'm %v, startElection end\n", rf.me)
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.patience = newPatience()
	rf.votedFor = rf.me
	index, term := 0, 0
	if len(rf.log) > 0 {
		index, term = rf.log[len(rf.log)-1].Id, rf.log[len(rf.log)-1].Term
	}
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: index, LastLogTerm: term}
	rf.mu.Unlock()
	voteDone := make(chan bool, len(rf.peers)-1)
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, &args, voteDone)
		}
	}
	for done, votes := 1, 1; done < len(rf.peers); done += 1 {
		voteGranted := <-voteDone
		if voteGranted {
			votes += 1
			if votes*2 > len(rf.peers) {
				rf.winElection(args.Term)
			}
		}
	}
}

func (rf *Raft) winElection(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != CANDIDATE || term < rf.currentTerm {
		return
	}
	rf.status = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	index := 0
	if len(rf.log) > 0 {
		index = rf.log[len(rf.log)-1].Id
	}
	for i := range rf.nextIndex {
		rf.nextIndex[i] = index + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) startApply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index, term := 0, 0
	if len(rf.log) > 1 {
		index, term = rf.log[len(rf.log)-2].Id, rf.log[len(rf.log)-2].Term
	}
	args := AppendEntriesArgs{rf.currentTerm, rf.me, index, term, []entrie{rf.log[len(rf.log)-1]}, rf.commitIndex}
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) validTerm(term int) (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term < rf.currentTerm {
		return rf.currentTerm, false
	}
	if rf.currentTerm < term {
		rf.votedFor = -1
		rf.currentTerm = term
	}
	rf.patience = newPatience()
	rf.status = FOLLOWER
	rf.nextIndex = nil
	rf.matchIndex = nil
	return rf.currentTerm, true
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.patience = newPatience()
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
