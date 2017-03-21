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
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	STATE_LEADER = iota
	STATE_CANDIDATE
	STATE_FOLLWER
	HB_INTERVAL = 100 * time.Millisecond //100ms
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
	LogIndex int
	LogTerm  int
	LogComd  interface{}
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

	//chanel
	state         int
	voteCount     int
	chanHearBeat  chan bool
	chanGrantVote chan bool
	chanLeader    chan bool
	chanApply     chan ApplyMsg
	chanCommit    chan bool
	//persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leaders:
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	/*	var term int
		var isleader bool
		// Your code here (2A).
		return term, isleader */
	return rf.currentTerm, rf.state == STATE_LEADER
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].LogIndex
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer rf.persist()
	DPrintf("receive requestvote from candi %d term %d , i'm rf %d , my state %d ,my term %d\n", args.CandidateId, args.Term, rf.me, rf.state, rf.currentTerm)
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLWER
		rf.votedFor = -1
		DPrintf("i'm rf %d , term set to %d, my state %d\n", rf.me, rf.currentTerm, rf.state)
	}
	reply.Term = rf.currentTerm

	term := rf.getLastTerm()
	index := rf.getLastIndex()

	uptoDate := false

	if args.LastLogTerm > term {
		uptoDate = true
	}

	if args.LastLogTerm == term && args.LastLogIndex >= index { //maybe same as new
		uptoDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptoDate {
		rf.chanGrantVote <- true
		rf.state = STATE_FOLLWER
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		DPrintf("i'm %d , grant vote to candi %d ,my state %d, my term %d\n", rf.me, rf.votedFor, rf.state, rf.currentTerm)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	DPrintf("receive AppendEntries from peer %d term %d , i'm rf %d , my state %d my term %d\n", args.LeaderId, args.Term, rf.me, rf.state, rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}
	rf.chanHearBeat <- true

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLWER
		rf.votedFor = -1
		DPrintf("i'm rf %d , term set to %d, my state %d\n", rf.me, rf.currentTerm, rf.state)
	}
	reply.Term = args.Term

	if args.PrevLogIndex > rf.getLastIndex() {
		reply.NextIndex = rf.getLastIndex()
		return
	}

	baseIndex := rf.log[0].LogIndex

	//*************
	if args.PrevLogIndex > baseIndex {
		term := rf.log[args.PrevLogIndex-baseIndex].LogTerm
		if args.PrevLogTerm != term {
			//prelog is not in the same term as this rf
			reply.NextIndex = baseIndex
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				//move left a whole term(the same term as PrelogIndex's term in this rf ) in the rf's log
				if rf.log[i-baseIndex].LogTerm != term {
					//roll back a term to try to sync with leader
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}

	//args.PreLogIndex > baseIndex && args.PreLogTerm == term ||
	//args.PreLogIndex <= baseIndex

	if args.PrevLogIndex < baseIndex {
		//**************** gap exists,with args.Term >= rf.currentTerm
		//reply.nextIndex = ?
		fmt.Println("args.PrevLogIndex < baseIndex")
	} else {
		//args.PreLogIndex > baseIndex && args.PreLogTerm == term ||
		//**********args.PreLogIndex == baseIndex =?> term is equal?

		rf.log = rf.log[:args.PrevLogIndex+1-baseIndex]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		reply.NextIndex = rf.getLastIndex() + 1
	}

	if args.LeaderCommit > rf.commitIndex {
		last := rf.getLastIndex()
		//****************
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit <- true
	}

	return
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
// can't be reached, a lost request, or a lost reply.args
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		DPrintf("receive vote reply from rf %d ,i'm cani %d\n", server, rf.me)
		term := rf.currentTerm

		if rf.state != STATE_CANDIDATE {
			return ok
		}

		if args.Term != term {
			return ok
		}

		if reply.Term > term {
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLWER
			rf.votedFor = -1
			DPrintf("i'm rf %d , term set to %d, my state %d\n", rf.me, rf.currentTerm, rf.state)
			rf.persist()
		}

		if reply.VoteGranted {
			rf.voteCount++
			if rf.state == STATE_CANDIDATE && rf.voteCount > len(rf.peers)/2 {
				//***************************rf.state =
				rf.chanLeader <- true
				DPrintf("got >half %d votes,i'm %d,  prepare to convert to leader, term %d\n", rf.voteCount, rf.me, rf.currentTerm)
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.state != STATE_LEADER {
			return ok
		}

		if args.Term != rf.currentTerm {
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLWER
			rf.votedFor = -1
			DPrintf("i'm rf %d , term set to %d, my state %d\n", rf.me, rf.currentTerm, rf.state)
			rf.persist()
			return ok
		}

		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1
				rf.matchIndex[server] = rf.nextIndex[server]
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	}

	return ok
}

func (rf *Raft) broadcastAppendEntries() {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	N := rf.commitIndex
	last := rf.getLastIndex()
	baseIndex := rf.log[0].LogIndex
	DPrintf("leader %d broadcastAppendEntries , term %d my state %d\n", rf.me, rf.currentTerm, rf.state)
	for i := rf.commitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-baseIndex].LogTerm == rf.currentTerm {
				//judge whether i is commited
				num++
			}
		}

		if 2*num > len(rf.peers) {
			N = i
		}
	}

	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.chanCommit <- true
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_LEADER {
			if rf.nextIndex[i] > baseIndex {
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].LogTerm
				args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1-baseIndex:]))
				copy(args.Entries, rf.log[args.PrevLogIndex+1-baseIndex:])
				args.LeaderCommit = rf.commitIndex

				go func(i int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					DPrintf("leader %d sendAppendEntries to rf %d ,LeaderCommit %d term %d\n", rf.me, i, args.LeaderCommit, rf.currentTerm)
					rf.sendAppendEntries(i, &args, &reply)
				}(i, args)
			} else {
				//snapshot
			}
		}
	}
}

func (rf *Raft) broadcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = rf.getLastTerm()
	args.LastLogIndex = rf.getLastIndex()
	rf.mu.Unlock()
	DPrintf("candiate %d broadcastRequestVote in term %d\n", rf.me, rf.currentTerm)
	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_CANDIDATE {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, &args, &reply)
			}(i)
		}
	}
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
	rf.chanApply = applyCh

	// Your initialization code here (2A, 2B, 2C).
	//persistent state
	rf.state = STATE_FOLLWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0, LogIndex: 0})
	rf.currentTerm = 0

	//chanel
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanHearBeat = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	rf.chanCommit = make(chan bool, 100)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//volatile of leader
	rf.commitIndex = 0
	rf.lastApplied = 0
	fmt.Printf("rf %d is up in term %d\n", me, rf.currentTerm)
	go func() {
		for {
			switch rf.state {
			case STATE_FOLLWER:
				select {
				case <-rf.chanHearBeat:
				case <-rf.chanGrantVote:
				//election timeout
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
					rf.state = STATE_CANDIDATE
					DPrintf("rf %d convert to candidate in term %d\n", rf.me, rf.currentTerm)
				}

			case STATE_LEADER:
				rf.broadcastAppendEntries()
				time.Sleep(HB_INTERVAL)

			case STATE_CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()
				go rf.broadcastRequestVote()

				select {
				//reset election timeout
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
				case <-rf.chanHearBeat:
					rf.state = STATE_FOLLWER

				case <-rf.chanLeader:
					rf.mu.Lock()
					rf.state = STATE_LEADER
					DPrintf("rf %d convert to leader in term %d\n", rf.me, rf.currentTerm)
					//get index of peers
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						//*****************************
						rf.nextIndex[i] = rf.getLastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			}
		}
	}()

	return rf
}
