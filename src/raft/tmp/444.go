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
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"time"
	"math/rand"
	"fmt"
	"bytes"
	"6.824/labgob"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Log struct {
	Command	interface{}
	Term	int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm		int
	votedFor		int
	log				[]Log
	isleader		bool

	commitIndex		int
	lastApplied		int

	time			time.Time
	timeout			time.Duration
	applyCh			chan ApplyMsg
	dirty			bool

	// only for leader
	count			[]map[int]int 
	nextIndex		[]int
	matchIndex		[]int
	lastCommit		[]int
	lastCommTime	[]time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isleader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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
	var votedFor int
	var log []Log
	if d.Decode(&currentTerm) == nil && d.Decode(&votedFor) == nil && d.Decode(&log) == nil {
	   rf.currentTerm = currentTerm
	   rf.votedFor = votedFor
	   rf.log = log
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]Log
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term			int
	Success			bool
	XTerm			int
	XIndex			int
	FollowerCommit	int
}

func (rf *Raft) ApplyEntries() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		msg.CommandValid = true
		msg.Command = rf.log[i].Command
		msg.CommandIndex = i

		// fmt.Printf("Server %v apply index %v\n", rf.me, i)

		rf.applyCh <- msg
	}

	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	reply.Success = false

	if args.Term >= rf.currentTerm {
		rf.time = time.Now()
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.isleader = false
			rf.votedFor = -1
			// rf.persist()
			rf.dirty = true
		}

		if args.PrevLogIndex < len(rf.log) {
			if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
				reply.Success = true
				if args.PrevLogIndex + 1 < len(rf.log) {
					rf.log = rf.log[: args.PrevLogIndex + 1]
				}
				rf.log = append(rf.log, args.Entries...)
				// rf.persist()
				rf.dirty = true
				if rf.commitIndex < args.LeaderCommit {
					if args.LeaderCommit < len(rf.log) {
						rf.commitIndex = args.LeaderCommit
					} else {
						rf.commitIndex = len(rf.log) - 1
					}
					rf.ApplyEntries()
				}
			} else {
				reply.XTerm = rf.log[args.PrevLogIndex].Term
				i := args.PrevLogIndex - 1
				for i > rf.commitIndex {
					if rf.log[i].Term != reply.XTerm {
						break
					}
					i--
				}
				reply.XIndex = i + 1
			}
		} else {
			reply.XTerm = -1
			reply.XIndex = len(rf.log)
		}

		reply.FollowerCommit = rf.commitIndex
	}

	reply.Term = rf.currentTerm

	if rf.dirty == true {
		rf.persist()
		rf.dirty = false
	}

	rf.mu.Unlock()
}


func (rf *Raft) sendAppendEntries(server int) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	if rf.matchIndex[server] == args.PrevLogIndex {
		args.Entries = rf.log[rf.nextIndex[server] : ]
	}
	args.LeaderCommit = rf.commitIndex
	if rf.dirty == true {
		rf.persist()
		rf.dirty = false
	}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	if ok == false {
		return 
	}

	rf.mu.Lock()
	rf.lastCommTime[server] = time.Now()
	if args.Term != rf.currentTerm || args.PrevLogIndex != rf.nextIndex[server] - 1 {
		rf.mu.Unlock()
		return  
	}

	if reply.FollowerCommit > rf.lastCommit[server] {
		rf.lastCommit[server] = reply.FollowerCommit
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.isleader = false
		rf.votedFor = -1
		// rf.persist()
		rf.dirty = true
	} else {
		if reply.Success == true {
			rf.nextIndex[server] += len(args.Entries)
			end := rf.matchIndex[server]
			if end < rf.commitIndex {
				end = rf.commitIndex
			}
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			i := rf.matchIndex[server]
			n := len(rf.peers)
			for i > end && rf.log[i].Term == rf.currentTerm {
				rf.count[i][server] = 0
				if len(rf.count[i]) > n / 2 {
					rf.commitIndex = i
					rf.ApplyEntries()
					break
				}
				i--
			}
		} else {
			if reply.XTerm != -1 {
				i := rf.nextIndex[server] - 1
				for i >= reply.XIndex {
					if rf.log[i].Term == reply.XTerm {
						rf.matchIndex[server] = i
						break
					}
					i--
				}
				rf.nextIndex[server] = i + 1
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
		}
	}
	rf.mu.Unlock()
}


func (rf *Raft) CheckSendAppendEntries(server int, term int) {
    for rf.killed() == false {
		toSend := false

        rf.mu.Lock()
		if rf.isleader == false || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		if rf.matchIndex[server] < len(rf.log) - 1 || rf.lastCommit[server] < rf.commitIndex  {
			toSend = true
		}

		interval := time.Now().Sub(rf.lastCommTime[server])
        rf.mu.Unlock()

		if interval > 400 * time.Millisecond {
			time.Sleep(100 * time.Millisecond)
		} else if interval > 200 * time.Millisecond {
			time.Sleep(40 * time.Millisecond)
		} else {
			if toSend == true {
				rf.sendAppendEntries(server)
			} else {
				time.Sleep(20 * time.Millisecond)
			}
		}
    }
}


type HeartbeatArgs struct {
    Term	int
}

type HeartbeatReply struct {
    Term    int
}


func (rf *Raft) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	rf.mu.Lock()

    if args.Term >= rf.currentTerm {
        rf.time = time.Now()
        if args.Term > rf.currentTerm {
            rf.currentTerm = args.Term
            rf.isleader = false
			rf.votedFor = -1
			// rf.persist()
			rf.dirty = true
        }
    }

    reply.Term = rf.currentTerm

	if rf.dirty == true {
		rf.persist()
		rf.dirty = false
	}

    rf.mu.Unlock()
}


func (rf *Raft) sendHeartbeat(server int, term int) {
	args := HeartbeatArgs{}
	reply := HeartbeatReply{}

	args.Term = term

	ok := rf.peers[server].Call("Raft.Heartbeat", &args, &reply)

	if ok == true {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.isleader = false
			rf.votedFor = -1
			// rf.persist()
			rf.dirty = true
		}
		rf.lastCommTime[server] = time.Now()
		rf.mu.Unlock()
	}
}


func (rf *Raft) startHeartbeat(term int) {
    for rf.killed() == false {
		rf.mu.Lock()
		if rf.isleader == false || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		n := len(rf.peers)
		me := rf.me
        rf.mu.Unlock()

		for i := 0; i < n; i++ {
			if i == me {
				continue
			}

			go rf.sendHeartbeat(i, term)
		}

		// sleeptime := time.Duration(100 + rand.Int() % 20) * time.Millisecond
		time.Sleep(100 * time.Millisecond)
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int
	VoteGranted		bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.isleader = false
		rf.dirty = true
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	n := len(rf.log)
	if args.Term < rf.currentTerm || args.LastLogTerm < rf.log[n-1].Term || (args.LastLogTerm == rf.log[n-1].Term && args.LastLogIndex < n - 1) {
		if rf.dirty == true {
			rf.persist()
			rf.dirty = false
		}
		rf.mu.Unlock()
		return
	}

	if rf.votedFor == -1 {
		rf.time = time.Now()
		rf.votedFor = args.CandidateId
		rf.dirty = true
		reply.VoteGranted = true
	} else if rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
	}

	if rf.dirty == true {
		rf.persist()
		rf.dirty = false
	}
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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
 	if rf.isleader == true {
		rf.log = append(rf.log, Log{command, rf.currentTerm})
		rf.count = append(rf.count, map[int]int{rf.me: 1})
		index = len(rf.log) - 1
		term = rf.currentTerm
		isLeader = true
		// rf.persist()
		rf.dirty = true
	}
	rf.mu.Unlock()

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var sleeptime time.Duration

	rf.mu.Lock()
	rf.time = time.Now()
	rf.timeout = time.Duration(100 + rand.Int() % 100) * time.Millisecond
	sleeptime = rf.timeout
	rf.mu.Unlock()

	// go rf.LeaderElection()

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(sleeptime)

		rf.mu.Lock()
		if rf.isleader == false {
			duetime := rf.time.Add(rf.timeout)
			now := time.Now()
			if now.After(duetime) == true {
				rf.time = now
				rf.timeout = time.Duration(300 + rand.Int() % 300) * time.Millisecond
				sleeptime = rf.timeout
				rf.mu.Unlock()

				go rf.LeaderElection()
			} else {
				sleeptime = duetime.Sub(now)
				rf.mu.Unlock()
			}
		} else {
			sleeptime = rf.timeout
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) LeaderElection() {
	args := RequestVoteArgs{}
	vote := 1
	complete := 1

	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	n := len(rf.peers)
	me := rf.me
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[args.LastLogIndex].Term
	rf.persist()
	rf.mu.Unlock()

	for i := 0; i < n; i++ {
		if i == me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			ok := false
			duetime := time.Now().Add(200 * time.Millisecond)

			for ok == false && time.Now().Before(duetime) == true {
				ok = rf.sendRequestVote(server, &args, &reply)
			}

			rf.mu.Lock()
			if ok {
				if reply.VoteGranted == true {
					vote++
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.isleader = false
					rf.votedFor = -1
					// rf.persist()
					rf.dirty = true
				}
			}
			complete++
			rf.mu.Unlock()
		} (i)
	}

	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)

		exit := false

		rf.mu.Lock()
		if complete == n || rf.currentTerm != args.Term {
			exit = true
		}
		if rf.currentTerm == args.Term && vote > n / 2 {
			rf.isleader = true
			rf.dirty = true
			exit = true

			fmt.Printf("Leader is %v\n", rf.me)

			rf.count = make([]map[int]int, len(rf.log))
			for i := rf.commitIndex + 1; i < len(rf.log); i++ {
				rf.count[i] = map[int]int{rf.me: 1}
			}

			go rf.startHeartbeat(rf.currentTerm)

			rf.nextIndex = make([]int, n)
			rf.matchIndex = make([]int, n)
			rf.lastCommit = make([]int, n)
			rf.lastCommTime = make([]time.Time, n)
			for i := 0; i < n; i++ {
				if i == me {
					continue
				}

				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = rf.commitIndex
				rf.lastCommit[i] = rf.commitIndex
				rf.lastCommTime[i] = time.Now()

				go rf.CheckSendAppendEntries(i, rf.currentTerm)
			}
		}
		rf.mu.Unlock()

		if exit == true {
			break
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 1)
	rf.log[0].Term = -1
	rf.isleader = false

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.dirty = false

	rand.Seed(time.Now().UnixNano() + int64(rf.me))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
