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
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"time"
	"sort"
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
	Command interface{}
	Term    int
}

type Raft struct {
	mu        sync.RWMutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []Log
	isleader    bool

	commitIndex int
	lastApplied int

	time    time.Time
	timeout time.Duration
	applyCh chan ApplyMsg

	// only for leader
	nextIndex    []int
	matchIndex   []int
	lastCommit   []int
	lastCommTime []time.Time

	lastIncludedIndex	int
	lastIncludedTerm	int
	snapshot			[]byte

	applyCond	*sync.Cond
	sendCond	*sync.Cond
	updateCond	*sync.Cond
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
func (rf *Raft) persist(saveSnapshot bool) {
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
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	state := w.Bytes()

	if saveSnapshot == false {
		rf.persister.SaveRaftState(state)
	} else {
		rf.persister.SaveStateAndSnapshot(state, rf.snapshot)
	}
	fmt.Printf("count\n")
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(state []byte) {
	if state == nil || len(state) < 1 { // bootstrap without any state?
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
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) == nil && d.Decode(&votedFor) == nil && d.Decode(&log) == nil && d.Decode(&lastIncludedIndex) == nil && d.Decode(&lastIncludedTerm) == nil {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
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
	rf.mu.Lock()
	// fmt.Printf("Snapshot\n")
	if index > rf.lastIncludedIndex {
		rf.log = rf.log[index-rf.lastIncludedIndex:]
		rf.lastIncludedTerm = rf.log[0].Term
		rf.lastIncludedIndex = index
		rf.snapshot = snapshot
		rf.persist(true)
	}
	rf.mu.Unlock()
}

type InstallSnapshotArgs struct {
	Term				int
	LeaderId			int
	LastIncludedIndex	int
	LastIncludedTerm	int
	Data				[]byte
}

type InstallSnapshotReply struct {
	Term				int
	FollowerCommit 		int
}

func (rf *Raft) ApplySnapshot() {
	msg := ApplyMsg{}
	msg.CommandValid = false
	msg.SnapshotValid = true
	msg.Snapshot = rf.snapshot
	msg.SnapshotTerm = rf.lastIncludedTerm
	msg.SnapshotIndex = rf.lastIncludedIndex

	// fmt.Printf("Server %v apply snapshot %v\n", rf.me, rf.lastIncludedIndex)

	rf.mu.Unlock()

	rf.applyCh <- msg

	rf.mu.Lock()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	reply.FollowerCommit = rf.commitIndex

	if args.Term >= rf.currentTerm {
        rf.time = time.Now()
        if args.Term > rf.currentTerm {
            rf.currentTerm = args.Term
            rf.isleader = false
            rf.votedFor = -1
			rf.persist(false)
        }

		if args.LastIncludedIndex > rf.commitIndex {
			if args.LastIncludedIndex < rf.lastIncludedIndex + len(rf.log) {
				rf.log = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]
			} else {
				rf.log = make([]Log, 1)
			}
			rf.log[0].Term = args.LastIncludedTerm
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm
			rf.snapshot = args.Data
			rf.lastApplied = args.LastIncludedIndex
			rf.commitIndex = args.LastIncludedIndex
			rf.persist(true)

			reply.FollowerCommit = rf.commitIndex

			rf.ApplySnapshot()
		}
    }

	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int) {
	args := InstallSnapshotArgs{}
    reply := InstallSnapshotReply{}

    args.Term = rf.currentTerm
    args.LeaderId = rf.me
    args.LastIncludedIndex = rf.lastIncludedIndex
    args.LastIncludedTerm = rf.lastIncludedTerm
    args.Data = rf.snapshot
	rf.mu.RUnlock()

	ch := make(chan bool, 2)

    go func() {
		// fmt.Printf("Leader %v send snapshot %v to server %v\n", args.LeaderId, args.LastIncludedIndex, server)
        ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
        ch <- ok
    }()

    go func() {
        time.Sleep(40 * time.Millisecond)
        ch <- false
    }()

    done := <- ch
    if done == false {
        return
    }

	rf.mu.RLock()

    if rf.isleader == false || args.Term != rf.currentTerm || reply.Term > rf.currentTerm {
        rf.mu.RUnlock()
        return
    }

    rf.lastCommTime[server] = time.Now()

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.lastCommit[server] = reply.FollowerCommit

	rf.mu.RUnlock()
}


type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term           int
	Success        bool
	XTerm          int
	XIndex         int
	FollowerCommit int
}

func (rf *Raft) ApplyEntries() {
	rf.mu.Lock()
	for rf.killed() == false {
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{}
			msg.CommandValid = true
			msg.Command = rf.log[rf.lastApplied - rf.lastIncludedIndex].Command
			msg.CommandIndex = rf.lastApplied 
			// fmt.Printf("Server %v apply index %v\n", rf.me, rf.lastApplied)
			rf.mu.Unlock()

			rf.applyCh <- msg

			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	rf.time = time.Now()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.isleader = false
		rf.votedFor = -1
		rf.persist(false)
	}

	if args.PrevLogIndex >= len(rf.log) + rf.lastIncludedIndex {
		reply.XTerm = -1
		reply.XIndex = len(rf.log) + rf.lastIncludedIndex
	} else if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.XTerm = -2
		reply.XIndex = rf.commitIndex
	} else if rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term == args.PrevLogTerm {
		reply.Success = true
		for i := 0; i < len(args.Entries); i++ {
			j := args.PrevLogIndex + 1 + i - rf.lastIncludedIndex
			if j >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			} else if rf.log[j].Term != args.Entries[i].Term {
				rf.log = rf.log[:j]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		}
		rf.persist(false)
		if rf.commitIndex < args.LeaderCommit {
			n := args.PrevLogIndex + 1 + len(args.Entries)
			if args.LeaderCommit < n {
				rf.commitIndex = args.LeaderCommit
			} else if n - 1 > rf.commitIndex {
				rf.commitIndex = n - 1
			}
			rf.applyCond.Broadcast()
		}
	} else {
		reply.XTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
		i := args.PrevLogIndex - 1 - rf.lastIncludedIndex
		for i > rf.commitIndex - rf.lastIncludedIndex {
			if rf.log[i].Term != reply.XTerm {
				break
			}
			i--
		}
		reply.XIndex = i + 1 + rf.lastIncludedIndex
	}

	reply.FollowerCommit = rf.commitIndex

	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex - rf.lastIncludedIndex].Term
	if rf.matchIndex[server] == args.PrevLogIndex {
		begin := rf.nextIndex[server] - rf.lastIncludedIndex
		n := len(rf.log[begin:])
		/*
		if n > 100 {
			n = 100
		}
		*/
		args.Entries = make([]Log, n)
		copy(args.Entries, rf.log[begin : begin + n])
	}
	args.LeaderCommit = rf.commitIndex
	rf.mu.RUnlock()

	ch := make(chan bool, 2)

	go func() {
		// fmt.Printf("Leader %v send entries %v to server %v\n", args.LeaderId, args.PrevLogIndex + 1, server)
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		ch <- ok
	}()

	go func() {
		time.Sleep(40 * time.Millisecond)
		ch <- false
	}()

	done := <- ch
	if done == false {
		return
	}

	rf.mu.Lock()

	if rf.isleader == false || args.Term != rf.currentTerm || reply.Term > rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	rf.lastCommTime[server] = time.Now()
	update := false

	if reply.Success == true {
		rf.nextIndex[server] += len(args.Entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		update = true
	} else {
		if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XIndex
		} else if reply.XTerm == -2 {
			rf.nextIndex[server] = reply.XIndex + 1
			rf.matchIndex[server] = reply.XIndex
			update = true
		} else {
			if args.PrevLogIndex >= rf.lastIncludedIndex {
				i := args.PrevLogIndex - rf.lastIncludedIndex
				for i >= reply.XIndex - rf.lastIncludedIndex && i >= 0 {
					if rf.log[i].Term == reply.XTerm {
						rf.matchIndex[server] = i + rf.lastIncludedIndex
						update = true
						break
					}
					i--
				}
				rf.nextIndex[server] = i + 1 + rf.lastIncludedIndex
			} else {
				rf.nextIndex[server] = rf.lastIncludedIndex + 1
			}
		}
	}

	rf.lastCommit[server] = reply.FollowerCommit

	if update == true {
		index := []int{}
        for i := 0; i < len(rf.peers); i++ {
            if i == rf.me {
                index = append(index, len(rf.log) - 1 + rf.lastIncludedIndex)
            } else {
                index = append(index, rf.matchIndex[i])
            }
        }

        sort.Ints(index)
        if rf.commitIndex < index[len(rf.peers) / 2] {
            rf.commitIndex = index[len(rf.peers) / 2]
            rf.applyCond.Broadcast()
            rf.sendCond.Broadcast()
        }
	}

	rf.mu.Unlock()
}

func (rf *Raft) UpdateCommitIndex(term int) {
	rf.mu.Lock()
	for rf.killed() == false && term == rf.currentTerm {
		index := []int{}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				index = append(index, len(rf.log) - 1 + rf.lastIncludedIndex)
			} else {
				index = append(index, rf.matchIndex[i])
			}
		}

		sort.Ints(index)
		if rf.commitIndex < index[len(rf.peers) / 2] {
			rf.commitIndex = index[len(rf.peers) / 2]
			rf.applyCond.Broadcast()
			rf.sendCond.Broadcast()
		}

		rf.updateCond.Wait()
	}
	rf.mu.Unlock()
}

func (rf *Raft) CheckSendMessage(server int, term int) {
	rf.mu.RLock()
	for rf.killed() == false && term == rf.currentTerm {
		interval := time.Now().Sub(rf.lastCommTime[server])

		if interval > 300 * time.Millisecond {
			rf.mu.RUnlock()
			time.Sleep(50 * time.Millisecond)
			rf.mu.RLock()
		} else if rf.matchIndex[server] < rf.lastIncludedIndex {
			rf.sendInstallSnapshot(server)
			rf.mu.RLock()
		} else if rf.matchIndex[server] < rf.lastIncludedIndex+len(rf.log)-1 || rf.lastCommit[server] < rf.commitIndex {
			rf.sendAppendEntries(server)
			rf.mu.RLock()
		} else {
			rf.sendCond.Wait()
		}
	}
	rf.mu.RUnlock()
}


type HeartbeatArgs struct {
	Term int
}

type HeartbeatReply struct {
	Term int
}

func (rf *Raft) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	if args.Term >= rf.currentTerm {
		rf.time = time.Now()
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.isleader = false
			rf.votedFor = -1
            rf.persist(false)
		}
	}

	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeat(server int, term int) {
	args := HeartbeatArgs{}
	reply := HeartbeatReply{}
	args.Term = term

	ch := make(chan bool, 2)

    go func() {
		ok := rf.peers[server].Call("Raft.Heartbeat", &args, &reply)
        ch <- ok
    }()

    go func() {
        time.Sleep(50 * time.Millisecond)
        ch <- false
    }()

    done := <- ch
    if done == false {
        return
    }

	rf.mu.Lock()
	if args.Term == rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.isleader = false
			rf.votedFor = -1
            rf.persist(false)
		} else {
			rf.lastCommTime[server] = time.Now()
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) startHeartbeat(term int) {
    for rf.killed() == false {
        rf.mu.RLock()
        if rf.isleader == false || rf.currentTerm != term {
            rf.mu.RUnlock()
            return
        }
        rf.mu.RUnlock()

        for i := 0; i < len(rf.peers); i++ {
            if i == rf.me {
                continue
            }

            go rf.sendHeartbeat(i, term)
        }

        time.Sleep(100 * time.Millisecond)
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
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.isleader = false
		rf.persist(false)
	}

	n := len(rf.log)
	if args.Term < rf.currentTerm || args.LastLogTerm < rf.log[n-1].Term || (args.LastLogTerm == rf.log[n-1].Term && args.LastLogIndex < n-1+rf.lastIncludedIndex) {
		rf.mu.Unlock()
		return
	}

	if rf.votedFor == -1 {
		rf.time = time.Now()
		rf.votedFor = args.CandidateId
		rf.persist(false)
		reply.VoteGranted = true
	} else if rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, vote map[int]int) bool {
	reply := RequestVoteReply{}

	ch := make(chan bool, 2)

    go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
        ch <- ok
    }()

    go func() {
        time.Sleep(40 * time.Millisecond)
        ch <- false
    }()

    done := <- ch
	if done == false {
        return false
    }

	rf.mu.Lock()
	if args.Term == rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.isleader = false
			rf.votedFor = -1
			rf.persist(false)
		} else if reply.VoteGranted == true {
			vote[server] = 1
		}
		rf.lastCommTime[server] = time.Now()
	}
	rf.mu.Unlock()

	return true
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
		// fmt.Printf("Leader %v receive index %v\n", rf.me, index)
		rf.log = append(rf.log, Log{command, rf.currentTerm})
		rf.persist(false)
		rf.sendCond.Broadcast()

		index = len(rf.log) - 1 + rf.lastIncludedIndex
		term = rf.currentTerm
		isLeader = true
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
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.sendCond.Broadcast()
	rf.updateCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		sleeptime := rf.timeout

		rf.mu.Lock()
		if rf.isleader == false {
			duetime := rf.time.Add(rf.timeout)
			now := time.Now()
			if now.After(duetime) == true {
				rf.time = now
				rf.timeout = time.Duration(300+rand.Int()%300) * time.Millisecond
				sleeptime = rf.timeout

				go rf.LeaderElection(rf.currentTerm)
			} else {
				sleeptime = duetime.Sub(now)
			}
		}
		rf.mu.Unlock()

		time.Sleep(sleeptime)
	}
}

func (rf *Raft) LeaderElection(term int) {
	rf.mu.Lock()
	if rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist(false)
	rf.lastCommTime = make([]time.Time, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		rf.lastCommTime[i] = time.Now().AddDate(-1, 0, 0)
	}


	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1 + rf.lastIncludedIndex
	args.LastLogTerm = rf.log[len(rf.log) - 1].Term

	cond := sync.NewCond(&rf.mu)
	vote := map[int]int{rf.me: 1}
	complete := 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			duetime := time.Now().Add(200 * time.Millisecond)
			for time.Now().Before(duetime) == true {
				ok := rf.sendRequestVote(server, &args, vote)
				if ok == true {
					break
				}
			}

			rf.mu.Lock()
			n := len(rf.peers)
			complete++
			if rf.currentTerm != args.Term || complete == n || len(vote) > n / 2 {
				cond.Broadcast()
			}
			rf.mu.Unlock()
		} (i)
	}

	cond.Wait()

	if rf.currentTerm != args.Term || len(vote) <= len(rf.peers) / 2 {
		rf.mu.Unlock()
		return
	}

	rf.isleader = true
	// fmt.Printf("Leader of term %v is %v\n", rf.currentTerm, rf.me)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastCommit = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
		rf.matchIndex[i] = 0
		rf.lastCommit[i] = 0

		go rf.CheckSendMessage(i, rf.currentTerm)
	}

	go rf.startHeartbeat(rf.currentTerm)
	// go rf.UpdateCommitIndex(rf.currentTerm)

	rf.mu.Unlock()
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
	rf.isleader = false

	rf.applyCh = applyCh

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1

	rf.applyCond = sync.NewCond(&rf.mu)
	rf.sendCond = sync.NewCond(rf.mu.RLocker())
	rf.updateCond = sync.NewCond(&rf.mu)
	// rf.sendCond = sync.NewCond(&rf.mu)

	rand.Seed(time.Now().UnixNano())
	rf.time = time.Now()
	rf.timeout = time.Duration(300+rand.Int()%300) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	rf.log[0].Term = rf.lastIncludedTerm
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ApplyEntries()

	return rf
}
