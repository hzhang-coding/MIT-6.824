package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
//	"fmt"
//	"strings"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type	int // 0:Get, 1:Put, 2:Append
	Key		string
	Value	string // if exists
	ClerkId	int64
	Seq		int
}

type Info struct {
	seq		int
	result	string // if exists
}

type Wait struct {
	seq		int
	cond	*sync.Cond
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// isleader	bool
	database	map[string]string
	clerkInfo	map[int64]Info
	notify		map[int64]Wait
	hasStart	map[int64]int
	isleader	bool	
	term		int
}


func (kv *KVServer) CheckState() {
	for kv.killed() == false {
		term, isleader := kv.rf.GetState()
		kv.mu.Lock()
		if term != kv.term {
			for _, wait := range kv.notify {
				wait.cond.Broadcast()
			}
			kv.hasStart = make(map[int64]int)
		}
		kv.isleader = isleader
		kv.term = term
		kv.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}


func (kv *KVServer) Execute() {
	for kv.killed() == false {
		msg := <- kv.applyCh

		kv.mu.Lock()
		if msg.CommandValid == true {
			op, _ := msg.Command.(Op)
			info, ok := kv.clerkInfo[op.ClerkId]
			if ok == false || op.Seq > info.seq {
				if op.Type == 0 {
					kv.clerkInfo[op.ClerkId] = Info{op.Seq, kv.database[op.Key]}
				} else if op.Type == 1 {
					kv.database[op.Key] = op.Value
					kv.clerkInfo[op.ClerkId] = Info{op.Seq, ""}
				} else if op.Type == 2 {
					kv.database[op.Key] = kv.database[op.Key] + op.Value
					kv.clerkInfo[op.ClerkId] = Info{op.Seq, ""}
				}

				if wait, ok := kv.notify[op.ClerkId]; ok == true && wait.seq == op.Seq {
					wait.cond.Broadcast()
				}
			}
		} else if msg.SnapshotValid == true {

		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
    if kv.isleader == false {
        reply.Err = "WrongLeader"
		kv.mu.Unlock()
        return
    }

	if info, ok := kv.clerkInfo[args.ClerkId]; ok == true && args.Seq <= info.seq {
		if args.Seq == info.seq {
			reply.Value = info.result
		} 
		kv.mu.Unlock()
		return
	}

	if _, ok := kv.notify[args.ClerkId]; ok == true {
		reply.Err = "Waiting"
		kv.mu.Unlock()
		return
	}

	cond := sync.NewCond(&kv.mu)
	kv.notify[args.ClerkId] = Wait{args.Seq, cond}

	if seq, ok := kv.hasStart[args.ClerkId]; ok == false || seq < args.Seq {
		kv.hasStart[args.ClerkId] = args.Seq
		op := Op{}
		op.Type = 0
		op.Key = args.Key
		op.Value = ""
		op.ClerkId = args.ClerkId
		op.Seq = args.Seq

		kv.rf.Start(op)
	}

	term := kv.term

	go func() {
		time.Sleep(500 * time.Millisecond)
		cond.Broadcast()
	} ()

	cond.Wait()

	delete(kv.notify, args.ClerkId)

	if info, ok := kv.clerkInfo[args.ClerkId]; ok == true && args.Seq <= info.seq {
		if args.Seq == info.seq {
			reply.Value = info.result
		}
		kv.mu.Unlock()
		return
	}

	if term != kv.term || kv.isleader == false {
		reply.Err = "WrongLeader"
		kv.mu.Unlock()
		return
	}

	reply.Err = "Timeout"
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.isleader == false {
		reply.Err = "WrongLeader"
		kv.mu.Unlock()
		return
	}

	if info, ok := kv.clerkInfo[args.ClerkId]; ok == true && args.Seq <= info.seq {
		kv.mu.Unlock()
		return
	}

	if _, ok := kv.notify[args.ClerkId]; ok == true {
		reply.Err = "Waiting"
		kv.mu.Unlock()
		return
	}

	cond := sync.NewCond(&kv.mu)
	kv.notify[args.ClerkId] = Wait{args.Seq, cond}

	if seq, ok := kv.hasStart[args.ClerkId]; ok == false || seq < args.Seq {
		kv.hasStart[args.ClerkId] = args.Seq

		op := Op{}
		if args.Op == "Put" {
			op.Type = 1
		} else {
			op.Type = 2
		}
		op.Key = args.Key
		op.Value = args.Value
		op.ClerkId = args.ClerkId
		op.Seq = args.Seq

		kv.rf.Start(op)
	}

	term := kv.term

	go func() {
		time.Sleep(500 * time.Millisecond)
		cond.Broadcast()
	} ()

	cond.Wait()

	delete(kv.notify, args.ClerkId)

	if info, ok := kv.clerkInfo[args.ClerkId]; ok == true && args.Seq <= info.seq {
		kv.mu.Unlock()
		return
	}

	if term != kv.term || kv.isleader == false {
		reply.Err = "WrongLeader"
		kv.mu.Unlock()
		return
	}

	reply.Err = "timeout"
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	// kv.mu.Lock()
	// kv.applyCh <- raft.ApplyMsg{}
	// kv.mu.Unlock()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.clerkInfo = make(map[int64]Info)
	kv.notify = make(map[int64]Wait)
	kv.hasStart = make(map[int64]int)
	kv.isleader = false
	kv.term = -1

	go kv.Execute()
	go kv.CheckState()

	return kv
}
