package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "time"


type Info struct {
	Seq		int
	Result	Config
}

type Wait struct {
    seq     int
    cond    *sync.Cond
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	persister		*raft.Persister
	maxraftstate	int
	configs			[]Config // indexed by config num
	assignment		map[int][]int
	clerkInfo		map[int64]Info // last seq
	notify			map[int64]Wait
	hasStart		map[int64]int
	isleader		bool
	term			int
}


type Op struct {
	// Your data here.
	Type    int // 0:Join, 1:Leave, 2:Move, 3:Query
	Args	interface{}
	ClerkId	int
	Seq		int
}

type Join struct {
	Servers map[int][]string // new GID -> servers mappings
}

type Leave struct {
	GIDs	[]int
}

type Move struct {
	Shard   int
    GID     int
}

type Query struct {
	Num	int // desired config number
}

func (sc *ShardCtrler) CheckState() {
    for sc.killed() == false {
        term, isleader := sc.rf.GetState()
        sc.mu.Lock()
        if term != sc.term {
            for _, wait := range sc.notify {
                wait.cond.Broadcast()
            }
            sc.hasStart = make(map[int64]int)
        }
        sc.isleader = isleader
        sc.term = term
        sc.mu.Unlock()

        time.Sleep(10 * time.Millisecond)
    }
}

func (sc *ShardCtrler) Execute() {
    for sc.killed() == false {
        msg := <- kv.applyCh

        sc.mu.Lock()
        if msg.CommandValid == true {
			op, _ := msg.Command.(Op)
            info, ok := kv.clerkInfo[op.ClerkId]
            if ok == false || op.Seq > info.Seq {
				var result Config
				if op.Type == 0 {
					args, _ := op.Args.(Join)
					for gid, servers := range args.Servers {
						if _, ok := sc.assignment[gid]; ok == true {
							continue
						}
						sc.Groups[gid] = servers

						if len(sc.assignment) == 0 {
							shards := make([]int, NShards)
							for i := 0; i < NShards; i++ {
								shards[i] = i
								sc.Shards[i] = gid
							}
							sc.assignment[gid] = shards
							continue
						}

						shards := []int
						for true {
							maxLen := 0
							maxGid := -1
							for id, shards := range sc.assignment {
								if len(shards) > maxLen {
									maxLen = len(shards)
									maxGid = id
								}
							}
							if maxLen - len(shards) > 1 {
								shards = append(shards, sc.assignment[maxGid][maxLen-1])
								sc.Shards[sc.assignment[maxGid][maxLen-1]] = gid
								sc.assignment[maxGid] = sc.assignment[maxGid][0 : maxLen-1]
							} else {
								break
							}
						}
						sc.assignment[gid] = shards
					}
				} else if op.Type == 1 {
					args, _ := op.Args.(Leave)
					shards := []int
					for _, gid := range args.GIDs {
						if _, ok := sc.assignment[gid]; ok == false {
							break
						}
						shards = append(shards, sc.assignment[gid])
						delete(sc.assignment, gid)
						delete(sc.Groups, gid)
					}
					if len(sc.assignment) == 0 {
						for i := 0; i < NShards; i++ {
							sc.Shards[i] = 0
						}
					} else {
						for _, shard := range shards {
							minLen := NShards + 1
							minGid := -1
							for id, s := range sc.assignment {
								if len(s) < minLen {
									minLen = len(s)
									minGid = id
								}
							}
							sc.assignment[minGid] = append(sc.assignment[minGid], shard)
							sc.Shards[shard] = minGid
						}
					}
				} else if op.Type == 2 {
					args, _ := op.Args.(Move)
					if _, ok := sc.Groups[args.GID]; ok == true {
						shards := []int
						for _, shard := range sc.assignment[sc.Shards[args.Shard]] {
							if shard != args.Shard {
								shards = append(shards, shard)
							}
						}
						sc.assignment[sc.Shards[args.Shard]] = shards
						sc.assignment[args.GID] = append(sc.assignment[args.GID], args.Shard)
						sc.Shards[args.Shard] = args.GID
					}
				} else if op.Type == 3 {
					args, _ := op.Args.(Query)
					if args.Num == -1 || args.Num >= len(sc.configs) {
						result = sc.configs[len(sc.configs)-1]
					} else {
						result == sc.configs[args.Num]
					}
				}
			}
			sc.clerkInfo[op.ClerkId] = Info{op.Seq, result}

			if wait, ok := sc.notify[op.ClerkId]; ok == true && wait.seq == op.Seq {
				wait.cond.Broadcast()
			}

			if sc.maxraftstate != -1 && sc.persister.RaftStateSize() > sc.maxraftstate {
                w := new(bytes.Buffer)
                e := labgob.NewEncoder(w)
                e.Encode(sc.configs)
                e.Encode(sc.clerkInfo)
                snapshot := w.Bytes()

                sc.rf.Snapshot(msg.CommandIndex, snapshot)
            }
		} else if msg.SnapshotValid == true {
			r := bytes.NewBuffer(sc.persister.ReadSnapshot())
            d := labgob.NewDecoder(r)
            var configs []Config
            var clerkInfo map[int64]int
            if d.Decode(&configs) == nil && d.Decode(&clerkInfo) == nil {
                sc.configs = configs
                sc.clerkInfo = clerkInfo
            }
		}

		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
    z := atomic.LoadInt32(&kv.dead)
    return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	// sc.configs = make([]Config, 1)
	// sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.persister = persister
	sc.maxraftstate = 1000
    r := bytes.NewBuffer(sc.persister.ReadSnapshot())
    d := labgob.NewDecoder(r)
    var configs []Config
    var clerkInfo map[int64]int
    if d.Decode(&database) == nil && d.Decode(&clerkInfo) == nil {
        sc.configs = configs
        sc.clerkInfo = clerkInfo
    } else {
		sc.configs = make([]Config, 1)
		sc.configs[0].Num = 0
		sc.configs[0].Groups = map[int][]string{}
		sc.configs[0].Shards = [NShards]int{}
		for i := 0; i < NShards; i++ {
			sc.configs[0].Shards[i] = 0
		}
		sc.clerkInfo = make(map[int64]int)
    }
    sc.notify = make(map[int64]Wait)
    sc.hasStart = make(map[int64]int)
    sc.isleader = false
    sc.term = -1

    go sc.Execute()
    go sc.CheckState()

	return sc
}
