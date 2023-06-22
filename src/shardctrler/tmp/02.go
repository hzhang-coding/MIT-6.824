package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "time"
import "sync/atomic"
import "bytes"
import "fmt"


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
	dead    int32 // set by Kill()

	// Your data here.
	persister		*raft.Persister
	maxraftstate	int
	configs			[]Config // indexed by config num
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
	ClerkId	int64
	Seq		int
}

type Join struct {
	// Servers map[int][]string // new GID -> servers mappings
	GIDs	[]int
	Servers	[][]string
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
        msg := <- sc.applyCh
		// fmt.Printf("Execute!\n")

        sc.mu.Lock()
        if msg.CommandValid == true {
			op, _ := msg.Command.(Op)
            info, ok := sc.clerkInfo[op.ClerkId]
            if ok == false || op.Seq > info.Seq {
				var result Config
				var config Config
				if op.Type != 3 {
					config = Config{}
					config.Num = sc.configs[len(sc.configs)-1].Num + 1
					config.Shards = [NShards]int{}
					for i := 0; i < NShards; i++ {
						config.Shards[i] = sc.configs[len(sc.configs)-1].Shards[i]
					}
					config.Groups = make(map[int][]string)
					for gid, server := range sc.configs[len(sc.configs)-1].Groups {
						config.Groups[gid] = server
					}
				}
				if op.Type == 0 {
					args, _ := op.Args.(Join)
					if sc.isleader == true {
						fmt.Printf("%v %v % v\n", op.Type, args.GIDs, args.Servers)
					}
					for i := 0; i < len(args.GIDs); i++ {
						gid := args.GIDs[i]
						/*
						if _, ok := config.Groups[gid]; ok == true {
							continue
						}
						*/
						// fmt.Printf("%v!!!\n", args.Servers[i])
						config.Groups[gid] = args.Servers[i]
						// fmt.Printf("%v!!!\n", args.Servers[i])
						if config.Shards[0] == 0 {
							for i := 0; i < NShards; i++ {
								config.Shards[i] = gid
							}
							continue
						}

						for true {
							cnt := map[int]int{}
							maxCnt := 0
							target := -1
							for shard, id := range config.Shards {
								cnt[id] = cnt[id] + 1
								if cnt[id] > maxCnt {
									maxCnt = cnt[id]
									target = shard
								}
							}
							if maxCnt - cnt[gid] > 1 {
								config.Shards[target] = gid
							} else {
								/*
								if cnt[gid] == 0 {
									delete(config.Groups, gid)
								}
								*/
								break
							}
						}
					}
					sc.configs = append(sc.configs, config)
				} else if op.Type == 1 {
					args, _ := op.Args.(Leave)
					if sc.isleader == true {
						fmt.Printf("%v % v\n", op.Type, args.GIDs)
					}
					for _, gid := range args.GIDs {
						/*
						if _, ok := config.Groups[gid]; ok == false {
							continue
						}
						*/
						delete(config.Groups, gid)
						for true {
							cnt := map[int]int{}
							for _, id := range config.Shards {
								cnt[id] = cnt[id] + 1
							}
							if cnt[gid] == 0 {
								break
							} else if cnt[gid] == NShards {
								for i := 0; i < NShards; i++ {
									config.Shards[i] = 0
								}
								break
							}
							minCnt := NShards + 1
							minGid := -1
							target := -1
							for shard, id := range config.Shards {
								if id == gid {
									target = shard
								} else if cnt[id] < minCnt {
									minCnt = cnt[id]
									minGid = id
								}
							}
							config.Shards[target] = minGid
						}
					}
					sc.configs = append(sc.configs, config)
				} else if op.Type == 2 {
					args, _ := op.Args.(Move)
					if _, ok := config.Groups[args.GID]; ok == true {
						config.Shards[args.Shard] = args.GID
						sc.configs = append(sc.configs, config)
						// continue
					}
				} else if op.Type == 3 {
					args, _ := op.Args.(Query)
					if args.Num == -1 || args.Num >= len(sc.configs) {
						result = sc.configs[len(sc.configs)-1]
					} else {
						result = sc.configs[args.Num]
					}
					// fmt.Printf("%v %v %v\n", args.Num, result.Num, len(sc.configs))
				}
				if sc.isleader == true && op.Type != 3 {
					fmt.Printf("%v\n", config)
				}
				sc.clerkInfo[op.ClerkId] = Info{op.Seq, result}

				if wait, ok := sc.notify[op.ClerkId]; ok == true && wait.seq == op.Seq {
					wait.cond.Broadcast()
				}
				//fmt.Printf("%v\n", len(sc.configs))
				//fmt.Printf("%v\n", sc.configs[len(sc.configs)-1].Num)
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
            var clerkInfo map[int64]Info
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
	reply.WrongLeader = true
	sc.mu.Lock()
    if sc.isleader == false {
        sc.mu.Unlock()
        return
    }

    if info, ok := sc.clerkInfo[args.ClerkId]; ok == true && args.Seq <= info.Seq {
		reply.WrongLeader = false
        sc.mu.Unlock()
        return
    }

    if _, ok := sc.notify[args.ClerkId]; ok == true {
        sc.mu.Unlock()
        return
    }

    cond := sync.NewCond(&sc.mu)
    sc.notify[args.ClerkId] = Wait{args.Seq, cond}

	if seq, ok := sc.hasStart[args.ClerkId]; ok == false || seq < args.Seq {
        sc.hasStart[args.ClerkId] = args.Seq
        op := Op{}
		op.Type = 0
		gids := []int{}
		servers := [][]string{}
		for gid, server := range args.Servers {
			gids = append(gids, gid)
			servers = append(servers, server)
		}
        op.Args = Join{gids, servers}
        op.ClerkId = args.ClerkId
        op.Seq = args.Seq

        sc.rf.Start(op)
    }

    go func() {
        time.Sleep(500 * time.Millisecond)
        cond.Broadcast()
    } ()

    cond.Wait()

	delete(sc.notify, args.ClerkId)

    if info, ok := sc.clerkInfo[args.ClerkId]; ok == true && args.Seq <= info.Seq {
		reply.WrongLeader = false
        sc.mu.Unlock()
        return
    }

    sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.WrongLeader = true
	sc.mu.Lock()
    if sc.isleader == false {
        sc.mu.Unlock()
        return
    }

    if info, ok := sc.clerkInfo[args.ClerkId]; ok == true && args.Seq <= info.Seq {
		reply.WrongLeader = false
        sc.mu.Unlock()
        return
    }

    if _, ok := sc.notify[args.ClerkId]; ok == true {
        sc.mu.Unlock()
        return
    }

    cond := sync.NewCond(&sc.mu)
    sc.notify[args.ClerkId] = Wait{args.Seq, cond}

	if seq, ok := sc.hasStart[args.ClerkId]; ok == false || seq < args.Seq {
        sc.hasStart[args.ClerkId] = args.Seq
        op := Op{}
		op.Type = 1
        op.Args = Leave{args.GIDs}
        op.ClerkId = args.ClerkId
        op.Seq = args.Seq

        sc.rf.Start(op)
    }

    go func() {
        time.Sleep(500 * time.Millisecond)
        cond.Broadcast()
    } ()

    cond.Wait()

	delete(sc.notify, args.ClerkId)

    if info, ok := sc.clerkInfo[args.ClerkId]; ok == true && args.Seq <= info.Seq {
		reply.WrongLeader = false
        sc.mu.Unlock()
        return
    }

    sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.WrongLeader = true
	sc.mu.Lock()
    if sc.isleader == false {
        sc.mu.Unlock()
        return
    }

    if info, ok := sc.clerkInfo[args.ClerkId]; ok == true && args.Seq <= info.Seq {
		reply.WrongLeader = false
        sc.mu.Unlock()
        return
    }

    if _, ok := sc.notify[args.ClerkId]; ok == true {
        sc.mu.Unlock()
        return
    }

    cond := sync.NewCond(&sc.mu)
    sc.notify[args.ClerkId] = Wait{args.Seq, cond}

	if seq, ok := sc.hasStart[args.ClerkId]; ok == false || seq < args.Seq {
        sc.hasStart[args.ClerkId] = args.Seq
        op := Op{}
		op.Type = 2
        op.Args = Move{args.Shard, args.GID}
        op.ClerkId = args.ClerkId
        op.Seq = args.Seq

        sc.rf.Start(op)
    }

    go func() {
        time.Sleep(500 * time.Millisecond)
        cond.Broadcast()
    } ()

    cond.Wait()

	delete(sc.notify, args.ClerkId)

    if info, ok := sc.clerkInfo[args.ClerkId]; ok == true && args.Seq <= info.Seq {
		reply.WrongLeader = false
        sc.mu.Unlock()
        return
    }

    sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.WrongLeader = true
	sc.mu.Lock()
    if sc.isleader == false {
        sc.mu.Unlock()
        return
    }

    if info, ok := sc.clerkInfo[args.ClerkId]; ok == true && args.Seq <= info.Seq {
		reply.WrongLeader = false
		reply.Config = info.Result
        sc.mu.Unlock()
        return
    }

    if _, ok := sc.notify[args.ClerkId]; ok == true {
        sc.mu.Unlock()
        return
    }

    cond := sync.NewCond(&sc.mu)
    sc.notify[args.ClerkId] = Wait{args.Seq, cond}

	if seq, ok := sc.hasStart[args.ClerkId]; ok == false || seq < args.Seq {
        sc.hasStart[args.ClerkId] = args.Seq
        op := Op{}
		op.Type = 3
        op.Args = Query{args.Num}
        op.ClerkId = args.ClerkId
        op.Seq = args.Seq

        sc.rf.Start(op)
    }

    go func() {
        time.Sleep(500 * time.Millisecond)
        cond.Broadcast()
    } ()

    cond.Wait()

	delete(sc.notify, args.ClerkId)

    if info, ok := sc.clerkInfo[args.ClerkId]; ok == true && args.Seq <= info.Seq {
		reply.WrongLeader = false
		reply.Config = info.Result
        sc.mu.Unlock()
        return
    }

    sc.mu.Unlock()
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
    z := atomic.LoadInt32(&sc.dead)
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
	labgob.Register(Join{})
	labgob.Register(Leave{})
	labgob.Register(Move{})
	labgob.Register(Query{})
	sc.persister = persister
	sc.maxraftstate = -1
    r := bytes.NewBuffer(sc.persister.ReadSnapshot())
    d := labgob.NewDecoder(r)
	var configs []Config
    var clerkInfo map[int64]Info
    if d.Decode(&configs) == nil && d.Decode(&clerkInfo) == nil {
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
		sc.clerkInfo = make(map[int64]Info)
    }
    sc.notify = make(map[int64]Wait)
    sc.hasStart = make(map[int64]int)
    sc.isleader = false
    sc.term = -1

	/// fmt.Printf("111\n")
	/*
	for x, y := range sc.configs[0].Shards {
		fmt.Printf("%v %v\n", x, y)
	}
	*/
    go sc.Execute()
    go sc.CheckState()

	return sc
}
