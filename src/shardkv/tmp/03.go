package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "6.824/shardctrler"
import "time"
import "sync/atomic"
import "bytes"
import "fmt"




type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type		int // 0:Get, 1:Put, 2:Append, 3:Config
    Key			string
    Value		string // if exists
    ClerkId		int64
    Seq			int
	Config		shardctrler.Config
	Shard		int
	Version		int
	Data		map[string]string
	ClerkInfo	map[int64]Info
}

type Info struct {
    Seq     int
    Result  string // if exists
}

type Wait struct {
    seq     int
    cond    *sync.Cond
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead		 int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	mck			 *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister   *raft.Persister
    database    [shardctrler.NShards]map[string]string
    clerkInfo   [shardctrler.NShards]map[int64]Info
    notify      map[int64]Wait
    hasStart    map[int64]int
    isleader    bool
    term        int
	configs		[]shardctrler.Config
	exist		[shardctrler.NShards]bool
	versions	[shardctrler.NShards]int
}

func (kv *ShardKV) CheckState() {
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

func (kv *ShardKV) Reconfiguration() {
    for kv.killed() == false {
		kv.mu.Lock()
		if kv.isleader == true {
			num := len(kv.configs)
			kv.mu.Unlock()

			config := kv.mck.Query(num)

			kv.mu.Lock()
			if config.Num == len(kv.configs) {
				op := Op{}
				op.Type = 3
				op.Config = config 
				kv.mu.Unlock()

				kv.rf.Start(op)
				time.Sleep(20 * time.Millisecond)
			} else {
				kv.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
			}
		} else {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

type MigrateArgs struct {
	Shard		int
	Version		int
	Data		map[string]string
	ClerkInfo	map[int64]Info
}

type MigrateReply struct {
	Success	bool
}

func (kv *ShardKV) Update(shard int) {
    for kv.killed() == false {
		kv.mu.Lock()
		version := kv.versions[shard]
		if kv.isleader == true && version < len(kv.configs)-1 && kv.exist[shard] == true && kv.configs[version+1].Shards[shard] != kv.gid { 
			args := MigrateArgs{}
			args.Shard = shard
			args.Version = version
			args.Data = make(map[string]string)
			for key, value := range kv.database[shard] {
				args.Data[key] = value
			}
			args.ClerkInfo = make(map[int64]Info)
			for clerkId, info := range kv.clerkInfo[shard] {
				args.ClerkInfo[clerkId] = info
			}
			kv.mu.Unlock()

			gid := kv.configs[version+1].Shards[shard]
			servers := kv.configs[version+1].Groups[gid]
			success := false
			for success == false {
				for si := 0; si < len(servers); si++ {
					srv := kv.make_end(servers[si])
					var reply MigrateReply
					ok := srv.Call("ShardKV.Migrate", &args, &reply)
					if ok && reply.Success == true {
						success = true
						break
					}
				}
				if success == false {
					time.Sleep(10 * time.Millisecond)
				}
			}

			kv.mu.Lock()
			op := Op{}
			op.Type = 4
			op.Shard = shard
			op.Version = version
			kv.rf.Start(op)
			// kv.mu.Unlock()

			/*
			duetime := time.Now().Add(200 * time.Millisecond)
            time.Sleep(20 * time.Millisecond)
            for time.Now().Before(duetime) {
                kv.mu.Lock()
                if kv.versions[shard] == version + 1 {
                    kv.mu.Unlock()
                    break
                }
                kv.mu.Unlock()
                time.Sleep(20 * time.Millisecond)
            }
			*/
		}
		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	reply.Success = false
	if kv.isleader == false {
		kv.mu.Unlock()
		return
	}

	if args.Version < kv.versions[args.Shard] {
		reply.Success = true
		kv.mu.Unlock()
		return
	}

	op := Op{}
	op.Type = 5
	op.Shard = args.Shard
	op.Version = args.Version
	op.Data = args.Data
	op.ClerkInfo = args.ClerkInfo
	kv.mu.Unlock()

	kv.rf.Start(op)

	duetime := time.Now().Add(500 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	for time.Now().Before(duetime) {
		kv.mu.Lock()
		if kv.versions[args.Shard] == args.Version + 1 {
			reply.Success = true
			kv.mu.Unlock()
			break
		}
		if kv.isleader == false {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}


func (kv *ShardKV) ReceiveMsg() {
    for kv.killed() == false {
		msg := <- kv.applyCh

        kv.mu.Lock()
        if msg.CommandValid == true {
            op, _ := msg.Command.(Op)
			kv.Execute(op)

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
                w := new(bytes.Buffer)
                e := labgob.NewEncoder(w)
                e.Encode(kv.database)
                e.Encode(kv.clerkInfo)
                snapshot := w.Bytes()

                kv.rf.Snapshot(msg.CommandIndex, snapshot)
            }
		} else if msg.SnapshotValid == true {
            r := bytes.NewBuffer(kv.persister.ReadSnapshot())
            d := labgob.NewDecoder(r)
			var database [shardctrler.NShards]map[string]string
			var clerkInfo [shardctrler.NShards]map[int64]Info
			var configs []shardctrler.Config
			var exist [shardctrler.NShards]bool
			var versions [shardctrler.NShards]int
			if d.Decode(&database) == nil && d.Decode(&clerkInfo) == nil && d.Decode(&configs) == nil && d.Decode(&exist) == nil && d.Decode(&versions) == nil {
				kv.database = database
				kv.clerkInfo = clerkInfo
				kv.configs = configs
				kv.exist = exist
				kv.versions = versions
			}
        }
        kv.mu.Unlock()
    }
}

func (kv *ShardKV) Execute(op Op) {
	fmt.Printf("Execute %v\n", op.Type)
	if op.Type < 3 {
		info, ok := kv.clerkInfo[op.Shard][op.ClerkId]
		if (ok == false || op.Seq > info.Seq) && kv.exist[op.Shard] == true && kv.versions[op.Shard] == len(kv.configs) - 1 {
		if kv.exist[op.Shard] == true && kv.versions[op.Shard] == len(kv.configs) - 1 {
			info, ok := kv.clerkInfo[op.Shard][op.ClerkId]
			if ok == false || op.Seq > info.Seq {
				fmt.Printf("apply %v\n", op.Type)
				if op.Type == 0 {
					kv.clerkInfo[op.Shard][op.ClerkId] = Info{op.Seq, kv.database[op.Shard][op.Key]}
				} else if op.Type == 1 {
					kv.database[op.Shard][op.Key] = op.Value
					kv.clerkInfo[op.Shard][op.ClerkId] = Info{op.Seq, ""}
				} else if op.Type == 2 {
					kv.database[op.Shard][op.Key] = kv.database[op.Shard][op.Key] + op.Value
					kv.clerkInfo[op.Shard][op.ClerkId] = Info{op.Seq, ""}
				}
			}
		}
		if wait, ok := kv.notify[op.ClerkId]; ok == true && wait.seq == op.Seq {
			wait.cond.Broadcast()
		}
	} else if op.Type == 3 {
		if op.Config.Num == len(kv.configs) {
			kv.configs = append(kv.configs, op.Config)
			fmt.Printf("%v add config %v\n", kv.gid, op.Config.Num)
			fmt.Printf("%v\n", op.Config)
			for i := 0; i < shardctrler.NShards; i++ {
				if kv.versions[i] != op.Config.Num - 1 {
					continue
				}
				if kv.versions[i] <= 0 {
					kv.versions[i]++
					if op.Config.Shards[i] == kv.gid {
						kv.exist[i] = true
						kv.database[i] = make(map[string]string)
						kv.clerkInfo[i] = make(map[int64]Info)
					}
				} else if kv.exist[i] == false {
					if op.Config.Shards[i] != kv.gid {
						kv.versions[i]++
					}
				} else if kv.exist[i] == true {
					if op.Config.Shards[i] == kv.gid {
						kv.versions[i]++
					}
				}
			}
			fmt.Printf("%v %v\n\n", kv.exist, kv.versions)
		}
	} else if op.Type == 4 {
		if kv.versions[op.Shard] == op.Version {
			fmt.Printf("%v %v version++\n", kv.gid, op.Shard)
			kv.versions[op.Shard]++
			kv.exist[op.Shard] = false
			kv.database[op.Shard] = make(map[string]string)
			kv.clerkInfo[op.Shard] = make(map[int64]Info)
			for i := kv.versions[op.Shard]; i + 1 < len(kv.configs); i++ {
				if kv.configs[i+1].Shards[op.Shard] != kv.gid {
					kv.versions[op.Shard]++
				} else {
					break;
				}
			}
			fmt.Printf("%v %v\n\n", kv.exist, kv.versions)
		}
	} else if op.Type == 5 {
		if kv.versions[op.Shard] == op.Version {
			fmt.Printf("%v %v version++\n", kv.gid, op.Shard)
			kv.versions[op.Shard]++
			kv.exist[op.Shard] = true
			// kv.database[op.Shard] = op.Data
			// kv.clerkInfo[op.Shard] = op.ClerkInfo
			kv.database[op.Shard] = make(map[string]string)
			for key, value := range op.Data {
				kv.database[op.Shard][key] = value
			}
			kv.clerkInfo[op.Shard] = make(map[int64]Info)
			for clerkId, info := range op.ClerkInfo {
				kv.clerkInfo[op.Shard][clerkId] = info
			}
			for i := kv.versions[op.Shard]; i + 1 < len(kv.configs); i++ {
                if kv.configs[i+1].Shards[op.Shard] == kv.gid {
                    kv.versions[op.Shard]++
                } else {
                    break;
                }
            }
			fmt.Printf("%v %v\n\n", kv.exist, kv.versions)
		}

	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.exist[args.Shard] == false || kv.versions[args.Shard] != len(kv.configs) - 1 {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

    if kv.isleader == false {
        reply.Err = ErrWrongLeader
        kv.mu.Unlock()
        return
    }

    if info, ok := kv.clerkInfo[args.Shard][args.ClerkId]; ok == true && args.Seq <= info.Seq {
        if args.Seq == info.Seq {
			reply.Err = OK
            reply.Value = info.Result
        }
        kv.mu.Unlock()
        return
    }

    if _, ok := kv.notify[args.ClerkId]; ok == true {
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
		op.Shard = args.Shard

        kv.rf.Start(op)
    }

    term := kv.term

    go func() {
        time.Sleep(500 * time.Millisecond)
        cond.Broadcast()
    } ()

    cond.Wait()

	delete(kv.notify, args.ClerkId)

	if kv.exist[args.Shard] == false || kv.versions[args.Shard] != len(kv.configs) - 1 {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

    if info, ok := kv.clerkInfo[args.Shard][args.ClerkId]; ok == true && args.Seq <= info.Seq {
        if args.Seq == info.Seq {
			reply.Err = OK
            reply.Value = info.Result
        }
        kv.mu.Unlock()
        return
    }

    if term != kv.term || kv.isleader == false {
        reply.Err = ErrWrongLeader
        kv.mu.Unlock()
        return
    }

    kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	fmt.Printf("%v Put\n", kv.gid)
	kv.mu.Lock()
	if kv.exist[args.Shard] == false || kv.versions[args.Shard] != len(kv.configs) - 1 {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

    if kv.isleader == false {
        reply.Err = ErrWrongLeader
        kv.mu.Unlock()
        return
    }

    if info, ok := kv.clerkInfo[args.Shard][args.ClerkId]; ok == true && args.Seq <= info.Seq {
        if args.Seq == info.Seq {
			reply.Err = OK
        }
        kv.mu.Unlock()
        return
    }

    if _, ok := kv.notify[args.ClerkId]; ok == true {
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
        op.Value = ""
        op.ClerkId = args.ClerkId
        op.Seq = args.Seq
		op.Shard = args.Shard

        kv.rf.Start(op)
    }

    term := kv.term

    go func() {
        time.Sleep(500 * time.Millisecond)
        cond.Broadcast()
    } ()

    cond.Wait()

	delete(kv.notify, args.ClerkId)

	if kv.exist[args.Shard] == false || kv.versions[args.Shard] != len(kv.configs) - 1 {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

    if info, ok := kv.clerkInfo[args.Shard][args.ClerkId]; ok == true && args.Seq <= info.Seq {
        if args.Seq == info.Seq {
			reply.Err = OK
        }
        kv.mu.Unlock()
        return
    }

    if term != kv.term || kv.isleader == false {
        reply.Err = ErrWrongLeader
        kv.mu.Unlock()
        return
    }

    kv.mu.Unlock()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
    z := atomic.LoadInt32(&kv.dead)
    return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	kv.persister = persister
    r := bytes.NewBuffer(kv.persister.ReadSnapshot())
    d := labgob.NewDecoder(r)
    var database [shardctrler.NShards]map[string]string
    var clerkInfo [shardctrler.NShards]map[int64]Info
	var configs []shardctrler.Config
	var exist [shardctrler.NShards]bool
	var versions [shardctrler.NShards]int
    if d.Decode(&database) == nil && d.Decode(&clerkInfo) == nil && d.Decode(&configs) == nil && d.Decode(&exist) == nil && d.Decode(&versions) == nil {
        kv.database = database
        kv.clerkInfo = clerkInfo
		kv.configs = configs
		kv.exist = exist
		kv.versions = versions
    } else {
        kv.database = [shardctrler.NShards]map[string]string{}
        kv.clerkInfo = [shardctrler.NShards]map[int64]Info{}
		kv.configs = []shardctrler.Config{}
		kv.exist = [shardctrler.NShards]bool{}
		kv.versions = [shardctrler.NShards]int{}
		for i := 0; i < shardctrler.NShards; i++ {
			kv.exist[i] = false
			kv.versions[i] = -1
		}
	}
    kv.notify = make(map[int64]Wait)
    kv.hasStart = make(map[int64]int)
    kv.isleader = false
    kv.term = -1

	for i := 0; i < shardctrler.NShards; i++ {
		go kv.Update(i)
	}

    go kv.ReceiveMsg()
    go kv.CheckState()
	go kv.Reconfiguration()

	return kv
}
