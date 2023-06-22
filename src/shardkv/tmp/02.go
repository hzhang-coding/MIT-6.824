package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "6.824/shardctrler"
import "time"
import "sync/atomic"
import "bytes"




type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    int // 0:Get, 1:Put, 2:Append, 3:Config
    Key     string
    Value   string // if exists
    ClerkId int64
    Seq     int
	Config	shardctrler.Config
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
	configs		int
	exist		[]bool
	versions	[]int
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

			config := kv.mck.Query(-1)

			kv.mu.Lock()
			if config.Num == len(kv.configs) {
				op := Op{}
				op.Type = 3
				op.Config = config 
				kv.rf.Start(op)
			}
		}
		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
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
	mck := shardctrler.MakeClerk(kv.ctrlers)
    for kv.killed() == false {
		kv.mu.Lock()
		if kv.isleader == true {
			version := kv.versions[shard]
			kv.mu.Unlock()

			config := mck.Query(version + 1)
			if config.Num == version + 1 {
				kv.mu.Lock()
				if kv.isleader == true && kv.versions[shard] == version {
					/* should not happen
					if config.Shards[shard] == 0 {
						kv.versions[shard]++
						kv.exist[shard] = false
						kv.database[shard] = make(map[string]string)
					}
					*/
					if version == 0 {
						kv.versions[shard]++
						if config.Shards[shard] == kv.gid {
							kv.exist[shard] = true
						}
					} else if kv.exist[shard] == false {
						if config.Shards[shard] != kv.gid {
							kv.versions[shard]++
						}
					} else if kv.exist[shard] == true {
						if config.Shards[shard] == kv.gid {
							kv.versions[shard]++
						} else {
							// transfer data to config.Shards[shard]
							args := MigrateArgs{}
							args.Shard = shard
							args.Version = version
							args.Data = make(map[string]string]
							for key, value := range kv.database[shard] {
								args.Data[key] = value
							}
							args.ClerkInfo = make(map[int64]Info)
							for clerkId, info := range kv.clerkInfo[shard] {
								args.ClerkInfo[clerkId] = info
							}
							kv.mu.Unlock()

							gid := config.Shards[shard]
							servers := config.Groups[gid]
							success := false
							for success == false {
								for si := 0; si < len(servers); si++ {
									srv := kv.make_end(servers[si])
									var reply PutAppendReply
									ok := srv.Call("ShardKV.Migrate", &args, &reply)
									if ok && reply.Success == true {
										success = true
										break
									}
								}
								// time.Sleep(10 * time.Millisecond)
							}

							kv.mu.Lock()
							kv.versions[shard]++
							kv.exist[shard] = false
							kv.database[shard] = make(map[string]string)
						}
					}
				}
				kv.mu.Unlock()
			}
		} else {
			kv.mu.Unlock()
		}

		time.Sleep(90 * time.Millisecond)
	}
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	reply.Success = false
	if kv.isleader == true {
		if args.Version == kv.versions[args.Shard] {
			kv.versions[args.Shard]++
			kv.database[args.Shard] = args.Data
			kv.clerkInfo[args.Shard] = args.ClerkInfo
			// snapshot
			reply.Success = true
		}
	}
	kv.mu.Unlock()
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
            var database map[string]string
            var clerkInfo map[int64]Info
            if d.Decode(&database) == nil && d.Decode(&clerkInfo) == nil {
                kv.database = database
                kv.clerkInfo = clerkInfo
            }
        }

        kv.mu.Unlock()
    }
}

func (kv *ShardKV) Execute(op Op) {
	if op.Type < 3 {
		info, ok := kv.clerkInfo[op.Shard][op.ClerkId]
		if ok == false || op.Seq > info.Seq {
			if op.Type == 0 {
				kv.clerkInfo[op.Shard][op.ClerkId] = Info{op.Seq, kv.database[op.Shard][op.Key]}
			} else if op.Type == 1 {
				kv.database[op.Shard][op.Key] = op.Value
				kv.clerkInfo[op.ClerkId] = Info{op.Seq, ""}
			} else if op.Type == 2 {
				kv.database[op.Key] = kv.database[op.Key] + op.Value
				kv.clerkInfo[op.ClerkId] = Info{op.Seq, ""}
			}

			if wait, ok := kv.notify[op.ClerkId]; ok == true && wait.seq == op.Seq {
				wait.cond.Broadcast()
			}
		}
	} else if op.Type == 3 {
		if op.Config.Num == len(kv.configs) {
			kv.configs = append(kv.configs, op.Config)
			for i = 0; i < shardctrler.NShards; i++ {
				if kv.versions[i] == 0 {
					kv.versions[i]++
					if config.Shards[i] == kv.gid {
						kv.exist[i] = true
					}
				} else if kv.exist[i] == false {
					if config.Shards[i] != kv.gid {
						kv.versions[i]++
					}
				} else if kv.exist[i] == true {
					if config.Shards[i] == kv.gid {
						kv.versions[i]++
					} else {
					}
				}
			}
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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
    var database map[string]string
    var clerkInfo map[int64]Info
    if d.Decode(&database) == nil && d.Decode(&clerkInfo) == nil {
        kv.database = database
        kv.clerkInfo = clerkInfo
    } else {
        kv.database = make(map[string]string)
        kv.clerkInfo = make(map[int64]Info)
    }
    kv.notify = make(map[int64]Wait)
    kv.hasStart = make(map[int64]int)
    kv.isleader = false
    kv.term = -1
	kv.num = -1

    go kv.ReceiveMsg()
    go kv.CheckState()

	return kv
}
