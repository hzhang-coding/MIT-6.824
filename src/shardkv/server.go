package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "6.824/shardctrler"
import "time"
import "sync/atomic"
import "bytes"
//import "fmt"



type OutResquest struct {
	Key         string
    Value       string // if exists
    ClerkId     int64
    Seq         int
	Shard       int
}

type InResquest struct {
	Config      shardctrler.Config
    Shard       int
    Version     int
    Data        map[string]string
    ClerkInfo   map[int64]Info
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type	int // 0:Get, 1:Put, 2:Append, 3:Config
	Args	interface{}
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
	persister		*raft.Persister
    database		[shardctrler.NShards]map[string]string
    clerkInfo		[shardctrler.NShards]map[int64]Info
    notify			map[int64]Wait
    isleader		bool
    term			int
	latestVersion	int
	configs			map[int]shardctrler.Config
	exist			[shardctrler.NShards]bool
	versions		[shardctrler.NShards]int
}

func (kv *ShardKV) CheckState() {
    for kv.killed() == false {
        term, isleader := kv.rf.GetState()

        kv.mu.Lock()
		if isleader == true && kv.isleader == false {
			go kv.CheckNewConfig(term)
			for i := 0; i < shardctrler.NShards; i++ {
				go kv.CheckMigration(i, term)
			}
		}

        kv.isleader = isleader
        kv.term = term
        kv.mu.Unlock()

        time.Sleep(10 * time.Millisecond)
    }
}

func (kv *ShardKV) CheckNewConfig(term int) {
    for kv.killed() == false {
		kv.mu.Lock()
		if term != kv.term {
			kv.mu.Unlock()
			return
		}

		num := kv.latestVersion + 1
		kv.mu.Unlock()

		config := kv.mck.Query(num)

		kv.mu.Lock()
		if config.Num == kv.latestVersion + 1 {
			op := Op{}
			op.Type = 3
			req := InResquest{}
			req.Config = config
			op.Args = req
			kv.mu.Unlock()

			kv.rf.Start(op)
			time.Sleep(20 * time.Millisecond)
		} else {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

type MigrationArgs struct {
	Shard		int
	Version		int
	Data		map[string]string
	ClerkInfo	map[int64]Info
}

type MigrationReply struct {
	Success	bool
}

func (kv *ShardKV) CheckMigration(shard int, term int) {
    for kv.killed() == false {
		kv.mu.Lock()
		if term != kv.term {
			kv.mu.Unlock()
			return
		}

		version := kv.versions[shard]
		if version < kv.latestVersion && kv.exist[shard] == true && kv.configs[version+1].Shards[shard] != kv.gid { 
			args := MigrationArgs{}
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
			gid := kv.configs[version+1].Shards[shard]
			servers := kv.configs[version+1].Groups[gid]
			kv.mu.Unlock()

			success := false
			for success == false {
				for si := 0; si < len(servers); si++ {
					srv := kv.make_end(servers[si])
					var reply MigrationReply
					ok := srv.Call("ShardKV.Migration", &args, &reply)
					if ok && reply.Success == true {
						success = true
						break
					}
				}
			}

			kv.mu.Lock()
			op := Op{}
			op.Type = 4
			req := InResquest{}
			req.Shard = shard
			req.Version = version
			op.Args = req
			kv.mu.Unlock()

			kv.rf.Start(op)
		} else {
			kv.mu.Unlock()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) Migration(args *MigrationArgs, reply *MigrationReply) {
	reply.Success = false

	kv.mu.Lock()
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
	req := InResquest{}
	req.Shard = args.Shard
	req.Version = args.Version
	req.Data = args.Data
	req.ClerkInfo = args.ClerkInfo
	op.Args = req
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
				minVersion := kv.latestVersion
				for _, version := range kv.versions {
					if version < minVersion {
						minVersion = version
					}
				}
				del := []int{}
				for version, _ := range kv.configs {
					if version <= minVersion {
						del = append(del, version)
					}
				}
				for _, version := range del {
					delete(kv.configs, version)
				}

                w := new(bytes.Buffer)
                e := labgob.NewEncoder(w)
                e.Encode(kv.database)
                e.Encode(kv.clerkInfo)
				e.Encode(kv.latestVersion)
				e.Encode(kv.configs)
				e.Encode(kv.exist)
				e.Encode(kv.versions)
                snapshot := w.Bytes()

                kv.rf.Snapshot(msg.CommandIndex, snapshot)
            }
		} else if msg.SnapshotValid == true {
            r := bytes.NewBuffer(kv.persister.ReadSnapshot())
            d := labgob.NewDecoder(r)
			var database [shardctrler.NShards]map[string]string
			var clerkInfo [shardctrler.NShards]map[int64]Info
			var latestVersion int
			var configs map[int]shardctrler.Config
			var exist [shardctrler.NShards]bool
			var versions [shardctrler.NShards]int
			if d.Decode(&database) == nil && d.Decode(&clerkInfo) == nil && d.Decode(&latestVersion) == nil && d.Decode(&configs) == nil && d.Decode(&exist) == nil && d.Decode(&versions) == nil {
				kv.database = database
				kv.clerkInfo = clerkInfo
				kv.latestVersion = latestVersion
				kv.configs = configs
				kv.exist = exist
				kv.versions = versions
			}
        }
        kv.mu.Unlock()
    }
}

func (kv *ShardKV) Execute(op Op) {
	if op.Type < 3 {
		req, _ := op.Args.(OutResquest)
		if kv.exist[req.Shard] == true && kv.versions[req.Shard] == kv.latestVersion {
			info, ok := kv.clerkInfo[req.Shard][req.ClerkId]
			if ok == false || req.Seq > info.Seq {
				if op.Type == 0 {
					kv.clerkInfo[req.Shard][req.ClerkId] = Info{req.Seq, kv.database[req.Shard][req.Key]}
				} else if op.Type == 1 {
					kv.database[req.Shard][req.Key] = req.Value
					kv.clerkInfo[req.Shard][req.ClerkId] = Info{req.Seq, ""}
				} else if op.Type == 2 {
					kv.database[req.Shard][req.Key] = kv.database[req.Shard][req.Key] + req.Value
					kv.clerkInfo[req.Shard][req.ClerkId] = Info{req.Seq, ""}
				}
			}
		}
		if wait, ok := kv.notify[req.ClerkId]; ok == true && wait.seq == req.Seq {
			wait.cond.Broadcast()
		}
	} else if op.Type == 3 {
		req, _ := op.Args.(InResquest)
		if req.Config.Num == kv.latestVersion + 1 {
			for i := 0; i < shardctrler.NShards; i++ {
				if kv.versions[i] != kv.latestVersion {
					continue
				}
				if kv.versions[i] <= 0 {
					kv.versions[i]++
					if req.Config.Shards[i] == kv.gid {
						kv.exist[i] = true
						kv.database[i] = make(map[string]string)
						kv.clerkInfo[i] = make(map[int64]Info)
					}
				} else if kv.exist[i] == false {
					if req.Config.Shards[i] != kv.gid {
						kv.versions[i]++
					}
				} else if kv.exist[i] == true {
					if req.Config.Shards[i] == kv.gid {
						kv.versions[i]++
					}
				}
			}
			kv.latestVersion++
			kv.configs[kv.latestVersion] = req.Config
		}
	} else if op.Type == 4 {
		req, _ := op.Args.(InResquest)
		if kv.versions[req.Shard] == req.Version {
			kv.versions[req.Shard]++
			kv.exist[req.Shard] = false
			kv.database[req.Shard] = nil
			kv.clerkInfo[req.Shard] = nil
			for i := kv.versions[req.Shard]; i < kv.latestVersion; i++ {
				if kv.configs[i+1].Shards[req.Shard] != kv.gid {
					kv.versions[req.Shard]++
				} else {
					break;
				}
			}
		}
	} else if op.Type == 5 {
		req, _ := op.Args.(InResquest)
		if kv.versions[req.Shard] == req.Version {
			kv.versions[req.Shard]++
			kv.exist[req.Shard] = true
			kv.database[req.Shard] = make(map[string]string)
			for key, value := range req.Data {
				kv.database[req.Shard][key] = value
			}
			kv.clerkInfo[req.Shard] = make(map[int64]Info)
			for clerkId, info := range req.ClerkInfo {
				kv.clerkInfo[req.Shard][clerkId] = info
			}
			for i := kv.versions[req.Shard]; i < kv.latestVersion; i++ {
                if kv.configs[i+1].Shards[req.Shard] == kv.gid {
                    kv.versions[req.Shard]++
                } else {
                    break;
                }
            }
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.exist[args.Shard] == false || kv.versions[args.Shard] != kv.latestVersion {
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
		reply.Err = OK
		reply.Value = info.Result
        kv.mu.Unlock()
        return
    }

    if _, ok := kv.notify[args.ClerkId]; ok == true {
        kv.mu.Unlock()
        return
    }

    cond := sync.NewCond(&kv.mu)
	kv.notify[args.ClerkId] = Wait{args.Seq, cond}

	op := Op{}
	op.Type = 0
	req := OutResquest{}
	req.Key = args.Key
	req.Value = ""
	req.ClerkId = args.ClerkId
	req.Seq = args.Seq
	req.Shard = args.Shard
	op.Args = req
	kv.rf.Start(op)

    term := kv.term

    go func() {
        time.Sleep(500 * time.Millisecond)
        cond.Broadcast()
    } ()

    cond.Wait()

	delete(kv.notify, args.ClerkId)

	if kv.exist[args.Shard] == false || kv.versions[args.Shard] != kv.latestVersion {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

    if info, ok := kv.clerkInfo[args.Shard][args.ClerkId]; ok == true && args.Seq <= info.Seq {
		reply.Err = OK
		reply.Value = info.Result
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
	kv.mu.Lock()
	if kv.exist[args.Shard] == false || kv.versions[args.Shard] != kv.latestVersion {
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
		reply.Err = OK
        kv.mu.Unlock()
        return
    }

    if _, ok := kv.notify[args.ClerkId]; ok == true {
        kv.mu.Unlock()
        return
    }

    cond := sync.NewCond(&kv.mu)
	kv.notify[args.ClerkId] = Wait{args.Seq, cond}

	op := Op{}
	if args.Op == "Put" {
		op.Type = 1
	} else {
		op.Type = 2
	}
	req := OutResquest{}
	req.Key = args.Key
	req.Value = args.Value
	req.ClerkId = args.ClerkId
	req.Seq = args.Seq
	req.Shard = args.Shard
	op.Args = req
	kv.rf.Start(op)

    term := kv.term

    go func() {
        time.Sleep(500 * time.Millisecond)
        cond.Broadcast()
    } ()

    cond.Wait()

	delete(kv.notify, args.ClerkId)

	if kv.exist[args.Shard] == false || kv.versions[args.Shard] != kv.latestVersion {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

    if info, ok := kv.clerkInfo[args.Shard][args.ClerkId]; ok == true && args.Seq <= info.Seq {
		reply.Err = OK
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
	labgob.Register(InResquest{})
	labgob.Register(OutResquest{})

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
	var latestVersion int
	var configs map[int]shardctrler.Config
	var exist [shardctrler.NShards]bool
	var versions [shardctrler.NShards]int
    if d.Decode(&database) == nil && d.Decode(&clerkInfo) == nil && d.Decode(&latestVersion) == nil && d.Decode(&configs) == nil && d.Decode(&exist) == nil && d.Decode(&versions) == nil {
        kv.database = database
        kv.clerkInfo = clerkInfo
		kv.latestVersion = latestVersion
		kv.configs = configs
		kv.exist = exist
		kv.versions = versions
    } else {
        kv.database = [shardctrler.NShards]map[string]string{}
        kv.clerkInfo = [shardctrler.NShards]map[int64]Info{}
		kv.latestVersion = -1
		kv.configs = map[int]shardctrler.Config{}
		kv.exist = [shardctrler.NShards]bool{}
		kv.versions = [shardctrler.NShards]int{}
		for i := 0; i < shardctrler.NShards; i++ {
			kv.exist[i] = false
			kv.versions[i] = -1
		}
	}
    kv.notify = make(map[int64]Wait)
    kv.isleader = false
    kv.term = -1

    go kv.ReceiveMsg()
    go kv.CheckState()

	return kv
}
