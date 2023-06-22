package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "fmt"
// import "sync/atomic"
import math_rand "math/rand"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId		int64
	seq			int
	leaderId	int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.seq = 0
	// atomic.StoreInt32(&ck.leaderId, rand.Int() % len(servers))
	math_rand.Seed(time.Now().UnixNano())
	ck.leaderId = math_rand.Int() % len(servers)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{}
	args.Key = key
	args.ClerkId = ck.clerkId
	args.Seq = ck.seq
	ck.seq++

	fail := 0
	for true {
		// leaderId := atomic.LoadInt32(&ck.leaderId)
		reply := GetReply{}
		leaderId := ck.leaderId

		fmt.Printf("%v send Get seq=%v to server %v\n", ck.clerkId, args.Seq, leaderId)
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)

		if ok == true {
			// fmt.Printf("%v\n", reply.Err)
			if reply.Err == "" {
				// fmt.Printf("done Get seq=%v\n", args.Seq)
				return reply.Value
			} else if reply.Err == "I'm not leader" {
				// ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				ck.leaderId = math_rand.Int() % len(ck.servers)
			} else if reply.Err == "repeat" {
				fmt.Printf("repeat\n")
			}
			fail = 0
		} else {
			// ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			fail++
			if fail >= 2 {
				ck.leaderId = math_rand.Int() % len(ck.servers)
			}
		}
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClerkId = ck.clerkId
	args.Seq = ck.seq
	ck.seq++

	fail := 0
	for true {
		// leaderId := atomic.LoadInt32(&ck.leaderId)
		reply := PutAppendReply{}
		leaderId := ck.leaderId

		fmt.Printf("%v send PutAppend seq=%v to server %v\n",ck.clerkId, args.Seq, leaderId)
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)

		if ok == true {
			// fmt.Printf("%v\n", reply.Err)
			if reply.Err == "" {
				// fmt.Printf("done PutAppend seq=%v\n", args.Seq)
				return
			} else if reply.Err == "I'm not leader" {
				// ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				ck.leaderId = math_rand.Int() % len(ck.servers)
			} else if reply.Err == "repeat" {
				return
			}
			fail = 0
		} else {
			// ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			fail++
			if fail >= 2 {
				ck.leaderId = math_rand.Int() % len(ck.servers)
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
