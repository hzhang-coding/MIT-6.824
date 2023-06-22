package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type HandOutArgs struct {
	Pid int
}

type HandOutReply struct {
	Type int // -1:No task, 0:Map, 1:Reduce

	// Map phase
	File string // Map filename
	Num int
	NReduce int // nums of reduce tasks

	// Reduce phase
	Idx int // Reduce index
	Total int // nums of input files
}

type CompletedArgs struct {
	Type int // -1:No task, 0:Map, 1:Reduce
	File string
	Idx int
}

type CompletedReply struct {
	Flag bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
