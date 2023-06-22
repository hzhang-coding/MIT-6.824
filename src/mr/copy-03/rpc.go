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
type ScheduleArgs struct {
	Pid int
}

type ScheduleReply struct {
	Type int // 0:Map phase, 1:Reduce phase, 2:Completed, 3:No task
	Task int
	NReduce int // nums of reduce tasks

	// Map phase
	File string // Map filename

	// Reduce phase
	Total int // nums of input files
}

type CommitArgs struct {
	Type int // 0:Map, 1:Reduce
	Task int
}

type CommitReply struct {
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
