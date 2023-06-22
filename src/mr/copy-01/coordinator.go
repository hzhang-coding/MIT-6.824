package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"
// import "time"

var mutex sync.Mutex

type Coordinator struct {
	// Your definitions here.
	// Files []string
	FileToNum map[string]int
	Total int
	NReduce int
	// FilesStates map[string][]int // 0:not completed, 1:in progress, 2:completed; pid
	// WorkingFiles map[string]int // pid
	// CompletedFiles map[string]int // pid
	// WorkingFiles map[string]int // pid
	// CompletedFiles map[string]int // pid

	MWaiting []string
	MWorking map[string]int
	MCompleted map[string]int
	MCnt map[string]int

	RWaiting []int
	RWorking map[int]int
	RCompleted map[int]int
	RCnt map[int]int
	// Tasks []int
	State int // 0:map, 1:reduce
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandOut(args *HandOutArgs, reply *HandOutReply) error {
	reply.Type = -1
	reply.NReduce = c.NReduce
	mutex.Lock();

	if c.State == 0 {
		n := len(c.MWaiting)
		if n > 0 {
			file := c.MWaiting[n - 1]
			c.MWaiting = c.MWaiting[: n - 1]
			c.MWorking[file] = args.Pid 
			c.MCnt[file] = 0
			reply.Type = 0
			reply.File = file
			reply.Num = c.FileToNum[file]
			reply.NReduce = c.NReduce
		}
	} else {
		n := len(c.RWaiting)
		if n > 0 {
			idx := c.RWaiting[n - 1]
			c.RWaiting = c.RWaiting[: n - 1]
			c.RWorking[idx] = args.Pid
			c.RCnt[idx] = 0
			reply.Type = 1
			reply.Idx = idx
			reply.Total = c.Total 
		}
	}

	mutex.Unlock()

	return nil
}

func (c *Coordinator) Completed(args *CompletedArgs, reply *CompletedReply) error {
	if args.Type == 0 {
		mutex.Lock()
		if pid, ok := c.MWorking[args.File]; ok {
			c.MCompleted[args.File] = pid
			delete(c.MWorking, args.File)
			delete(c.MCnt, args.File)
		}

		if len(c.MCompleted) == c.Total {
			c.State = 1
		}
		mutex.Unlock()

		reply.Flag = true
	} else if args.Type == 1 {
		mutex.Lock()
		if pid, ok := c.RWorking[args.Idx]; ok {
			c.RCompleted[args.Idx] = pid
			delete(c.RWorking, args.Idx)
			delete(c.RCnt, args.Idx)
		}
		mutex.Unlock()

		reply.Flag = true
	}

	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	mutex.Lock()

	if c.State == 0 {
		files := []string{}
		for file, t := range c.MCnt {
			if t >= 10 {
				c.MWaiting = append(c.MWaiting, file)
				delete(c.MWorking, file)
				files = append(files, file)
			}
			c.MCnt[file] = t + 1
		}

		for _, file := range files {
			delete(c.MCnt, file)
		}
	} else {
		idxes := []int{}
		for idx, t := range c.RCnt {
			if t >= 10 {
				c.RWaiting = append(c.RWaiting, idx)
				delete(c.RWorking, idx)
				idxes = append(idxes, idx)
			}
			c.RCnt[idx] = t + 1
		}

		for _, idx := range idxes {
			delete(c.RCnt, idx)
		}
	}

	ret = c.State == 1 && len(c.RWaiting) + len(c.RWorking) == 0 
	mutex.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Print(files)
	fmt.Print("\n")
	c := Coordinator{}

	// Your code here.
	c.FileToNum = make(map[string]int)
	for i, file := range files {
		c.FileToNum[file] = i
	}
	c.Total = len(files)
	c.NReduce = nReduce
	c.MWaiting = files
	c.MWorking = make(map[string]int)
	c.MCompleted = make(map[string]int)
	c.MCnt = make(map[string]int)
	// c.RWaiting = 
	for i := 0; i < nReduce; i++ {
		c.RWaiting = append(c.RWaiting, i)
	}
	c.RWorking = make(map[int]int)
	c.RCompleted = make(map[int]int)
	c.RCnt = make(map[int]int)
	c.State = 0

	c.server()
	return &c
}
