package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
// import "fmt"
import "time"

var mutex sync.Mutex

type Coordinator struct {
	// Your definitions here.
	NReduce int

	Files []string
	MWaiting []int
	MWorking map[int]int // timestamp

	RWaiting []int
	RWorking map[int]int // timestamp

	State int // 0:Map phase, 1:Reduce phase, 2:Finish, 3:No waiting task
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Schedule(args *ScheduleArgs, reply *ScheduleReply) error {
	reply.Type = 3 // No waiting task

	mutex.Lock();

	reply.NReduce = c.NReduce

	if c.State == 0 {
		n := len(c.MWaiting)
		if n > 0 {
			task := c.MWaiting[n - 1]
			c.MWaiting = c.MWaiting[: n - 1]
			c.MWorking[task] = 0

			reply.Type = 0
			reply.Task = task
			reply.File = c.Files[task]
		}
	} else if c.State == 1 {
		n := len(c.RWaiting)
		if n > 0 {
			task := c.RWaiting[n - 1]
			c.RWaiting = c.RWaiting[: n - 1]
			c.RWorking[task] = 0

			reply.Type = 1
			reply.Task = task
			reply.Total = len(c.Files)
		}
	} else if c.State == 2 {
		reply.Type = 2 // Finish
	}

	mutex.Unlock()

	return nil
}

func (c *Coordinator) Commit(args *CommitArgs, reply *CommitReply) error {
	if args.Type == 0 {
		mutex.Lock()

		if _, ok := c.MWorking[args.Task]; ok {
			delete(c.MWorking, args.Task)
		}

		if len(c.MWaiting) + len(c.MWorking) == 0 {
			c.State = 1
		}

		mutex.Unlock()
	} else if args.Type == 1 {
		mutex.Lock()

		if _, ok := c.RWorking[args.Task]; ok {
			delete(c.RWorking, args.Task)
		}

		if len(c.RWaiting) + len(c.RWorking) == 0 {
			c.State = 2
		}

		mutex.Unlock()
	}

	return nil
}

func (c *Coordinator) CheckCrash() {
	for c.Done() == false {
		time.Sleep(time.Second)

		mutex.Lock()

		if c.State == 0 {
			remove := []int{}
			for task, t := range c.MWorking {
				if t >= 10 {
					remove = append(remove, task)
				}
				c.MWorking[task] = t + 1
			}

			for _, task := range remove {
				c.MWaiting = append(c.MWaiting, task)
				delete(c.MWorking, task)
			}
		} else {
			remove := []int{}
			for task, t := range c.RWorking {
				if t >= 10 {
					remove = append(remove, task)
				}
				c.RWorking[task] = t + 1
			}

			for _, task := range remove {
				c.RWaiting = append(c.RWaiting, task)
				delete(c.RWorking, task)
			}
		}

		mutex.Unlock()
	}
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

	ret = (c.State == 2)

	mutex.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// fmt.Print(files)
	// fmt.Print("\n")
	c := Coordinator{}

	// Your code here.
	c.NReduce = nReduce

	c.Files = files
	n := len(files)
	c.MWaiting = make([]int, n)
	for i := 0; i < n; i++ {
		c.MWaiting[i] = n - 1 - i
	}
	c.MWorking = make(map[int]int)

	c.RWaiting = make([]int, nReduce)
	for i := 0; i < nReduce; i++ {
		c.RWaiting[i] = nReduce - 1 - i
	}
	c.RWorking = make(map[int]int)

	c.State = 0

	c.server()

	go c.CheckCrash()

	return &c
}
