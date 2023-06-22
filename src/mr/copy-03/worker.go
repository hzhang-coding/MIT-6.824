package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "io/ioutil"
import "encoding/json"
import "strconv"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// CallExample()

	for true {
		flag := CallSchedule(mapf, reducef) // -1:exit, 0:idle, 1:success
		if flag == -1 {
			break // error or completed, so exit
		} else if flag == 0 {
			time.Sleep(time.Second / 10) // no task, sleep for waiting
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallSchedule(mapf func(string, string) []KeyValue,
    reducef func(string, []string) string) int {

	args := ScheduleArgs{}
	args.Pid = os.Getpid()
	reply := ScheduleReply{}

	ok := call("Coordinator.Schedule", &args, &reply)

	if ok == false {
		fmt.Printf("CallSchedule failed!\n")
		return -1 // failed, so exit
	}

	if reply.Type == 0 {
		// fmt.Printf("reply.File %v\n", reply.File)
		filename := reply.File
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))

		nReduce := reply.NReduce
		tempfiles := make([]*os.File, nReduce)
		tempdir := os.TempDir()
		// fmt.Print(tempdir)
		// fmt.Printf("\n")
		encs := make([]*json.Encoder, nReduce)
		prefix := "mr-" + strconv.Itoa(reply.Task) + "-"
		for i := 0; i < nReduce; i++ {
			filename := prefix + strconv.Itoa(i)
			tempfile, _ := ioutil.TempFile(tempdir, filename)
			tempfiles[i] = tempfile
			encs[i] = json.NewEncoder(tempfile)
		}
		for _, kv := range kva {
			err := encs[ihash(kv.Key) % nReduce].Encode(&kv)
			if err != nil {
				log.Fatalf("cannot append to json file")
			}
		}
		for i, tempfile := range tempfiles {
			tempfile.Close()
			filename := prefix + strconv.Itoa(i)
			// fmt.Printf(tempfile.Name() + "\n")
			os.Rename(tempfile.Name(), filename) 
		}

		CallCommit(reply.Type, reply.Task)

		return 1 // succeed
	} else if reply.Type == 1 {
		// fmt.Printf("reply.Idx %v\n", reply.Idx)
		intermediate := []KeyValue{}

		for i := 0; i < reply.Total; i++ {
			filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Task)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			file.Close()
		}

		sort.Sort(ByKey(intermediate))

		filename := "mr-out-" + strconv.Itoa(reply.Task)
		tempdir := os.TempDir()
		tempfile, _ := ioutil.TempFile(tempdir, filename)

		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		tempfile.Close()
		os.Rename(tempfile.Name(), filename) 

		CallCommit(reply.Type, reply.Task)

		return 1 // succeed
	} else if reply.Type == 2 {
		return -1 // finish, so exit
	} else {
		return 0 // no task
	}
}

func CallCommit(typ int, task int) {
	args := CommitArgs{}
    args.Type = typ
	args.Task = task
    reply := CommitReply{}

    ok := call("Coordinator.Commit", &args, &reply)

    if ok == false {
        fmt.Printf("CallCommit failed!\n")
    }
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
