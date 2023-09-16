package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type TaskStatus int

const (
	Idle       TaskStatus = 0
	InProgress TaskStatus = 1
	Finish     TaskStatus = 2
)

type TaskType int

const (
	Wait   TaskType = 0
	Map    TaskType = 1
	Reduce TaskType = 2
	NoMore TaskType = 3 // no more taks to assign
)

type Task struct {
	Task_type     TaskType
	Task_id       int
	File          []string // for mapper, only one file; for reducer, R files
	Partition_num int
}

type EmptyArgs struct{}

type TaskFinish struct {
	Output_file []string
	Task_id     int
}

type Coordinator struct {
	// Your definitions here.
	File_name         []string   // input file, processed by map
	Parition_file     [][]string // first dim: different partition; second dim: files belonging to this partition
	Reduce_num        int
	Reduce_finish_num int
	All_finish        bool
	Map_start_time    []time.Time
	Reduce_start_time []time.Time
	Map_status        []TaskStatus
	Reduce_status     []TaskStatus
	mutex             sync.Mutex // protect all data structure
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// assume hold lock before call this function
func (c *Coordinator) CanAssign(index int, task_type TaskType) bool {
	cur := time.Now()

	if task_type == Map {
		status := c.Map_status[index]
		if status == Idle {
			c.Map_start_time[index] = cur
			return true
		} else if status == InProgress {
			if cur.Sub(c.Map_start_time[index]).Seconds() >= 10 {
				// re-assign task
				c.Map_start_time[index] = cur
				return true
			}
		}
	} else {
		status := c.Reduce_status[index]
		if status == Idle && len(c.Parition_file[index]) == len(c.File_name) {
			c.Reduce_start_time[index] = cur
			return true
		} else if status == InProgress {
			if cur.Sub(c.Reduce_start_time[index]).Seconds() >= 10 {
				// re-assign task
				c.Reduce_start_time[index] = cur
				return true
			}
		}
	}

	return false
}

// get task, called by worker
func (c *Coordinator) GetTask(args *EmptyArgs, reply *Task) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	get_task := false
	for i := 0; i < len(c.File_name); i++ {
		if c.CanAssign(i, Map) {
			reply.Task_type = Map
			reply.Task_id = i
			reply.File = []string{c.File_name[i]}
			reply.Partition_num = c.Reduce_num
			// mask as in-progress
			c.Map_status[i] = InProgress
			get_task = true
			log.Println("Assign map:", i)
			break
		}
	}

	if get_task {
		return nil
	}

	for i := 0; i < c.Reduce_num; i++ {
		if c.CanAssign(i, Reduce) {
			// assign it
			reply.Task_type = Reduce
			reply.Task_id = i
			reply.File = c.Parition_file[i]
			c.Reduce_status[i] = InProgress
			get_task = true
			log.Println("Assign reduce:", i)
			break
		}
	}

	if get_task {
		return nil
	}
	// note don't call c.Done() due to we hold lock now
	// just use c.All_finish is fine, otherwise deadlock, because Done try to lock but we hold lock
	if c.All_finish {
		reply.Task_type = NoMore
	} else {
		reply.Task_type = Wait
	}

	return nil
}

func (c *Coordinator) FinishMap(args *TaskFinish, reply *EmptyArgs) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.Map_status[args.Task_id] == Finish {
		return nil
	}

	c.Map_status[args.Task_id] = Finish
	// record parition file
	for i := 0; i < c.Reduce_num; i++ {
		c.Parition_file[i] = append(c.Parition_file[i], args.Output_file[i])
	}

	return nil
}

func (c *Coordinator) FinishReduce(args *TaskFinish, reply *EmptyArgs) error {
	// lock
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.Reduce_status[args.Task_id] == Finish {
		return nil
	}

	c.Reduce_status[args.Task_id] = Finish
	c.Reduce_finish_num++
	if c.Reduce_finish_num == c.Reduce_num {
		c.All_finish = true
	}
	os.Rename(args.Output_file[0], "mr-out-"+strconv.Itoa(args.Task_id))

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
	c.mutex.Lock()
	ret := c.All_finish
	c.mutex.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.File_name = files
	c.Reduce_num = nReduce
	c.All_finish = false
	c.Reduce_finish_num = 0
	l := len(files)
	c.Map_status = make([]TaskStatus, l)
	for i := 0; i < l; i++ {
		c.Map_status[i] = Idle
		c.Map_start_time = make([]time.Time, l)
	}
	c.Reduce_status = make([]TaskStatus, nReduce)
	for i := 0; i < nReduce; i++ {
		c.Reduce_status[i] = Idle
		c.Reduce_start_time = make([]time.Time, nReduce)
	}
	c.Parition_file = make([][]string, nReduce)
	c.server()
	return &c
}
