package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		task := WorkerAskForTask()
		if task.Task_type == NoMore {
			log.Println("Worker finish")
			break
		} else if task.Task_type == Wait {
			log.Println("Worker wait")
			time.Sleep(time.Second)
		} else if task.Task_type == Map {
			log.Println("Worker do map:", task.Task_id)
			do_map(mapf, &task)
		} else {
			log.Println("Worker do reduce:", task.Task_id)
			do_reduce(reducef, &task)
		}
	}
}

// do map job
func do_map(mapf func(string, string) []KeyValue, task *Task) {
	// two dimension, due to K partition for reduce
	intermediate := [][]KeyValue{}
	for i := 0; i < task.Partition_num; i++ {
		tmp := make([]KeyValue, 0)
		intermediate = append(intermediate, tmp)
	}
	for _, filename := range task.File {
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

		// append to different partition
		for i := 0; i < len(kva); i++ {
			cur_partition := ihash(kva[i].Key) % task.Partition_num
			intermediate[cur_partition] = append(intermediate[cur_partition], kva[i])
		}

	}

	intermediate_file := make([]string, 0)
	// write intermediate key/value to file
	for i := 0; i < task.Partition_num; i++ {
		// sort each partition
		sort.Sort(ByKey(intermediate[i]))
		// write json file
		// add time to make it private, no conflict with others
		file_name := "mr-" + strconv.Itoa(task.Task_id) + "-" + strconv.Itoa(i) + "-" + time.Now().String()
		intermediate_file = append(intermediate_file, file_name)
		file, _ := os.Create(file_name)
		enc := json.NewEncoder(file)
		for _, v := range intermediate[i] {
			err := enc.Encode(&v)
			if err != nil {
				fmt.Println("Error in encoding", err)
			}
		}
		file.Close()
	}

	// finish map and notify master
	args := TaskFinish{}
	args.Task_id = task.Task_id
	args.Output_file = intermediate_file
	reply := EmptyArgs{}
	call("Coordinator.FinishMap", &args, &reply)
}

// do reduce job
func do_reduce(reducef func(string, []string) string, task *Task) {

	oname := "mr-final-" + strconv.Itoa(task.Task_id) + "-" + time.Now().String()
	ofile, _ := os.Create(oname)
	intermediate := make([]KeyValue, 0)
	for _, file_name := range task.File {
		file, _ := os.Open(file_name)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	// finsih and notify master
	args := TaskFinish{}
	args.Task_id = task.Task_id
	args.Output_file = append(args.Output_file, oname)
	reply := EmptyArgs{}
	call("Coordinator.FinishReduce", &args, &reply)
}

// use RPC to acquire task
func WorkerAskForTask() Task {
	args := EmptyArgs{}
	Task := Task{}

	call("Coordinator.GetTask", &args, &Task)
	return Task
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
