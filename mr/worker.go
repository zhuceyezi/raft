package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var nReduce int

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// General skeleton:
	// Repeatedly ask for a task
	// 1. Request a task from coordinator.
	for {
		// I have to know how many reducers there are to determine how many
		// intermediate files I have to store.
		n, successful := GetnReduce()
		nReduce = n
		if !successful {
			fmt.Println("Failed to contact coordinator")
			return
		}
		reply, successful := RequestTask()
		if !successful {
			fmt.Println("Failed to get a task")
			return
		}
		// 2. Identify the TaskType.
		// 2.1 If All done then just exit the worker.
		if reply.TaskType == AllDone {
			return
		}
		// 2.2 If No task is assigned then it means all tasks are being processed. So this worker should wait.

		// 2.3 If it is a map task then do the map job.
		task := &Task{}
		task.FileAddress = reply.FileAddress
		task.Id = reply.TaskId
		task.WorkerId = os.Getpid()
		task.Type = reply.TaskType

		KeepWorking, _ := true, true
		if reply.TaskType == MapTask {
			// fmt.Printf("Got MapTask %v\n", task.Id)
			KeepWorking, _ = ProcessMap(mapf, task)
			//Only when job done is accepted will the worker rename the output.

		} else if reply.TaskType == ReduceTask {
			// 2.4 If it is a reduce task then do the reduce job.
			// fmt.Printf("Got ReduceTask %v\n", task.Id)
			KeepWorking, _ = ProcessReduce(reducef, task)
		}
		if !KeepWorking {
			fmt.Println("Worker Asked to exit")
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func RequestTask() (*RequestReply, bool) {
	// have to make an RPC call on c.RequestTask.
	// So have to set up args and replies.
	args := RequestArgs{os.Getpid()}
	reply := RequestReply{}
	// RPC call
	successful := call("Coordinator.RequestTask", &args, &reply)
	return &reply, successful

}
func GetnReduce() (int, bool) {
	args := GetArgs{}
	reply := GetReply{}
	successful := call("Coordinator.GetnReduce", &args, &reply)
	return reply.NReduce, successful
}

// We should have a function to do the Map job.
func ProcessMap(mapf func(string, string) []KeyValue, task *Task) (bool, bool) {
	// get the contents of a file
	// fmt.Println("Opening file")
	file, err := os.Open(task.FileAddress)
	// fmt.Println("Opened a file")
	if err != nil {
		log.Fatalf("Error opening file at %v\n", task.FileAddress)
	}
	// fmt.Println("Reading file")
	Readbuffer := bufio.NewReader(file)
	content, err := ioutil.ReadAll(Readbuffer)
	if err != nil {
		log.Fatalf("Error fecthing content at %v\n", task.FileAddress)
	}
	file.Close()
	// fmt.Println("Complete reading file")
	KeyValuePairs := mapf(task.FileAddress, string(content))
	// And then write the output
	// create a temp file for output
	// stores all files
	files := make([]*os.File, 0, nReduce)
	//  stores all encoders so I can find it later when I do the hashing.
	encoders := make([]*json.Encoder, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	// It seems that using buffered IO will improve performance (from Google)
	// fmt.Println("Creating temp file")
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("MRTemp/mrTemp-%v-%v-%v", task.Id, i, task.WorkerId)
		file, err = os.Create(filename)
		if err != nil {
			// fmt.Println(err)
			log.Fatalf("Unable to create temp map file: %v", filename)
		}
		files = append(files, file)
		buffer := bufio.NewWriter(file)
		buffers = append(buffers, buffer)
		encoders = append(encoders, json.NewEncoder((buffer)))
	}

	// do the hashing, map&write to each bin
	// fmt.Println("Done creating temp file, Hashing and writing.")

	for _, KeyValue := range KeyValuePairs {
		index := ihash(KeyValue.Key) % nReduce
		// write the file
		// Code referenced from readme.md

		// now this will store output into the buffers
		err := encoders[index].Encode(&KeyValue)
		if err != nil {
			log.Fatalf("Unable to encode key: %v", KeyValue)
		}
	}

	// we have to flush the buffers to the file
	for _, buffer := range buffers {
		err := buffer.Flush()
		if err != nil {
			log.Fatalf("Failed to flush buffer")
		}
	}
	// now we have written all the files let's do the rename.
	// fmt.Println("Done writing. Renaming files")
	// only rename after job request is accepted.

	for i, File := range files {
		File.Close()
		NewName := fmt.Sprintf("MRTemp/mr-%v-%v", task.Id, i)
		err := os.Rename(File.Name(), NewName)
		if err != nil {
			log.Fatalf("Unable to rename file: %v", File)
		}
	}
	_, KeepWorking, successful := JobIsDone(task)
	return KeepWorking, successful
}

// Also a function to do the reduce job
func ProcessReduce(reducef func(string, []string) string, task *Task) (bool, bool) {
	// first get all files
	files, err := filepath.Glob(fmt.Sprintf("MRTemp/mr-%v-%v", "*", task.Id))
	AllKeyValues := make(map[string][]string)
	var kv KeyValue
	// for each file apply reduce function

	// first collect all key value pairs
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Unable to open file")
		}
		ReadBuffer := bufio.NewReader(file)
		dec := json.NewDecoder(ReadBuffer)
		for dec.More() {
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatalf("Error decoding")
			}
			AllKeyValues[kv.Key] = append(AllKeyValues[kv.Key], kv.Value)
		}
	}
	// Then apply reduce function
	// before that we have to sort first
	// put in an slice so we can use sort function
	// KvSlice := make([]string, 0, len(AllKeyValues))
	// for key := range AllKeyValues {
	// 	KvSlice = append(KvSlice, key)
	// }
	// Tabnine does a great job here LOL
	// sort.Strings(KvSlice)
	// Then things are sorted and we can apply reduce function
	// Write to temp file first. This time only one
	filename := fmt.Sprintf("MRTemp/mrTemp-out-%v-%v", task.Id, task.WorkerId)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed to create temp file: %v", filename)
	}
	// apply the reduce function for each key
	buffer := bufio.NewWriter(file)
	for key := range AllKeyValues {
		value := reducef(key, AllKeyValues[key])
		output := fmt.Sprintf("%v %v\n", key, value)
		// _, err := fmt.Fprintf(file, output)
		_, err := fmt.Fprintf(buffer, output)
		if err != nil {
			log.Fatalf("Failed to write to output file.")
		}
	}
	buffer.Flush()
	// after written we rename

	// Have to close the file or We can't rename it
	file.Close()

	NewName := fmt.Sprintf("mr-out-%v", task.Id)
	err = os.Rename(filename, NewName)
	if err != nil {
		log.Fatalf("Unable to rename file: %v", file)
	}
	_, KeepWorking, successful := JobIsDone(task)
	return KeepWorking, successful
}

// Return three bools: Job accepted, worker should KeepWorking and if report is successful
func JobIsDone(task *Task) (bool, bool, bool) {
	args := ReportArgs{os.Getpid(), task.Id, task.Type}
	reply := ReportReply{}
	successful := call("Coordinator.JobIsDone", &args, &reply)
	if reply.JobAccepted {
		if task.Type == MapTask {
			fmt.Printf("√ %s Task %v is done.\n", "Map", task.Id)
		} else if task.Type == ReduceTask {
			fmt.Printf("√ %s Task %v is done.\n", "Reduce", task.Id)
		}
	}
	return reply.JobAccepted, reply.KeepWorking, successful
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
