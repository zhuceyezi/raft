package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TASK_TIMEOUT = 10 * time.Second
const TaskNotAssigned = -1
const WorkerNotAssigned = -1

type TaskStatus int
type TaskType int

// The states of a task
const (
	NoTask TaskStatus = iota
	InProgress
	Complete
)

const (
	MapTask TaskType = iota
	ReduceTask
	EmptyTask
	AllDone // The please exit task type.
)

type Task struct {
	Type        TaskType
	Status      TaskStatus
	Id          int
	FileAddress string
	WorkerId    int //Worker Id should be the process Id in the system, that comes in handy
}

type Coordinator struct {
	// Your definitions here.
	nMap                 int
	nReduce              int
	locker               sync.Mutex
	mapTasks             []Task
	reduceTasks          []Task
	RemainingMapTasks    int
	RemainingReduceTasks int
	// reduceInputWorkers   map[int]int
}

// Your code here -- RPC handlers for the worker to call.

// The worker will request for a task. RPC
func (c *Coordinator) RequestTask(args *RequestArgs, reply *RequestReply) error {
	// first lock so only one worker requests a task a time.
	// So two workers don't get the same task?
	c.locker.Lock()
	var task *Task
	if c.RemainingMapTasks > 0 {
		// assign map task
		task = c.AssignTask(c.mapTasks, args.WorkerId)
	} else if c.RemainingReduceTasks > 0 {
		// assign reduce task
		task = c.AssignTask(c.reduceTasks, args.WorkerId)
	} else {
		// No task to assign, done
		task = &Task{AllDone, Complete, TaskNotAssigned, "", WorkerNotAssigned}
	}
	reply.TaskType = task.Type
	reply.TaskId = task.Id
	reply.FileAddress = task.FileAddress
	c.locker.Unlock()
	go c.MonitorTask(task)
	return nil
}
func (c *Coordinator) GetnReduce(args *GetArgs, reply *GetReply) error {
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) AssignTask(taskList []Task, workerId int) *Task {
	var task *Task
	for i := 0; i < len(taskList); i++ {
		if taskList[i].Status == NoTask {
			task = &taskList[i]
			task.Status = InProgress
			task.WorkerId = workerId
			return task
		}
	}
	task = &Task{EmptyTask, Complete, TaskNotAssigned, "", WorkerNotAssigned}
	return task
}

// monitor the task, check if task is alive.
func (c *Coordinator) MonitorTask(task *Task) {
	// we will not monitor a task if it is not a map or reduce task.

	if task.Type != MapTask && task.Type != ReduceTask {
		return
	} else {
		<-time.After(TASK_TIMEOUT)
		c.locker.Lock()
		defer c.locker.Unlock()
		// after 10s if the task is still not done we say this task is dead.

		if task.Status == InProgress {
			if task.Type == ReduceTask {
				fmt.Printf("! Reduce Task %v is dead.\n", task.Id)
			} else if task.Type == MapTask {
				fmt.Printf("! Map Task %v is dead.\n", task.Id)
			}
			c.ResetTaskStatus(task)
		}

	}

}

// RPC handler when worker says its job is done.
func (c *Coordinator) JobIsDone(args *ReportArgs, reply *ReportReply) error {
	// lock or there is race
	c.locker.Lock()
	defer c.locker.Unlock()
	// First find the task in the task list
	var task *Task
	if args.TaskType == MapTask {
		// fmt.Println("Map Here")
		task = &c.mapTasks[args.TaskId]
	} else if args.TaskType == ReduceTask {
		// fmt.Println("Here")
		task = &c.reduceTasks[args.TaskId]
	} else {
		return fmt.Errorf("invalid task type: %v", args.TaskType)
	}
	// fmt.Println("Point")
	// only if the task is not reassigned will the coordinator accept the report and reply.

	// if the task is redid or did by a non-assigned worker, then ignore.
	if task.WorkerId == args.WorkerId && task.Status == InProgress {
		task.Status = Complete
		reply.JobAccepted = true
		// decrement the counters
		if args.TaskType == MapTask && c.RemainingMapTasks > 0 {
			c.RemainingMapTasks--
			// c.reduceInputWorkers[task.WorkerId] = 0 // arbitary value works
		} else if args.TaskType == ReduceTask && c.RemainingReduceTasks > 0 {
			c.RemainingReduceTasks--
		}
	} else if task.WorkerId != args.WorkerId {
		reply.JobAccepted = false
		// fmt.Println("WorkerId doesn't match.")
		// fmt.Printf("task.WorkerId = %v\n", task.WorkerId)
		// fmt.Printf("args.WorkerId = %v\n", args.WorkerId)
	} else if task.Status != InProgress {
		reply.JobAccepted = false
		// fmt.Println("Task not in progress")
	}
	// reply if worker is safe to exit
	reply.KeepWorking = c.RemainingMapTasks > 0 || c.RemainingReduceTasks > 0
	return nil
}

// function to reset the status of a task to not assigned status.
func (c *Coordinator) ResetTaskStatus(task *Task) {
	task.Status = NoTask
	task.WorkerId = WorkerNotAssigned
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.locker.Lock()
	defer c.locker.Unlock()
	return c.RemainingMapTasks == 0 && c.RemainingReduceTasks == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// need nMap for n files.
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapTasks = make([]Task, 0, c.nMap)
	c.reduceTasks = make([]Task, 0, c.nReduce)
	c.RemainingMapTasks = c.nMap
	c.RemainingReduceTasks = c.nReduce
	// c.reduceInputWorkers = make(map[int]int)

	// initiate all map tasks, let mapper i do file i.
	for i := 0; i < c.nMap; i++ {
		mapTask := Task{MapTask, NoTask, i, files[i], WorkerNotAssigned} // workerId -1 means no worker yet.
		c.mapTasks = append(c.mapTasks, mapTask)
	}

	// initialize all reduce tasks. No file path now, waiting for mappers to be done.
	for i := 0; i < c.nReduce; i++ {
		reduceTask := Task{ReduceTask, NoTask, i, "", WorkerNotAssigned}
		c.reduceTasks = append(c.reduceTasks, reduceTask)
	}
	c.server()

	// create temp directory
	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Remove file %v\n failed", f)
		}
	}
	err := os.RemoveAll("MRTemp")
	if err != nil {
		log.Fatalf("Remove Failed %v\n", "MRTemp")
	}
	err = os.Mkdir("MRTemp", os.ModePerm)
	if err != nil {
		log.Fatalf("Create Failed %v\n", "MRTemp")
	}
	return &c
}
