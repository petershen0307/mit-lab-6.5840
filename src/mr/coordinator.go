package mr

import (
	"log"
	"maps"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

/*
Map state
	id
	file name
	task state
	last update time
Reduce state
	id
	task state
	last update time
*/

type Task struct {
	FileName        string
	State           TaskState
	LastUpdatedTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	lock          sync.Mutex
	queue         chan GetTaskOutput
	reduceBuckets int
	MapTasks      map[int]Task
	ReduceTasks   map[int]Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) updateTask(id int, state TaskState, tasks map[int]Task, failedReplay GetTaskOutput) {
	t := tasks[id]
	t.State = state
	t.LastUpdatedTime = time.Now().UTC()
	tasks[id] = t
	if tasks[id].State == Failed {
		c.queue <- failedReplay
	}
}

/*
Refactor
[v] 1. separate MR() to GetTask and ReportTask()
2. [MAP] write file format to Key Value and the key should be sorted
3. [REDUCE] collect file start with the smallest file index
4. user priority queue(heap) as the queue, heap can help us to maintain the queue order
*/

func (c *Coordinator) GetTask(input *GetTaskInput, output *GetTaskOutput) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.queue) == 0 {
		return nil
	}
	*output = <-c.queue
	t := c.MapTasks[output.TaskID]
	t.LastUpdatedTime = time.Now().UTC()
	t.State = Running
	c.MapTasks[output.TaskID] = t
	return nil
}

func (c *Coordinator) ReportTask(input *ReportTaskInput, output *ReportTaskOutput) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if input.ExecType == Map {
		c.updateTask(input.TaskID, input.State, c.MapTasks, GetTaskOutput{
			TaskID:        input.TaskID,
			ExecType:      Map,
			FileName:      c.MapTasks[input.TaskID].FileName,
			ReduceBuckets: c.reduceBuckets,
		})
	} else {
		c.updateTask(input.TaskID, input.State, c.ReduceTasks, GetTaskOutput{
			TaskID:   input.TaskID,
			ExecType: Reduce,
		})
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	// need to check when to add reduce task to queue
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.queue) != 0 {
		return false
	}
	tMap := maps.Collect(func(yield func(int, Task) bool) {
		for k, v := range c.MapTasks {
			if v.State != Finished {
				yield(k, v)
			}
		}
	})
	tReduce := maps.Collect(func(yield func(int, Task) bool) {
		for k, v := range c.ReduceTasks {
			if v.State != Finished {
				yield(k, v)
			}
		}
	})
	// all map task done, produce reduce task
	if len(tMap) == 0 && len(tReduce) != 0 {
		for i := range c.reduceBuckets {
			c.queue <- GetTaskOutput{
				TaskID:   i,
				ExecType: Reduce,
			}
		}
	}

	return len(tMap) == 0 && len(tReduce) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	// Your code here.

	c := Coordinator{
		queue:         make(chan GetTaskOutput, max(len(files), nReduce)), // ensure channel size is enough
		MapTasks:      make(map[int]Task),
		ReduceTasks:   make(map[int]Task),
		reduceBuckets: nReduce,
	}

	for n, file := range files {
		c.queue <- GetTaskOutput{
			TaskID:        n,
			FileName:      file,
			ExecType:      Map,
			ReduceBuckets: nReduce,
		}
		c.MapTasks[n] = Task{
			FileName:        file,
			State:           Ready,
			LastUpdatedTime: time.Now().UTC(),
		}
	}
	// initial reduce task map
	for i := range nReduce {
		c.ReduceTasks[i] = Task{
			State:           Ready,
			LastUpdatedTime: time.Now().UTC(),
		}
	}

	c.server(sockname)
	return &c
}
