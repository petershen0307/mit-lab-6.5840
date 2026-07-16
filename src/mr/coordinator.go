package mr

import (
	"log"
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

type TaskManage struct {
	Tasks           map[int]Task
	RemainTaskCount int
}

type Coordinator struct {
	// Your definitions here.
	lock          sync.Mutex
	queue         chan GetTaskOutput
	reduceBuckets int
	mapTasks      TaskManage
	reduceTasks   TaskManage
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) updateTask(id int, state TaskState, taskM *TaskManage, failedReplay GetTaskOutput) {
	t := taskM.Tasks[id]
	t.State = state
	t.LastUpdatedTime = time.Now().UTC()
	taskM.Tasks[id] = t
	switch taskM.Tasks[id].State {
	case Failed:
		c.queue <- failedReplay
	case Finished:
		taskM.RemainTaskCount--
	}
}

/*
Refactor
[v] 1. separate MR() to GetTask and ReportTask()
[v] 2. [MAP] write file format to Key Value and the key should be sorted
[v] 3. [REDUCE] collect file start with the smallest file index
[v] 4. user priority queue(heap) as the queue, heap can help us to maintain the queue order
*/

/*
add the session
task running -(didn't receive the feedback)-> task timeout
task timeout -(new session)-> task running
task running -(the session is correct)-> receive task report
task running -(the session is stale)-> dorp stale task report
*/

func (c *Coordinator) GetTask(input *GetTaskInput, output *GetTaskOutput) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.queue) == 0 && c.reduceTasks.RemainTaskCount <= 0 {
		return nil
	}
	if len(c.queue) == 0 {
		*output = GetTaskOutput{
			ExecType: Wait,
		}
		return nil
	}
	*output = <-c.queue
	switch output.ExecType {
	case Map:
		c.updateTask(output.TaskID, Running, &c.mapTasks, GetTaskOutput{})
	case Reduce:
		c.updateTask(output.TaskID, Running, &c.reduceTasks, GetTaskOutput{})
	}
	log.Println("[coordinator]", output.ExecType, output.TaskID)
	return nil
}

func (c *Coordinator) ReportTask(input *ReportTaskInput, output *ReportTaskOutput) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	switch input.ExecType {
	case Map:
		c.updateTask(input.TaskID, input.State, &c.mapTasks, GetTaskOutput{
			TaskID:        input.TaskID,
			ExecType:      Map,
			FileName:      c.mapTasks.Tasks[input.TaskID].FileName,
			ReduceBuckets: c.reduceBuckets,
		})
		// check the map task, if all map tasks are done, create the reduce task
		log.Println(len(c.queue), cap(c.queue), input.ExecType, input.TaskID, c.mapTasks.RemainTaskCount, c.reduceTasks.RemainTaskCount)
		if c.mapTasks.RemainTaskCount == 0 {
			for n := range c.reduceBuckets {
				c.queue <- GetTaskOutput{
					TaskID:   n,
					ExecType: Reduce,
				}
			}
		}
	case Reduce:
		c.updateTask(input.TaskID, input.State, &c.reduceTasks, GetTaskOutput{
			TaskID:   input.TaskID,
			ExecType: Reduce,
		})
	default:
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
	return len(c.queue) == 0 && c.reduceTasks.RemainTaskCount <= 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	// Your code here.

	c := Coordinator{
		mapTasks: TaskManage{
			Tasks:           make(map[int]Task),
			RemainTaskCount: len(files),
		},
		reduceTasks: TaskManage{
			Tasks:           make(map[int]Task),
			RemainTaskCount: nReduce,
		},
		reduceBuckets: nReduce,
		queue:         make(chan GetTaskOutput, max(len(files), nReduce)),
	}

	for n, file := range files {
		c.queue <- GetTaskOutput{
			TaskID:        n,
			FileName:      file,
			ExecType:      Map,
			ReduceBuckets: nReduce,
		}
		c.mapTasks.Tasks[n] = Task{
			FileName:        file,
			State:           Ready,
			LastUpdatedTime: time.Now().UTC(),
		}
	}
	// initial reduce task map
	for i := range nReduce {
		c.reduceTasks.Tasks[i] = Task{
			State:           Ready,
			LastUpdatedTime: time.Now().UTC(),
		}
	}
	c.server(sockname)
	return &c
}
