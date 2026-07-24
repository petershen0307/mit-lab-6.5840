package mr

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/looplab/fsm"
)

func init() {
	// set the log
	log.SetOutput(io.Discard)
	log.SetPrefix("[Coordinator]")
}

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
	ID              int
	FileName        string
	LastUpdatedTime time.Time
	state           *fsm.FSM
}

type Coordinator struct {
	// Your definitions here.
	lock          sync.Mutex
	queue         chan GetTaskOutput
	reduceBuckets int
	mapTasks      []Task
	reduceTasks   []Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

func areDone(tasks []Task) bool {
	b := true
	for _, t := range tasks {
		b = b && (t.state.Is(StateComplete) || t.state.Is(StateFailed))
	}
	return b
}

func arePending(tasks []Task) bool {
	b := true
	for _, t := range tasks {
		b = b && t.state.Is(StatePending)
	}
	return b
}

func printState(tasks []Task) {
	for _, t := range tasks {
		log.Println(t.state.Current())
	}
}

func updateTaskState(tasks *[]Task, id int, event string) {
	for i, t := range *tasks {
		if t.ID == id {
			if err := (*tasks)[i].state.Event(context.Background(), event); err != nil {
				log.Panicln("state machine failed with event", event, "and error", err)
			}
		}
	}
}

func (c *Coordinator) GetTask(input *GetTaskInput, output *GetTaskOutput) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.queue) == 0 && areDone(c.reduceTasks) {
		return nil
	}
	if len(c.queue) == 0 {
		// add the reduce task to queue
		*output = GetTaskOutput{
			ExecType: Wait,
		}
		return nil
	}
	log.Println("[GetTask] request task")
	*output = <-c.queue
	var tasks *[]Task
	switch output.ExecType {
	case Map:
		tasks = &c.mapTasks
	case Reduce:
		tasks = &c.reduceTasks
	}
	// update task state to pending
	updateTaskState(tasks, output.TaskID, EventGetTask)
	log.Println("[GetTask]", output.ExecType, output.TaskID)
	return nil
}

func (c *Coordinator) ReportTask(input *ReportTaskInput, output *ReportTaskOutput) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	var tasks *[]Task
	switch input.ExecType {
	case Map:
		tasks = &c.mapTasks
	case Reduce:
		tasks = &c.reduceTasks
	}
	event := EventReportTaskSuccess
	if input.State == Failed {
		event = EventReportTaskFailed
	}
	// update task state to pending
	updateTaskState(tasks, input.TaskID, event)
	log.Println("[ReportTask]", input.ExecType, input.TaskID, input.State)
	if areDone(c.mapTasks) && arePending(c.reduceTasks) {
		// get map complete task id
		mapTaskIDs := []int{}
		for _, m := range c.mapTasks {
			if m.state.Is(StateComplete) {
				mapTaskIDs = append(mapTaskIDs, m.ID)
			}
		}
		for _, t := range c.reduceTasks {
			c.queue <- GetTaskOutput{
				TaskID:     t.ID,
				ExecType:   Reduce,
				MapTaskIDs: mapTaskIDs,
			}
		}
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
	return len(c.queue) == 0 && areDone(c.reduceTasks) && areDone(c.mapTasks)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		mapTasks:      []Task{},
		reduceTasks:   []Task{},
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
		c.mapTasks = append(c.mapTasks, Task{
			ID:              n,
			FileName:        file,
			LastUpdatedTime: time.Now().UTC(),
			state:           newMrFsm(),
		})
	}
	// initial reduce task map
	for i := range nReduce {
		c.reduceTasks = append(c.reduceTasks, Task{
			ID:              i,
			LastUpdatedTime: time.Now().UTC(),
			state:           newMrFsm(),
		})
	}
	c.server(sockname)
	return &c
}
