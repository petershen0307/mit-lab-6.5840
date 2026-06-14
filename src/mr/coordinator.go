package mr

import (
	"container/heap"
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

const (
	MapPriority    = 2
	ReducePriority = 1
)

type Coordinator struct {
	// Your definitions here.
	lock          sync.Mutex
	pqueue        PriorityQueue
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
		priority := ReducePriority
		if failedReplay.ExecType == Map {
			priority = MapPriority
		}
		heap.Push(&(c.pqueue), &QueueItem{
			value:    failedReplay,
			priority: priority,
		})
	}
}

/*
Refactor
[v] 1. separate MR() to GetTask and ReportTask()
2. [MAP] write file format to Key Value and the key should be sorted
3. [REDUCE] collect file start with the smallest file index
[v] 4. user priority queue(heap) as the queue, heap can help us to maintain the queue order
*/

func (c *Coordinator) GetTask(input *GetTaskInput, output *GetTaskOutput) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.pqueue.Len() == 0 {
		return nil
	}
	*output = heap.Pop(&(c.pqueue)).(*QueueItem).value
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
	return c.pqueue.Len() == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	// Your code here.

	c := Coordinator{
		MapTasks:      make(map[int]Task),
		ReduceTasks:   make(map[int]Task),
		reduceBuckets: nReduce,
	}

	heap.Init(&(c.pqueue))

	for n, file := range files {
		heap.Push(&(c.pqueue),
			&QueueItem{
				value: GetTaskOutput{
					TaskID:        n,
					FileName:      file,
					ExecType:      Map,
					ReduceBuckets: nReduce,
				},
				priority: MapPriority,
			})
		c.MapTasks[n] = Task{
			FileName:        file,
			State:           Ready,
			LastUpdatedTime: time.Now().UTC(),
		}
	}
	// initial reduce task map
	for i := range nReduce {
		heap.Push(&(c.pqueue), &QueueItem{
			value: GetTaskOutput{
				TaskID:   i,
				ExecType: Reduce,
			},
			priority: ReducePriority,
		})
		c.ReduceTasks[i] = Task{
			State:           Ready,
			LastUpdatedTime: time.Now().UTC(),
		}
	}
	c.server(sockname)
	return &c
}

// -------------------------
// use priority queue
type QueueItem struct {
	value    GetTaskOutput // The value of the item; arbitrary.
	priority int           // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*QueueItem

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*QueueItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // don't stop the GC from reclaiming the item eventually
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
