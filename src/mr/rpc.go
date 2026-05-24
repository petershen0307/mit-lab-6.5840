package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskState string

const (
	// ready -> running
	// running -> failed
	// running -> finished
	// failed -> running
	Ready    TaskState = "ready"
	Running  TaskState = "running"
	Failed   TaskState = "failed"
	Finished TaskState = "finished"
)

type TaskType string

const (
	Map    TaskType = "MAP"
	Reduce TaskType = "REDUCE"
)

type MessageArgs struct {
	TaskID   int
	State    TaskState
	ExecType TaskType
}

type MessageReply struct {
	TaskID        int
	ExecType      TaskType
	FileName      string
	ReduceBuckets int
}
