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
type MessageType string

const (
	RequestTask  MessageType = "RequestTask"
	FinishedTask MessageType = "FinishedTask"
)

type MessageArgs struct {
	X int
}

type ExecType string

const (
	Map    ExecType = "MAP"
	Reduce ExecType = "REDUCE"
)

type MessageReply struct {
	ExecType ExecType
}
