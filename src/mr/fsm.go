package mr

import (
	"github.com/looplab/fsm"
)

/*
	pending -> running -> complete
				|-> failed (timeout)
*/

const (
	StatePending  = "pending"
	StateRunning  = "running"
	StateComplete = "complete"
	StateFailed   = "failed"

	EventGetTask           = "getTask"
	EventReportTaskSuccess = "reportTaskSuccess"
	EventReportTaskFailed  = "reportTaskFailed"
)

func newMrFsm() *fsm.FSM {
	return fsm.NewFSM(StatePending,
		[]fsm.EventDesc{
			{Name: EventGetTask, Src: []string{StatePending}, Dst: StateRunning},
			{Name: EventReportTaskSuccess, Src: []string{StateRunning}, Dst: StateComplete},
			{Name: EventReportTaskFailed, Src: []string{StateRunning}, Dst: StateFailed},
		},
		map[string]fsm.Callback{},
	)
}
