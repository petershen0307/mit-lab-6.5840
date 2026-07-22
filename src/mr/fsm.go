package mr

import (
	"github.com/looplab/fsm"
)

/*
	pending -> running -> complete
				|-> failed
				|- (timeout) -> pending
*/

const (
	StatePending  = "pending"
	StateRunning  = "running"
	StateComplete = "complete"
	StateFailed   = "failed"

	EventGetTask           = "getTask"
	EventReportTaskSuccess = "reportTaskSuccess"
	EventReportTaskFailed  = "reportTaskFailed"
	EventReportTaskTimeout = "reportTaskTimeout"
)

func newMrFsm() *fsm.FSM {
	return fsm.NewFSM(StatePending,
		[]fsm.EventDesc{
			{Name: EventGetTask, Src: []string{StatePending}, Dst: StateRunning},
			{Name: EventReportTaskSuccess, Src: []string{StateRunning}, Dst: StateComplete},
			{Name: EventReportTaskFailed, Src: []string{StateRunning}, Dst: EventReportTaskFailed},
			{Name: EventReportTaskTimeout, Src: []string{StateRunning}, Dst: StatePending},
		},
		map[string]fsm.Callback{},
	)
}
