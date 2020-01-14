package mr

//
// RPC definitions.

const (
	MapTask    TaskType = 100
	ReduceTask TaskType = 101
)

const (
	InProgress TaskStatus = 102
	Idle       TaskStatus = 103
	Completed  TaskStatus = 104
)

const (
	RT_Stop ReplyType = 106
	RT_Wait ReplyType = 107
	RT_Task ReplyType = 108
)

const (
	Running WorkerStatus = 1
	Halt    WorkerStatus = -1
)

type WorkerRequest struct {
	Worker Worker
	Task   Task
}

type MasterResponse struct {
	NumPartition int
	Task         Task
	Type         ReplyType
}

type HeartBeat struct {
	IsAlive bool
}
