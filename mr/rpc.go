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
	Halt 	WorkerStatus = -1
)

type WorkerRequest struct {
	WorkerWrapper WorkerWrapper
	Task		  Task
}

type WorkerWrapper struct {
	Host         string
	Port		 string
	WorkerID     string
	CpuAvailable int
	MemAvailable int
	Status		 WorkerStatus
}

type MasterResponse struct {
	NumPartition int
	Task         Task
	Type         ReplyType
}
