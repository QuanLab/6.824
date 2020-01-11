package mr

import (
	"fmt"
	"log"
	"os"
	"sync"
)
import "net"
import "net/rpc"
import "net/http"
import "github.com/uber-go/atomic"

var taskUniqueID = atomic.NewInt32(0)

type TaskType int
type TaskStatus int
type WorkerStatus int
type ReplyType int

type Master struct {
	//map worker -> status
	WorkerMap          map[string]WorkerWrapper
	MapperTask         map[int32]Task
	ReducerTask        map[int]Task
	NumMapperCompleted int
	NumReduceCompleted int
	NReduce            int
	mu                 sync.Mutex
}

type Job struct {
	ID      int32
	TaskMap map[int32]Task
}

type Task struct {
	ID        int32
	Type      TaskType
	Status    TaskStatus
	InputFile []string
	// partition --> file
	OutputFile map[int]string
	WorkerID   string
}

// Your code here -- RPC handlers for the worker to call.

//
// distribute tasks over workers when received notification
//
func (m *Master) DistributeTask(req *WorkerRequest, reply *MasterResponse) error {

	worker := req.WorkerWrapper
	fmt.Printf("received request from worker: %s\n", worker.WorkerID)
	fmt.Println(req.Task)
	// check worker id if exists and assign worker id to Worker Manager
	if _, ok := m.WorkerMap[worker.WorkerID]; !ok {
		m.WorkerMap[worker.WorkerID] = worker
	}

	// update task has done
	if req.Task.Type == MapTask {
		if _, ok := m.MapperTask[req.Task.ID]; ok {
			m.UpdateTaskCompleted(req.Task)
		}
	} else if req.Task.Type == ReduceTask {
		if _, ok := m.ReducerTask[int(req.Task.ID)]; ok {
			m.UpdateTaskCompleted(req.Task)
		}
	}

	// get task in queue and assign to worker
	if task, ok := m.GetTaskInQueue(); ok {
		reply.Type = RT_Task
		reply.Task = task
		reply.NumPartition = m.NReduce
		//update task status in Task Manager (MasterMapTask)
		task.Status = InProgress
		task.WorkerID = workerId

		// update status of task in master data structure
		if task.Type == MapTask {
			m.MapperTask[task.ID] = task
		} else if task.Type == ReduceTask {
			m.ReducerTask[int(task.ID)] = task
		}
	} else {
		if m.NumMapperCompleted < len(m.MapperTask) {
			fmt.Println("Wait for all mapper complete....")
			reply.Type = RT_Wait
		} else {
			reply.Type = RT_Stop
		}
	}
	return nil
}

//
// get task in queue
//
func (m *Master) GetTaskInQueue() (Task, bool) {
	if m.NumMapperCompleted < len(m.MapperTask) {
		for _, mTask := range m.MapperTask {
			if mTask.Status == Idle {
				return mTask, true
			}
		}
	} else {
		for _, rTask := range m.ReducerTask {
			if rTask.Status == Idle {
				return rTask, true
			}
		}
	}
	return Task{ID: -1}, false
}

//
// update task status
//
func (m *Master) UpdateTaskCompleted(task Task) {
	// update task has completed
	if task.Type == MapTask {

		m.mu.Lock()
		m.NumMapperCompleted = m.NumMapperCompleted + 1
		m.mu.Unlock()

		fmt.Printf("MapTask has completed: %d/%d\n", m.NumMapperCompleted, len(m.MapperTask))
		m.MapperTask[task.ID] = task
		// assign files output from Mapper to Reducer Manager
		// iterate map MapTask get output files
		for i := 0; i < m.NReduce; i++ {
			reducerTask := m.ReducerTask[i]
			reducerTask.InputFile = append(reducerTask.InputFile, task.OutputFile[i])
			m.ReducerTask[i] = reducerTask
		}
	} else if task.Type == ReduceTask {

		m.mu.Lock()
		m.NumReduceCompleted =  m.NumReduceCompleted + 1
		m.mu.Unlock()

		fmt.Printf("ReduceTask has completed: %d/%d\n", m.NumReduceCompleted, m.NReduce)
		fmt.Println(task.ID, task.InputFile)
		m.ReducerTask[int(task.ID)] = task
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("localhost:8080")
	l, e := net.Listen("tcp", "localhost:8080")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Println("Server is running at [::1]:8080")
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	if m.NumReduceCompleted == m.NReduce {
		ret = true
	}
	return ret
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{NReduce: nReduce, NumMapperCompleted: 0, NumReduceCompleted:0}
	fmt.Println("TaskMap is initializing")
	m.MapperTask = make(map[int32]Task, 0)
	m.WorkerMap = make(map[string]WorkerWrapper)

	fmt.Println("Master Data Structure is initializing...")
	for _, file := range files {
		task := Task{ID: taskUniqueID.Inc(), Type: MapTask, Status: Idle, InputFile: []string{file}}
		m.MapperTask[task.ID] = task
	}

	fmt.Println("Master Data Structure has created")
	m.ReducerTask = make(map[int]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		m.ReducerTask[i] = Task{ID: int32(i), Type: ReduceTask, Status: Idle}
	}
	m.server()
	return &m
}
