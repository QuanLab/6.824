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

const (
	masterHost = "localhost"
	masterPort= 8080
)

var taskUniqueID = atomic.NewInt32(0)

type TaskType int
type TaskStatus int
type WorkerStatus int
type ReplyType int

type Master struct {
	//map worker -> status
	WorkerMap          map[string]Worker
	MapperTask         map[int32]Task
	ReducerTask        map[int]Task
	NumMapperCompleted int
	NumReduceCompleted int
	NReduce            int
	PeriodCheck   	   int
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

//
// distribute tasks over workers when received notification
//
func (m *Master) DistributeTask(req *WorkerRequest, reply *MasterResponse) error {
	worker := req.Worker
	log.Printf("received request from: %s\n", worker.ID)
	// check worker id if exists and assign worker id to Worker Manager
	if _, ok := m.WorkerMap[worker.ID]; !ok {
		m.mu.Lock()
		m.WorkerMap[worker.ID] = worker
		m.mu.Unlock()
	}

	// update task has done
	if req.Task.Type == MapTask {
		if mTask, ok := m.MapperTask[req.Task.ID]; ok {
			// only update in-progress task, avoid multi-worker execute the same task
			if mTask.Status == InProgress {
				m.mu.Lock()
				m.MapperTask[req.Task.ID] = req.Task
				m.mu.Unlock()

				log.Printf("MapTask has completed: %d/%d\n", m.NumMapperCompleted, len(m.MapperTask))
				// assign files output from Mapper to Reducer Manager
				// iterate map MapTask get output files
				for i := 0; i < m.NReduce; i++ {
					reducerTask := m.ReducerTask[i]
					reducerTask.InputFile = append(reducerTask.InputFile, req.Task.OutputFile[i])
					m.mu.Lock()
					m.ReducerTask[i] = reducerTask
					m.mu.Unlock()
				}
				m.mu.Lock()
				m.NumMapperCompleted = m.NumMapperCompleted + 1
				m.mu.Unlock()
			}
		}
	} else if req.Task.Type == ReduceTask {
		if _, ok := m.ReducerTask[int(req.Task.ID)]; ok {
			m.mu.Lock()
			m.ReducerTask[int(req.Task.ID)] = req.Task
			m.NumReduceCompleted =  m.NumReduceCompleted + 1
			m.mu.Unlock()
			log.Printf("ReduceTask has completed: %d/%d\n", m.NumReduceCompleted, m.NReduce)
		}
	}

	// get task in queue and assign to worker
	if task, ok := m.GetTaskInQueue(); ok {
		reply.Type = RT_Task
		reply.Task = task
		reply.NumPartition = m.NReduce
		//update task status in Task Manager (MasterMapTask)
		task.Status = InProgress
		task.WorkerID = worker.ID

		// update status of task in master data structure
		if task.Type == MapTask {
			m.mu.Lock()
			m.MapperTask[task.ID] = task
			m.mu.Unlock()
		} else if task.Type == ReduceTask {
			m.mu.Lock()
			m.ReducerTask[int(task.ID)] = task
			m.mu.Unlock()
		}
	} else {
		if m.NumMapperCompleted < len(m.MapperTask) {
			log.Println("Wait for all mapper complete....")
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
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	os.Create("mr-socket")

	l, e := net.Listen("tcp", "0.0.0.0:8080")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("Server is running at %s\n", l.Addr().String())
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	for _, worker := range m.WorkerMap {
		// ping to worker
		_, err := rpc.DialHTTP("tcp", worker.Host + fmt.Sprintf(":%d", worker.Port))
		if err != nil {
			m.mu.Lock()
			delete(m.WorkerMap, worker.ID)
			m.mu.Unlock()

			log.Printf("worker %s died!\n", worker.ID)
			log.Println("re-assign task to other worker")
			if m.NumMapperCompleted < len (m.MapperTask) {
				for _, mTask := range m.MapperTask {
					if mTask.WorkerID == worker.ID {
						mTask.Status = Idle
						m.mu.Lock()
						m.MapperTask[mTask.ID] = mTask
						m.mu.Unlock()
						log.Println("mapper task hold by worker has released. Task ID: ", mTask.ID)
					}
				}
			}
			if m.NumReduceCompleted < len (m.ReducerTask) {
				for _, rTask := range m.ReducerTask {
					if rTask.WorkerID == worker.ID {
						rTask.Status = Idle
						m.mu.Lock()
						m.ReducerTask[int(rTask.ID)] = rTask
						m.mu.Unlock()
						log.Println("reducer task hold by worker has released. Task ID: ", rTask.ID)
					}
				}
			}
		} else {
			log.Printf("worker %s is running \n", worker.ID)
		}
	}
	// tasks completed
	if m.NumReduceCompleted == m.NReduce {
		ret = true
	}
	return ret
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{NReduce: nReduce, NumMapperCompleted: 0, NumReduceCompleted:0, PeriodCheck: 5}
	m.MapperTask = make(map[int32]Task, 0)
	m.WorkerMap = make(map[string]Worker)

	for _, file := range files {
		task := Task{ID: taskUniqueID.Inc(), Type: MapTask, Status: Idle, InputFile: []string{file}}
		m.MapperTask[task.ID] = task
	}

	m.ReducerTask = make(map[int]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		m.ReducerTask[i] = Task{ID: int32(i), Type: ReduceTask, Status: Idle}
	}
	m.server()
	return &m
}
