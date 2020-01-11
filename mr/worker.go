package mr

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import (
	"hash/fnv"
	"os"
	"io/ioutil"
	"encoding/json"
	"sort"
)

var workerId = ""
var workerAddress = ""

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	workerAddress = GetOutboundIP()
	workerId = fmt.Sprintf("worker-%d-%s", time.Now().UnixNano(), workerAddress)
	// create worker temporary directory
	workerDir := fmt.Sprintf(".%s", workerId)
	os.Mkdir(workerDir, 0766)

	reply := AskForTask()

	for {
		if reply.Type == RT_Task {
			task := reply.Task
			if task.Type == MapTask {
				var intermediatePartition = make(map[int][]KeyValue)
				for p:= 0; p < reply.NumPartition; p++ {
					intermediatePartition[p] = []KeyValue{}
				}

				for _, filename :=range task.InputFile {
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}
					content, err := ioutil.ReadAll(file)
					if err != nil {
						log.Fatalf("cannot read %v", filename)
					}
					file.Close()

					kva := mapf(filename, string(content))
					for _, kv := range kva {
						partition := ihash(kv.Key) % reply.NumPartition
						kva := intermediatePartition[partition]
						kva = append(kva, kv)
						intermediatePartition[partition] = kva
					}
				}

				outputFile := make(map[int]string)
				for partition, kva := range intermediatePartition {
					// write immediate pairs to file with format mr-X-Y (X is map task ID, Y is reduce task ID)
					var tmpFileName = fmt.Sprintf("%s/mr-%d-%d", workerDir, task.ID, partition)
					outputFile[partition] = tmpFileName
					tmpFile, err := os.OpenFile(tmpFileName,  os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					if err!=nil {
						log.Fatal(err)
					}
					enc := json.NewEncoder(tmpFile)
					for _, kv :=range kva {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatal(err)
						}
					}
					tmpFile.Close()
				}
				task.OutputFile = outputFile

			} else if task.Type == ReduceTask {
				// read intermediate key from files
				intermediate:= []KeyValue{}
				for _, filename :=range task.InputFile {
					file, err:= os.Open(filename)
					if err != nil {
						fmt.Println(err)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
				}
				// sort intermediate pairs by key
				sort.Sort(ByKey(intermediate))

				oname := fmt.Sprintf("mr-out-%d", reply.Task.ID)
				ofile, _ := os.Create(oname)
				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-0.
				//
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
					i = j
				}
			}

			task.Status = Completed
			reply = NotifyTaskDone(task)

		} else if reply.Type == RT_Stop {
			break
		} else if reply.Type == RT_Wait {
			time.Sleep(time.Duration(1) * time.Second)
			reply = AskForTask()
		}
	}
}

//
// function to show how to make an RPC call to the master.
//
func AskForTask() *MasterResponse {
	// fill in the argument(s).
	worker := WorkerWrapper{WorkerID: workerId, Host: workerAddress, CpuAvailable: runtime.NumCPU(), Status:Running}
	worker.CpuAvailable = runtime.NumCPU()

	resp := &MasterResponse{}

	// send the RPC request, wait for the reply.
	call("Master.DistributeTask", &WorkerRequest{WorkerWrapper:worker}, &resp)
	return resp
}

//
// notify master that task has done
//
func NotifyTaskDone(task Task) *MasterResponse {
	workerWrapper := WorkerWrapper{WorkerID: workerId, Host: workerAddress, CpuAvailable: runtime.NumCPU()}
	reply := &MasterResponse{}
	call("Master.DistributeTask", &WorkerRequest{WorkerWrapper:workerWrapper, Task:task}, &reply)
	return reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "localhost"+":8080")
	if err != nil {
		log.Fatal("dialing:", err.Error())
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func GetOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		fmt.Println(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().String()
	idx := strings.LastIndex(localAddr, ":")
	return localAddr[0:idx]
}
