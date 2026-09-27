package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		task := FetchTask()
		switch task.TaskType {
		case Map:
			file, err := os.Open(task.FileName)
			if err != nil {
				log.Fatalf("Can not open file name %v %s", task.FileName, err)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("Can not read file %v %s", task.FileName, err)
			}
			ikv := mapf(task.FileName, string(content))
			part := make(map[int][]KeyValue)
			for _, v := range ikv {
				y := ihash(v.Key) % task.NReduce
				part[y] = append(part[y], v)
			}
			for k, v := range part {
				var mrxy = "mr-" + strconv.Itoa(task.TaskNumber) + "-" + strconv.Itoa(k)
				mrFile, err := os.Create(mrxy)
				if err != nil {
					log.Fatalf("Cant open file %v", mrxy)
				}
				enc := json.NewEncoder(mrFile)
				for _, v2 := range v {
					_ = enc.Encode(&v2)
				}
				mrFile.Close()
			}
			OnMapTaskCompleted(os.Getpid(), task.FileName)
		case Reduce:
			kvi := make([]KeyValue, 0)
			for i := 0; i < task.NMap; i++ {
				fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.TaskNumber)
				rmrFile, err := os.Open(fileName)
				if err != nil {
					// do nothing
					// log.Printf("Can't read %v %s", fileName, err)
				} else {
					dec := json.NewDecoder(rmrFile)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kvi = append(kvi, kv)
					}
				}
				// log.Printf("Completed reduce file %v", fileName)

			}
			sort.Sort(ByKey(kvi))
			oname := "mr-out-" + strconv.Itoa(task.TaskNumber)
			ofile, _ := os.OpenFile(oname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			i := 0
			for i < len(kvi) {
				j := i + 1
				for j < len(kvi) && kvi[j].Key == kvi[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kvi[k].Value)
				}
				output := reducef(kvi[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kvi[i].Key, output)
				i = j
			}
			ofile.Close()
			OnReduceTaskCompleted(task.TaskNumber)
		case Wait:
			time.Sleep(2 * time.Second)
			// log.Printf("[%v] Waiting ...", os.Getpid())
		case Finished:
			// log.Printf("[%v] Finished", os.Getpid())
			return
		}
	}

}

func FetchTask() TaskReply {
	args := TaskArgs{Pid: os.Getpid()}
	reply := TaskReply{}
	for range 3 {
		ok := call("Coordinator.FetchTask", &args, &reply)
		if ok {
			return reply
		} else {
			log.Println("RPC call failed, rety")
		}
	}

	log.Fatalln("Retried 3 times, can't reach the coodinator")
	return TaskReply{TaskType: Finished}
}

func OnMapTaskCompleted(pid int, fileName string) TaskReportReply {
	args := MapTaskReportArgs{Pid: pid, FileName: fileName}
	reply := TaskReportReply{}
	ok := call("Coordinator.OnMapTaskCompleted", &args, &reply)
	if ok {
		return reply
	} else {
		log.Println("Failed to pass intermediate files")
		return reply
	}
}

func OnReduceTaskCompleted(taskNumber int) TaskReportReply {
	args := ReduceTaskReportArgs{Pid: os.Getpid(), TaskNumber: taskNumber}
	reply := TaskReportReply{}
	ok := call("Coordinator.OnReduceTaskCompleted", &args, &reply)
	if ok {
		return reply
	} else {
		log.Println("Failed on OnReduceTaskCompleted")
		return reply
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
