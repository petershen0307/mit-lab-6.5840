package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"regexp"
)

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
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	Run(mapf, reducef)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and getTaskOutput types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	reportTaskInput := ExampleArgs{}

	// fill in the argument(s).
	reportTaskInput.X = 99

	// declare a getTaskOutput structure.
	getTaskOutput := ExampleReply{}

	// send the RPC request, wait for the getTaskOutput.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &reportTaskInput, &getTaskOutput)
	if ok {
		// getTaskOutput.Y should be 100.
		fmt.Printf("getTaskOutput.Y %v\n", getTaskOutput.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

type KV struct {
	K string
	V string
}

func Run(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	for {
		getTaskOutput := GetTaskOutput{}
		call("Coordinator.GetTask", &GetTaskInput{}, &getTaskOutput)
		reportTaskInput := ReportTaskInput{
			TaskID:   getTaskOutput.TaskID,
			ExecType: getTaskOutput.ExecType,
			State:    Finished,
		}
		switch getTaskOutput.ExecType {
		case Map:
			// read the file from getTaskOutput
			b, err := os.ReadFile(getTaskOutput.FileName)
			if err != nil {
				log.Println("can't open the file", getTaskOutput.FileName)
				reportTaskInput.State = Failed
				continue
			}
			// output to mr-X-Y
			outputGroupByXY := map[string]*os.File{}
			kvs := mapf("not in use", string(b))
			for _, kv := range kvs {
				fileName := fmt.Sprintf("mr-%d-%d", getTaskOutput.TaskID, ihash(kv.Key)%getTaskOutput.ReduceBuckets)
				if _, ok := outputGroupByXY[fileName]; !ok {
					f, err := os.OpenFile(fileName, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.FileMode(0o666))
					if err != nil {
						log.Println("file create failed", fileName)
						reportTaskInput.State = Failed
						break
					}
					outputGroupByXY[fileName] = f
				}
				if json.NewEncoder(outputGroupByXY[fileName]).Encode(KV{
					K: kv.Key, V: kv.Value,
				}) != nil {
					log.Println("write file failed", fileName)
					reportTaskInput.State = Failed
					break
				}
			}
			// close the files
			for _, f := range outputGroupByXY {
				_ = f.Close()
			}
		case Reduce:
			// output to mr-out-Y
			// read all mr-*-Y files
			dirs, err := os.ReadDir("./")
			if err != nil {
				reportTaskInput.State = Failed
				return
			}
			r := make(map[string]int)
			for _, d := range dirs {
				if d.IsDir() {
					continue
				}
				if b, _ := regexp.MatchString(fmt.Sprintf(`mr-\d{1}-%d`, getTaskOutput.TaskID), d.Name()); !b {
					continue
				}
				f, err := os.OpenFile(d.Name(), os.O_RDONLY, os.ModePerm)
				if err != nil {
					log.Println("file create failed", d.Name())
					reportTaskInput.State = Failed
					return
				}
				reader := json.NewDecoder(f)
				kv := KV{}
				for reader.Decode(&kv) != nil {
					r[kv.K] += 1
				}
				f.Close()
			}
			outf, err := os.OpenFile(fmt.Sprintf("mr-out-%d", getTaskOutput.TaskID), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.FileMode(0o666))
			if err != nil {
				reportTaskInput.State = Failed
				return
			}
			if json.NewEncoder(outf).Encode(r) != nil {
				reportTaskInput.State = Failed
				return
			}
			reportTaskInput.State = Finished
		default:
			log.Println("leave")
			return
		}
		call("Coordinator.ReportTask", &reportTaskInput, &ReportTaskOutput{})
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
