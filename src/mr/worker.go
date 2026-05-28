package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
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
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func Run(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	args := MessageArgs{}
	reply := MessageReply{}
	for call("Coordinator.MR", &args, &reply) {
		args = MessageArgs{
			TaskID:   reply.TaskID,
			ExecType: reply.ExecType,
		}
		switch reply.ExecType {
		case Map:
			// read the file from reply
			b, err := os.ReadFile(reply.FileName)
			if err != nil {
				log.Println("can't open the file", reply.FileName)
				args.State = Failed
				continue
			}
			// output to mr-X-Y
			outputGroupByXY := map[string]*os.File{}
			kvs := mapf("not in use", string(b))
			for _, kv := range kvs {
				fileName := fmt.Sprintf("mr-%d-%d", reply.TaskID, ihash(kv.Key)%reply.ReduceBuckets)
				if v, ok := outputGroupByXY[fileName]; ok {
					if json.NewEncoder(v).Encode(map[string]string{
						kv.Key: kv.Value,
					}) != nil {
						log.Println("write file failed", fileName)
						args.State = Failed
						break
					}
				} else {
					f, err := os.OpenFile(fileName, os.O_TRUNC|os.O_CREATE, os.ModePerm)
					if err != nil {
						log.Println("file create failed", fileName)
						args.State = Failed
						break
					}
					outputGroupByXY[fileName] = f
				}
			}
			// close the files
			for _, f := range outputGroupByXY {
				_ = f.Close()
			}
			if args.State != "" {
				args.State = Finished
			}
		case Reduce:
			// output to mr-out-Y
		default:
			log.Println("leave")
			return
		}
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
