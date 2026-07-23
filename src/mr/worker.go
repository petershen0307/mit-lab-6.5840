package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"slices"
	"strings"
	"time"
)

func init() {
	// set the log
	// log.SetOutput(io.Discard)
}

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
			reportTaskInput.State = workerDoMap(getTaskOutput, mapf)
		case Reduce:
			reportTaskInput.State = workerDoReduce(getTaskOutput, reducef)
		case Wait:
			time.Sleep(10 * time.Millisecond)
			continue
		default:
			// log.Println("leave")
			return
		}
		// log.Println("[Report]", getTaskOutput.ExecType, getTaskOutput.TaskID)
		call("Coordinator.ReportTask", &reportTaskInput, &ReportTaskOutput{})
	}
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

const mapIntermediateFile = `mr-%d-%d`

func workerDoMap(getTaskOutput GetTaskOutput, mapf func(string, string) []KeyValue) TaskState {
	// read the file from getTaskOutput
	b, err := os.ReadFile(getTaskOutput.FileName)
	if err != nil {
		log.Println("[MAP] can't open the file", getTaskOutput.FileName)
		return Failed
	}
	// output to mr-X-Y
	// collect all intermediate files
	intermediateFileWriterMap := map[string]*bufio.Writer{}
	kvs := mapf(getTaskOutput.FileName, string(b))
	slices.SortStableFunc(kvs, func(a, b KeyValue) int {
		return strings.Compare(a.Key, b.Key)
	})
	for _, kv := range kvs {
		fileName := fmt.Sprintf(mapIntermediateFile, getTaskOutput.TaskID, ihash(kv.Key)%getTaskOutput.ReduceBuckets)
		if _, ok := intermediateFileWriterMap[fileName]; !ok {
			f, err := os.OpenFile(fileName, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.FileMode(0o666))
			if err != nil {
				log.Println("[MAP] file create failed", fileName)
				return Failed
			}
			// registered closer
			defer f.Close()
			intermediateFileWriterMap[fileName] = bufio.NewWriter(f)
		}
		if _, err := intermediateFileWriterMap[fileName].Write(fmt.Appendln(nil, kv.Key, kv.Value)); err != nil {
			log.Println("[MAP] write file failed", fileName, err)
			return Failed
		}
	}
	for _, writer := range intermediateFileWriterMap {
		writer.Flush()
	}
	return Finished
}

func workerDoReduce(getTaskOutput GetTaskOutput, reducef func(string, []string) string) TaskState {
	// output to mr-out-Y
	// read all mr-*-Y files
	mapFiles := []string{}
	for _, mapTaskID := range getTaskOutput.MapTaskIDs {
		mapFiles = append(mapFiles, fmt.Sprintf(mapIntermediateFile, mapTaskID, getTaskOutput.TaskID))
	}
	keyValues := make(map[string][]string)
	for _, fileName := range mapFiles {
		_, err := os.Stat(fileName)
		if os.IsNotExist(err) {
			continue
		}
		f, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
		if err != nil {
			log.Println("[REDUCE] open file failed", fileName, err)
			return Failed
		}
		defer f.Close()
		// file format: k v
		reader := bufio.NewScanner(f)
		for reader.Scan() {
			t := strings.Split(reader.Text(), " ")
			if len(t) != 2 {
				log.Println("[REDUCE] string split size is not 2", f.Name(), t)
				continue
			}
			k, v := t[0], t[1]
			keyValues[k] = append(keyValues[k], v)
		}
	}
	outf, err := os.OpenFile(fmt.Sprintf("mr-out-%d", getTaskOutput.TaskID), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.FileMode(0o666))
	if err != nil {
		log.Println("[REDUCE] create output file failed", err)
		return Failed
	}
	defer outf.Close()
	writer := bufio.NewWriter(outf)
	for k, v := range keyValues {
		if _, err := writer.Write(fmt.Appendln(nil, k, reducef(k, v))); err != nil {
			log.Println("[REDUCE] write result file failed", err)
			return Failed
		}
	}
	writer.Flush()
	return Finished
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
