package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"

// sorting by Key
type ByKey []KeyValue
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
  for true {
    args := AllocateTaskArgs{}
    reply := AllocateTaskReply{}
    call("Coordinator.AllocateTask", &args, &reply)
    if reply.TaskIsMap {
      computeMapTask(mapf, reply.Filename, reply.MapTaskNum, reply.NumReduce)
      args := CompleteTaskArgs{}
      args.LastTaskFile = reply.Filename
      args.LastTaskIsMap = true
      reply := CompleteTaskReply{}
      call("Coordinator.CompleteTask", &args, &reply)
    } else if reply.TaskIsReduce {
      computeReduceTask(reducef, reply.BucketNum, reply.MapTaskNum)
      args := CompleteTaskArgs{}
      args.LastTaskBucket = reply.BucketNum
      args.LastTaskIsMap = false
      reply := CompleteTaskReply{}
      call("Coordinator.CompleteTask", &args, &reply)
    }
  }
	
  // uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func computeMapTask(mapf func(string, string) []KeyValue, filename string, mapTaskNum int, nReduce int) {
    file, err := os.Open(filename)
    if err != nil {
      log.Fatalf("cannot open %v", file)
    }
    content, err := ioutil.ReadAll(file)
    if err != nil {
      log.Fatalf("cannot read %v", file)
    }
    file.Close()
    kva := mapf(filename, string(content))
    kvaBuckets := Partition(kva, nReduce)
    for i := 0; i < nReduce; i++ {
      outputFilename := "mr-" + strconv.Itoa(mapTaskNum) + "-" + strconv.Itoa(i)
      output, _ := os.Create(outputFilename)
      enc := json.NewEncoder(output)
      for _, kv := range kvaBuckets[i] {
        enc.Encode(&kv)
      }
      output.Close()
    }
}

func computeReduceTask(reducef func(string, []string) string, bucket int, mapNum int) {
  intermediate := []KeyValue{}
  for i := 0; i < mapNum; i++ {
    inputFilename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(bucket)
    if _, err := os.Stat(inputFilename); err != nil {
      continue
    }
    file, err := os.Open(inputFilename)
    if err != nil {
      log.Fatalf("cannot open %v", inputFilename)
    }
    dec := json.NewDecoder(file)
    for {
      var kv KeyValue
      if err := dec.Decode(&kv); err != nil {
        break
      }
      intermediate = append(intermediate, kv)
    }
    file.Close()
  }
  sort.Sort(ByKey(intermediate))
  
  oname := "mr-out-" + strconv.Itoa(bucket)
  ofile, _ := os.Create(oname)

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
    fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
    i = j
  }

  ofile.Close()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func Partition(kva []KeyValue, nReduce int) [][]KeyValue {
  kvaBuckets := make([][]KeyValue, nReduce)
  for _, kv := range kva {
    idx := ihash(kv.Key) % nReduce
    kvaBuckets[idx] = append(kvaBuckets[idx], kv)
  }
  return kvaBuckets
}
