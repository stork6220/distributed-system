package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

var unAllocatedFiles   chan string
var unAllocatedBuckets chan int 

type Coordinator struct {
	// Your definitions here.
  mu                 sync.Mutex
  MapStatus          map[string]int64
  ReduceStatus       map[int]int64
  MapTaskNum         int
  nReduce            int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AllocateTask(args *AllocateTaskArgs, reply *AllocateTaskReply) error {
  c.mu.Lock()
  defer c.mu.Unlock()

  for len(c.MapStatus) > 0 || len(unAllocatedFiles) > 0 {
    curTime := time.Now().Unix()
    if len(unAllocatedFiles) > 0 {
      reply.TaskIsMap = true
      reply.TaskIsReduce = false
      reply.Filename = <- unAllocatedFiles
      reply.MapTaskNum, c.MapTaskNum = c.MapTaskNum, c.MapTaskNum + 1
      reply.NumReduce = c.nReduce
      c.MapStatus[reply.Filename] = curTime
      return nil
    } else {
      for file, timestamp := range c.MapStatus {
        if curTime - timestamp > 10 {
          delete(c.MapStatus, file);
          unAllocatedFiles <- file
        }
      }
    }
    c.mu.Unlock()
    time.Sleep(1000)
    c.mu.Lock()
  }

  for len(c.ReduceStatus) > 0 || len(unAllocatedBuckets) > 0 {
    curTime := time.Now().Unix()
    if len(unAllocatedBuckets) > 0 {
      reply.TaskIsMap = false
      reply.TaskIsReduce = true
      reply.BucketNum = <-unAllocatedBuckets
      reply.MapTaskNum = c.MapTaskNum
      reply.NumReduce = c.nReduce
      c.ReduceStatus[reply.BucketNum] = curTime
      return nil
    } else {
      for bucket, timestamp := range c.ReduceStatus {
        if curTime - timestamp > 10 {
          delete(c.ReduceStatus, bucket)
          unAllocatedBuckets <- bucket
        }
      }
    }
    c.mu.Unlock()
    time.Sleep(1000)
    c.mu.Lock()
  } 
  
  return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
  c.mu.Lock()
  if args.LastTaskIsMap {
    delete(c.MapStatus, args.LastTaskFile)
  } else {
    delete(c.ReduceStatus, args.LastTaskBucket)
  }
  c.mu.Unlock()
  return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
  c.mu.Lock()
  defer c.mu.Unlock()

	return len(c.MapStatus) == 0 && len(c.ReduceStatus) == 0 && len(unAllocatedFiles) == 0 && len(unAllocatedBuckets) == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
  c.MapTaskNum = 0
  c.nReduce = nReduce
  c.MapStatus = make(map[string]int64)
  c.ReduceStatus = make(map[int]int64)
  unAllocatedFiles = make(chan string, 10)
  unAllocatedBuckets = make(chan int, 10)
  for _, file := range files {
    unAllocatedFiles <- file
  }
  for i := 0; i < nReduce; i++ {
    unAllocatedBuckets <- i
  }

	c.server()
	return &c
}
