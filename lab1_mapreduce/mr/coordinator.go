package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"

	// "os"
	"time"
)

const (
	READY    = 0
	QUEUE    = 1
	RUNNING  = 2
	FINISHED = 3
	ERR      = -1
)

type Coordinator struct {
	// Your definitions here.
	mu      sync.Mutex
	done    bool
	phase   int
	status  []int
	files   []string
	cur     int
	taskch  chan Task
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

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
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
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
	return c.done
}

func (c *Coordinator) run() {

	c.mu.Lock()
	defer c.mu.Unlock()
	// 任务分配器
	ok := true
	for idx, status := range c.status {
		switch status {
		case READY:
			c.status[idx] = QUEUE
			c.taskch <- c.getTask(idx)
			ok = false
		case QUEUE:
			ok = false
		case RUNNING:
			ok = false
		case ERR:
			c.status[idx] = READY
			ok = false
		}
	}

	if ok { //任务都完成了 
		if c.phase == MAP_PHASE {
			c.phase = REDUCE_PHASE
			c.status = make([]int, c.nReduce) // 初始化status
			// 执行reduce
		} else {
			c.done = true
		}
	}

}

func (c *Coordinator) getTask(idx int) Task {
	ret := Task{
		Phase:    c.phase,
		WorkerId: idx,
		NMap:     len(c.files),
		NReduce:  c.nReduce,
		FileName: "",
	}
	if c.phase == MAP_PHASE {
		ret.FileName = c.files[idx]
	}
	return ret
}

// 用于Worker RPC调用来获取一个任务
func (c *Coordinator) Register(args RegisterArgs, reply *RegisterReply) error {
	task := <-c.taskch
	reply.Task = task // 阻塞，等待接收一个任务

	c.mu.Lock()
	c.status[task.WorkerId] = RUNNING
	c.mu.Unlock()

	return nil
}

func (c *Coordinator) Report(args ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.err != nil {
		log.Fatalln(args.err)
		c.status[args.WorkerId] = ERR
	} else {
		c.status[args.WorkerId] = FINISHED
	}
	
	go c.run()
	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// init
	c := Coordinator{
		files:   files,
		done:    false,
		mu:      sync.Mutex{},
		phase:   MAP_PHASE,
		nReduce: nReduce,
	}
	c.status = make([]int, len(files))
	if len(files) > nReduce {
		c.taskch = make(chan Task, len(files))
	} else {
		c.taskch = make(chan Task, nReduce)
	}

	go func() {
		for !c.Done() {
			go c.run()
			time.Sleep(1 * time.Second)
		}
	}()
	c.server()
	return &c
}
