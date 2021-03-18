package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import . "github.com/yyeatgrass/go-datastructures/queue"
import cmap "github.com/yyeatgrass/concurrent-map"
import "time"
import "sync"
import "strconv"



type Coordinator struct {
	// Your definitions here.
	mu            sync.Mutex
	usMapTasks    *Queue
	ifMapTasks    cmap.ConcurrentMap
	usReduceTasks *Queue
	ifReduceTasks cmap.ConcurrentMap
	timeOutChan   chan *MrTask
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *MrArgs, reply *MrReply) error {
	if !c.usMapTasks.Empty() {
		mts, err := c.usMapTasks.Get(1)
		if err != nil {
			*reply = MrReply{IsTaskAssigned: false}
			return err
		}
		mt := mts[0].(*MrTask)
		*reply = MrReply{
			IsTaskAssigned: true,
			Task:           *mt,
		}
		c.ifMapTasks.Set(mt.TaskNum, mt)
		go func() {
			time.Sleep(10*time.Second)
			c.timeOutChan <- mt
		}()
		return nil
	}
	if !c.usReduceTasks.Empty() {
		rts, err := c.usReduceTasks.Get(1)
		if err != nil {
			*reply = MrReply{IsTaskAssigned: false}
			return err
		}
		rt := rts[0].(*MrTask)
		*reply = MrReply{
			IsTaskAssigned: true,
			Task:           *rt,
		}
		c.ifReduceTasks.Set(rt.TaskNum, rt)
		go func() {
			time.Sleep(10*time.Second)
			c.timeOutChan <- rt
		}()
		return nil
	}

	*reply = MrReply{IsTaskAssigned: false}
	if c.ifMapTasks.IsEmpty() && c.ifReduceTasks.IsEmpty() {
		reply.IsAllWorkDone = true
	}
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		usMapTasks:    new(Queue),
		ifMapTasks:    cmap.New(),
		usReduceTasks: new(Queue),
		ifReduceTasks: cmap.New(),
		timeOutChan:   make(chan *MrTask),
	}
	for i, f := range files {
		c.usMapTasks.Put(
			&MrTask{
				TaskType: MAP,
				TaskNum:  strconv.Itoa(i),
				File:     f,
			},
		)
	}
	fmt.Printf("usMapTasks: %v", c.usMapTasks)
	go func() {
		for c.Done() == false {
			t := <- c.timeOutChan
			var ifTasks cmap.ConcurrentMap
			var usTaskQueue *Queue
			if t.TaskType == MAP {
				ifTasks = c.ifMapTasks
				usTaskQueue = c.usMapTasks
			} else {
				ifTasks = c.ifReduceTasks
				usTaskQueue = c.usReduceTasks
			}
			c.mu.Lock()
			_, ok := ifTasks.Get(t.TaskNum)
			if ok {
				ifTasks.Remove(t.TaskNum)
				c.mu.Unlock()
				usTaskQueue.Put(t)
			} else {
				c.mu.Unlock()
			}
		}
	}()
	// Your code here.
	c.server()
	return &c
}