package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import . "github.com/yyeatgrass/go-datastructures/queue"

type Coordinator struct {
	// Your definitions here.
	usMapTasks    *Queue
	ifMapTasks    map[int]*MrTask
	usReduceTasks *Queue
	ifReduceTasks map[int]*MrTask
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
		c.ifMapTasks[mt.TaskNum] = mt
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
		c.ifReduceTasks[rt.TaskNum] = rt
		return nil
	}

	*reply = MrReply{IsTaskAssigned: false}
	if len(c.ifMapTasks) == 0 && len(c.ifReduceTasks) == 0 {
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
		ifMapTasks:    map[int]*MrTask{},
		usReduceTasks: new(Queue),
		ifReduceTasks: map[int]*MrTask{},
	}
	for i, f := range files {
		c.usMapTasks.Put(
			&MrTask{
				TaskType: MAP,
				TaskNum:  i,
				File:     f,
			},
		)
	}
	fmt.Printf("usMapTasks: %s", c.usMapTasks)
	// Your code here.
	c.server()
	return &c
}
