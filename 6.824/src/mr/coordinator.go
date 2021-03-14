package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"

type Coordinator struct {
	// Your definitions here.
	usMapTasks    []string
	ifMapTasks    []string
	usReduceTasks []string
	ifReduceTasks []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *MrArgs, reply *MrReply) error {
	if len(c.usMapTasks) > 0 {
		*reply = MrReply{
			IsTaskAssigned: true,
			TaskType:       MAP,
			File:           c.usMapTasks[0],
		}
		c.ifMapTasks = append(c.ifMapTasks, c.usMapTasks[0])
		c.usMapTasks = c.usMapTasks[1:]
		return nil
	}
	if len(c.usReduceTasks) > 0 {
		*reply = MrReply{
			IsTaskAssigned: true,
			TaskType:       REDUCE,
			File:           c.usReduceTasks[0],
		}
		c.ifReduceTasks = append(c.ifReduceTasks, c.usReduceTasks[0])
		c.usReduceTasks = c.usReduceTasks[1:]
		return nil
	}
	*reply = MrReply{
		IsTaskAssigned: false,
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
		usMapTasks: files,
	}
	fmt.Printf("usMapTasks: %s", c.usMapTasks)
	// Your code here.
	c.server()
	return &c
}
