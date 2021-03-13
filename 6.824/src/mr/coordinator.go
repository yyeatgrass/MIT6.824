package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	unstartedTasks []string
	inflightTasks  []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *MrArgs, reply *MrReply) error {
	c.inflightTasks = append(c.inflightTasks, c.unstartedTasks[0])
	c.unstartedTasks = c.unstartedTasks[1:]
	reply.File = c.unstartedTasks[0]
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
		unstartedTasks: files,
	}

	// Your code here.
	c.server()
	return &c
}
