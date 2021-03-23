package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "encoding/json"
import "os"
import "io/ioutil"
import "regexp"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		atArgs := ATArgs{}
		atReply := ATReply{}
		call("Coordinator.AssignTask", &atArgs, &atReply)
		if atReply.IsAllWorkDone {
			break
		}
		log.Printf("%v\n", atReply)
		if !atReply.IsTaskAssigned {
			time.Sleep(1 * time.Second)
			continue
		}
		t := atReply.Task
		isTaskDone := true
		tmpf2f := make(map[string]string)
		if t.TaskType == MAP {
			file, err := os.Open(t.File)
			if err != nil {
				log.Fatalf("cannot open %v", t.File)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", t.File)
			}
			file.Close()
			kva := mapf(t.File, string(content))
			kvm := make(map[string][]KeyValue)
			for _, kv := range kva {
				rn := ihash(kv.Key) % atReply.NReduce
				imFile := fmt.Sprintf("mr-%s-%d", t.TaskNum, rn)
				_, ok := kvm[imFile]
				if !ok {
					kvm[imFile] = []KeyValue{}
				}
				kvm[imFile] = append(kvm[imFile], kv)
			}
			for imFile, kvma := range kvm {
				tmpFile, err := ioutil.TempFile(".", imFile)
				if err != nil {
					log.Fatalf("cannot create temporary file for %s", imFile)
					isTaskDone = false
					break
				}
				tmpf2f[tmpFile.Name()] = imFile
				enc := json.NewEncoder(tmpFile)
				for _, kv := range kvma {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode key value for %v", kv)
						isTaskDone = false
						break
					}
				}
				tmpFile.Close()
				if !isTaskDone {
					break
				}
			}
		} else {
			// TODO
			files, err := ioutil.ReadDir("./")
			if err != nil {
				log.Fatal(err)
			}
			rtNum := atReply.Task.TaskNum
			reg := fmt.Sprintf("mr-.*-%s", rtNum)
			kva := []KeyValue{}
			for _, file := range files {
				match, _ := regexp.MatchString(reg, file.Name())
				if match {
					// TODO
					f, _ := os.Open(file.Name())
					dec := json.NewDecoder(f)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}
			}

			outfileName := fmt.Sprintf("mr-out-%s", rtNum)
			tmpFile, err := ioutil.TempFile(".", outfileName)
			tmpf2f[tmpFile.Name()] = outfileName
			sort.Sort(ByKey(kva))
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			tmpFile.Close()
		}
		atdArgs := ATDArgs{
			IsTaskDone: isTaskDone,
			Task:       t,
		}
		atdReply := ATDReply{}
		call("Coordinator.AssignedTaskDone", &atdArgs, &atdReply)
		if isTaskDone && atdReply.Committed {
			for tmpf, f := range tmpf2f {
				os.Rename(tmpf, f)
			}
		} else {
			for tmpf, _ := range tmpf2f {
				os.Remove(tmpf)
			}
		}
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

	log.Println(err)
	return false
}
