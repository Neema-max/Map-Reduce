package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

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
	requestWork(mapf, reducef, "Trying to Connect to Master...")
}

func performMapTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, job Job, job_id int) {
	file, err := os.Open(job.Files[0])
	if err != nil {
		log.Fatalf("cannot open %v", job.Files)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", job.Files)
	}
	file.Close()
	kva := mapf(job.Files[0], string(content))
	sort.Sort(ByKey(kva))
	tmps := make([]*os.File, job.NReduce)
	files_name := []string{}
	for i := 0; i < job.NReduce; i++ {
		tmps[i], err = ioutil.TempFile("./", "temp_map_")
		if err != nil {
			log.Fatal("cannot create temp file")
		}
	}
	defer func() {
		for i := 0; i < job.NReduce; i++ {
			tmps[i].Close()
		}
	}()

	for _, kv := range kva {
		hash := ihash(kv.Key) % job.NReduce
		fmt.Fprintf(tmps[hash], "%v %v\n", kv.Key, kv.Value)
	}
	for i := 0; i < job.NReduce; i++ {
		taskIdentifier := strings.Split(job.Files[0], "-")[1]
		os.Rename(tmps[i].Name(), "mr-"+taskIdentifier+"-"+strconv.Itoa(i))
		files_name = append(files_name, "mr-"+taskIdentifier+"-"+strconv.Itoa(i))
	}
	newArgs := &CompleteTaskArgs{
		Job_id: job_id,
		Files:  files_name,
	}
	newReply := &EmptyResponse{}
	log.Println("Job finishes, calling Master.CompletedTask")
	log.Printf("Job Type is %v", job.Job_type)
	call("Master.CompletedTask", newArgs, newReply, "Sending Result to Master")
	requestWork(mapf, reducef, "Requesting for work...")
}

func performReduceTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, job Job, job_id int) {
	kvas := make([][]KeyValue, len(job.Files))
	for i, fileName := range job.Files {
		f, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)

		for scanner.Scan() {
			tokens := strings.Split(scanner.Text(), " ")
			kvas[i] = append(kvas[i], KeyValue{tokens[0], tokens[1]})
		}

	}
	ids := make([]int, len(job.Files))
	ofile, _ := ioutil.TempFile("./", "temp_reduce_")
	files_name := []string{}
	defer ofile.Close()
	values := []string{}
	prevKey := ""
	for {
		findNext := false
		var nextI int
		for i, kva := range kvas {
			if ids[i] < len(kva) {
				if !findNext {
					findNext = true
					nextI = i
				} else if strings.Compare(kva[ids[i]].Key, kvas[nextI][ids[nextI]].Key) < 0 {
					nextI = i
				}
			}
		}
		if findNext {
			nextKV := kvas[nextI][ids[nextI]]
			if prevKey == "" {
				prevKey = nextKV.Key
				values = append(values, nextKV.Value)
			} else {
				if nextKV.Key == prevKey {
					values = append(values, nextKV.Value)
				} else {
					fmt.Fprintf(ofile, "%v %v\n", prevKey, reducef(prevKey, values))
					prevKey = nextKV.Key
					values = []string{nextKV.Value}
				}
			}
			ids[nextI]++
		} else {
			break
		}
	}
	if prevKey != "" {
		fmt.Fprintf(ofile, "%v %v\n", prevKey, reducef(prevKey, values))
	}
	taskIdentifier := strings.Split(job.Files[0], "-")[2]
	os.Rename(ofile.Name(), "mr-out-"+taskIdentifier)
	files_name = append(files_name, "mr-out-"+taskIdentifier)
	newArgs := &CompleteTaskArgs{
		Job_id: job_id,
		Files:  files_name,
	}
	newReply := &EmptyResponse{}
	log.Println("Job finishes, calling Master.CompletedTask")
	log.Printf("JobType is %v", job.Job_type)
	call("Master.CompletedTask", newArgs, newReply, "Sending Result to Master")
	requestWork(mapf, reducef, "Requesting for work...")
}

func requestWork(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, msg string) {
	for {
		args := EmptyResponse{}
		reply := GetTaskReply{
			Job_id: -1,
		}
		call("Master.AssignTask", &args, &reply, msg)
		msg = "Sending Heartbeat..."
		if reply.Job_id != -1 {
			if reply.Job.Job_type == "map" {
				performMapTask(mapf, reducef, reply.Job, reply.Job_id)
			} else {
				performReduceTask(mapf, reducef, reply.Job, reply.Job_id)
			}
			return
		}
		time.Sleep(time.Second)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}, msg string) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	var c = &rpc.Client{}
	var err error
	log.Println(msg)
	for {
		sockname := masterSock()
		c, err = rpc.DialHTTP("unix", sockname)
		if err == nil {
			break
		}
	}

	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
