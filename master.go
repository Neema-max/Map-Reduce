package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Job struct {
	Files    []string
	Job_type string
	NReduce  int
}

type Master struct {
	jobs_count             int
	remaining_map_tasks    int
	remaining_reduce_tasks int
	pending_jobs           chan Job
	reduce_jobs            []Job
	completed_jobs         map[int][]string
	completed_jobsMutex    sync.RWMutex
}

func (m *Master) ExpectedResponsePeriod(job_ *Job, job_id_ *int) error {
	job := *job_
	job_id := *job_id_
	for timeout := time.After(10 * time.Second); ; {
		select {
		case <-timeout:
			m.pending_jobs <- job
			return nil
		default:
			m.completed_jobsMutex.RLock()
			if len(m.completed_jobs[job_id]) > 0 {
				if job.Job_type == "map" {
					m.remaining_map_tasks--
					for i := 0; i < len(m.reduce_jobs); i++ {
						m.reduce_jobs[i].Files = append(m.reduce_jobs[i].Files, m.completed_jobs[job_id][i])
					}
				} else {
					m.remaining_reduce_tasks--
				}
				m.completed_jobsMutex.RUnlock()
				return nil
			}
			m.completed_jobsMutex.RUnlock()
		}
	}
}

func (m *Master) AssignTask(args *EmptyResponse, reply *GetTaskReply) error {
	if m.remaining_map_tasks == 0 {
		m.remaining_map_tasks = -1
		for i := 0; i < len(m.reduce_jobs); i++ {
			m.pending_jobs <- m.reduce_jobs[i]
		}
	}
	if len(m.pending_jobs) == 0 {
		return nil
	}
	m.jobs_count += 1
	reply.Job = <-m.pending_jobs
	reply.Job_id = m.jobs_count
	go m.ExpectedResponsePeriod(&reply.Job, &reply.Job_id)
	return nil
}

func (m *Master) CompletedTask(args *CompleteTaskArgs, reply *EmptyResponse) error {
	m.completed_jobsMutex.Lock()
	m.completed_jobs[args.Job_id] = args.Files
	m.completed_jobsMutex.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	Jobs := make(chan Job, 100)
	for _, file := range files {
		Jobs <- Job{
			Files:    []string{file},
			Job_type: "map",
			NReduce:  nReduce,
		}
	}
	m := Master{
		jobs_count:             0,
		remaining_map_tasks:    len(files),
		remaining_reduce_tasks: nReduce,
		pending_jobs:           Jobs,
		reduce_jobs:            make([]Job, nReduce),
		completed_jobs:         make(map[int][]string),
		completed_jobsMutex:    sync.RWMutex{},
	}

	for i := 0; i < nReduce; i++ {
		m.reduce_jobs[i] = Job{
			Files:    []string{},
			Job_type: "reduce",
			NReduce:  nReduce,
		}
	}

	m.server()
	return &m
}
