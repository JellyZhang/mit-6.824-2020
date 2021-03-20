package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// status of coordinator.Status
const (
	MapPeroid = iota + 1
	ReducePeroid
	AllDone
)

const WorkerDieTime = 10 * time.Second

type Coordinator struct {
	taskQueue []*Task
	index     int
	mu        sync.Mutex
	status    int32 // definitions above.
	nReduce   int32
}

// Your code here -- RPC handlers for the worker to call.

func (m *Coordinator) GetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	//log.Println("[Coordinator.GetTask]")
	m.mu.Lock()
	defer m.mu.Unlock()

	// if all work is done, tell worker to shut down.
	if m.status == AllDone {
		resp.ErrCode = ErrAllDone
		return nil
	}

	hasWaiting := false
	for cnt := 0; cnt < len(m.taskQueue); cnt++ {
		v := m.taskQueue[m.index]
		m.index = (m.index + 1) % len(m.taskQueue)
		//log.Printf("taskId=%v, taskType=%v, status=%v", v.TaskId, v.TaskType, v.Status)

		if v.Status == StatusReady {
			// 1.1 find a ready task, just send it.
			v.Status = StatusSent
			resp.ErrCode = ErrSuccess
			resp.Task = *v

			// 1.2 check this task after 10 seconds.
			go checkTask(m, v.TaskId, v.TaskType)
			//log.Printf("sent taskId=%v", v.TaskId)
			return nil
		} else if v.Status == StatusSent {
			hasWaiting = true
		}
	}

	// 2. some task is given out and not finished yet.
	// so tell this worker to standby and waiting for other worker to finish.
	if hasWaiting {
		//log.Println("tell worker to wait")
		resp.ErrCode = ErrWait
		return nil
	}

	// 3. program come to this line meaning all tasks in m.taskQueue is finished.
	switch m.status {
	case MapPeroid:
		//log.Println("now reducing")

		// 3.1.1 map finished, change to reduce-period
		m.status = ReducePeroid

		// 3.1.2 Load reduce tasks.
		loadReduceTasks(m)

		// 3.1.3 tell worker to come by later
		resp.ErrCode = ErrWait
		return nil
	case ReducePeroid:
		//log.Println("now all done")

		// 3.2 reduce finished, tell the worker to close.
		m.status = AllDone
		resp.ErrCode = ErrAllDone
		return nil
	}

	return nil
}

// load all map tasks to m.TaskMap
func loadMapTasks(m *Coordinator, files []string) {
	m.taskQueue = make([]*Task, 0)
	m.index = 0
	for i := 0; i < len(files); i++ {
		m.taskQueue = append(m.taskQueue, &Task{
			TaskId:   int32(i),
			TaskType: TypeMap,
			Content:  files[i],
			Status:   StatusReady,
		})
	}
}

// load all reduce tasks to m.TaskMap
func loadReduceTasks(m *Coordinator) {
	m.taskQueue = make([]*Task, 0)
	m.index = 0
	for i := 0; int32(i) < m.nReduce; i++ {
		m.taskQueue = append(m.taskQueue, &Task{
			TaskId:   int32(i),
			TaskType: TypeReduce,
			Content:  fmt.Sprint(m.nReduce),
			Status:   StatusReady,
		})
	}
}

// check one task is finished or not after it given out 10 seconds.
// if Not finished, meaning that worker might creshed. Reset the task.Status to Ready, so that it can be given out again.
func checkTask(m *Coordinator, taskId int32, taskType int32) {
	time.Sleep(WorkerDieTime)
	//log.Printf("[checkTask] taskId=%v, taskType=%v", taskId, taskType)
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.taskQueue[taskId].TaskType != taskType {
		//log.Println("Too old taskType, ignore")
		// this means we are already at reduce-period, and this func try to check one of the map-tasks.
		// since we can get into reduce-period, which means map-tasks all done.
		// so just ignore it.
		return
	}
	if m.taskQueue[taskId].Status == StatusSent {
		//log.Printf("put taskId=%v back", taskId)
		m.taskQueue[taskId].Status = StatusReady
	}
}

// Notified by worker about which task is done.
func (m *Coordinator) Notify(req *NotifyRequest, resp *NotifyResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	//log.Printf("[Notify], taskId=%v", req.TaskId)

	if m.taskQueue[req.TaskId].TaskType != req.TaskType {
		//log.Println("Notify too old")
		// old notify, ignore.
		return nil
	}

	switch m.taskQueue[req.TaskId].Status {
	case StatusFinish:
		//log.Printf("already done %v", req.TaskId)
		return nil

	case StatusReady, StatusSent:
		//log.Printf("set %v to finish", req.TaskId)
		m.taskQueue[req.TaskId].Status = StatusFinish
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Coordinator) server() {
	rpc.Register(m)
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
func (m *Coordinator) Done() bool {
	ret := false
	// Your code here.

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status == AllDone {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := Coordinator{
		status:  MapPeroid,
		nReduce: int32(nReduce),
	}

	// Your code here.
	loadMapTasks(&m, files)

	m.server()
	return &m
}
