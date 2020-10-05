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

// status of master.Status
const (
	MapPeroid = iota + 1
	ReducePeroid
	AllDone
)

type Master struct {
	// Your definitions here.
	taskMap map[int32]*Task
	mu      sync.Mutex
	status  int32 // definitions above.
	nReduce int32
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	waitingTask := 0
	for _, v := range m.taskMap {

		if v.Status == StatusReady {
			// 1.1 find a ready task, just send it.
			v.Status = StatusSent
			resp.ErrCode = ErrSuccess
			resp.Task = *v

			// 1.2 check this task after 10 seconds.
			go checkTask(m, v.TaskId, v.TaskType)
			return nil
		} else if v.Status == StatusSent {
			waitingTask++
		}
	}

	// 2. some task is given out and not finished yet.
	// so tell this worker to standby and waiting for his colleagues to finish.
	if waitingTask > 0 {
		resp.ErrCode = ErrWait
		return nil
	}

	// 3. program come to this line meaning all tasks in m.taskMap is finished.
	switch m.status {
	case MapPeroid:
		// 3.1.1 map finished, change to reduce-period
		m.status = ReducePeroid

		// 3.1.2 Load reduce tasks.
		loadReduceTasks(m)

		// 3.1.3 pick the first task to given out.(in fact, any is ok).
		resp.Task = *m.taskMap[0]
		resp.ErrCode = ErrSuccess
		return nil
	case ReducePeroid:
		// 3.2 reduce finished, tell the worker to close.
		m.status = AllDone
		resp.ErrCode = ErrAllDone
		return nil
	case AllDone:
		// 3.3 all done, tell the worker to close.
		resp.ErrCode = ErrAllDone
		return nil

	}

	return nil
}

// load all map tasks to m.TaskMap
func loadMapTasks(m *Master, files []string) {
	m.taskMap = make(map[int32]*Task)
	for i := 0; i < len(files); i++ {
		m.taskMap[int32(i)] = &Task{
			TaskId:   int32(i),
			TaskType: TypeMap,
			Content:  files[i],
			Status:   StatusReady,
		}
	}
}

// load all reduce tasks to m.TaskMap
func loadReduceTasks(m *Master) {
	m.taskMap = make(map[int32]*Task)
	for i := 0; int32(i) < m.nReduce; i++ {
		m.taskMap[int32(i)] = &Task{
			TaskId:   int32(i),
			TaskType: TypeReduce,
			Content:  fmt.Sprint(m.nReduce),
			Status:   StatusReady,
		}
	}
}

// check one task is finished or not after it given out 10 seconds.
// if Not finished, meaning that worker might creshed. Reset the task.Status to Ready, so that it can be given out again.
func checkTask(m *Master, taskId int32, taskType int32) {
	time.Sleep(10 * time.Second)
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.taskMap[taskId].TaskType != taskType {
		// this means we are already at reduce-period, and this func try to check one of the map-tasks.
		// since we can get into reduce-period, which means map-tasks all done.
		// so just ignore it.
		return
	}
	if m.taskMap[taskId].Status == StatusSent {
		m.taskMap[taskId].Status = StatusReady
	}
}

// Noticed by worker to know which task is done.
func (m *Master) Notice(req *NoticeRequest, resp *NoticeResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.taskMap[req.TaskId].TaskType != req.TaskType {
		// old notice, ignore.
		return nil
	}

	switch m.taskMap[req.TaskId].Status {
	case StatusFinish:
		return nil
	case StatusReady, StatusSent:
		m.taskMap[req.TaskId].Status = StatusFinish
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status == AllDone {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		status:  MapPeroid,
		nReduce: int32(nReduce),
	}

	// Your code here.
	loadMapTasks(&m, files)

	m.server()
	return &m
}
