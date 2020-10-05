package mr

// type of task.TaskType
const (
	TypeMap = iota + 1
	TypeReduce
)

// status of task.Status
const (
	StatusReady = iota + 1
	StatusSent
	StatusFinish
)

// errcode
const (
	ErrSuccess = iota
	ErrWait
	ErrAllDone
)

type Task struct {
	TaskId   int32
	TaskType int32
	Content  string // map task => file_name , reduce task => reduce number
	Status   int32
}

type GetTaskRequest struct {
}

type GetTaskResponse struct {
	ErrCode int32
	Task    Task
}

type NoticeRequest struct {
	TaskId   int32
	TaskType int32
}

type NoticeResponse struct {
}
