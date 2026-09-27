package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Finished
)

type TaskArgs struct {
	Pid int
}

type TaskReply struct {
	FileName   string
	TaskType   TaskType
	TaskNumber int
	NReduce    int
	NMap       int
}

type MapTaskReportArgs struct {
	Pid      int
	FileName string
}

type TaskReportReply struct {
	Ok bool
}

type ReduceTaskReportArgs struct {
	Pid        int
	TaskNumber int
}

// Add your RPC definitions here.
