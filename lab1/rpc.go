package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
	
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type LoginArgs struct {
	Pid int
}

type LoginReply struct {
	Ack bool
}

type RequestTaskArgs struct {
	Pid int
}

type TaskReply struct {
	Stage int//任务类型
	TaskId int//任务号
	Filename string//输入文件名
	MapNum int//map任务数
	ReduceNum int//reduce任务数
}

type SubmitTaskArgs struct {
	Pid int//worker id
	Stage int//提交任务类型
	TaskId int//任务号
}

type SubmitReply struct {
	Ack bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
