package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
//import "fmt"
import "time"
import "sync"
import "sync/atomic"
import "syscall"

type Mapstate struct{
	InputFile string
	State int //0:未分配 1：正在执行 2：已完成
	Start time.Time //开始运行时间
}

type Reducestate struct{
	State int //0:未分配 1：正在执行 2：已完成
	Start time.Time //开始运行时间
}

type Master struct {
	// Your definitions here.
	MapNum int32 //map任务数
	ReduceNum int32 //reduce任务数
	Stage int32 //当前任务阶段0：map  1:reduce
	Finished int32//已完成任务数
	Timeout time.Duration//最大执行时间
	InputFiles []string //输入文件
	WorkerState map[int]int  //k:pid value:0(空闲）1(正在执行)
	MapTable map[int]Mapstate //map任务状态表
	ReduceTable map[int]Reducestate //reduce任务表
	mutexWorker sync.Mutex
	mutexMapTable sync.Mutex
	mutexReduceTable sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (m*Master) Login(log *LoginArgs,ack *LoginReply) error {
	ack.Ack=false
	m.mutexWorker.Lock()
	m.WorkerState[log.Pid]=0
	m.mutexWorker.Unlock()
	//fmt.Println(log.Pid,"已注册")
	ack.Ack=true
	return nil
}

func (m*Master) SubmitTask(args *SubmitTaskArgs,reply *SubmitReply) error {
	
	m.mutexWorker.Lock()
	m.WorkerState[args.Pid]=0
	m.mutexWorker.Unlock()
	if args.TaskId==-1{
		reply.Ack=true
		return nil
	}
	if args.Stage==0{
		tmp1:=Mapstate{}
		m.mutexMapTable.Lock()
		if m.MapTable[args.TaskId].State != 2{
			tmp1=m.MapTable[args.TaskId]
			tmp1.State=2
			m.MapTable[args.TaskId] = tmp1
			m.Finished++
			reply.Ack=true
		}
		m.mutexMapTable.Unlock()
	} else {
		tmp2:=Reducestate{}
		m.mutexReduceTable.Lock()
		if m.ReduceTable[args.TaskId].State != 2{
			tmp2=m.ReduceTable[args.TaskId]
			tmp2.State=2
			m.ReduceTable[args.TaskId] = tmp2
			m.Finished++
			reply.Ack=true
		}
		m.mutexReduceTable.Unlock()
	}
	return nil
}

func (m *Master) RequestTask(args *RequestTaskArgs,reply *TaskReply) error {
	reply.TaskId=-1
	m.mutexWorker.Lock()
	m.WorkerState[args.Pid]=1
	m.mutexWorker.Unlock()
	if m.Stage==0{
		//查询一个未分配任务或超时任务
		m.mutexMapTable.Lock()
		for k,v := range m.MapTable {
			if v.State==0 {
				v.State=1
				v.Start=time.Now()
				m.MapTable[k]=v
				reply.TaskId=k
				reply.Stage=0
				reply.Filename=v.InputFile
				reply.ReduceNum=int(m.ReduceNum)
				break
			} else if v.State==1 {
				end:=time.Now()
				duration:=end.Sub(v.Start)
				if duration>=m.Timeout {
					v.Start=time.Now()
					m.MapTable[k]=v
					reply.TaskId=k
					reply.Stage=0
					reply.Filename=v.InputFile
					reply.ReduceNum=int(m.ReduceNum)
					break
				}
			}
		}
		m.mutexMapTable.Unlock()
	} else {
		reply.TaskId=-1
		m.mutexReduceTable.Lock()
		for k,v := range m.ReduceTable {
			if v.State==0 {
				v.State=1
				v.Start=time.Now()
				m.ReduceTable[k]=v
				reply.TaskId=k
				reply.Stage=1
				reply.MapNum=int(m.MapNum)
				break
			} else if v.State==1 {
				end:=time.Now()
				duration:=end.Sub(v.Start)
				if duration>=m.Timeout {
					v.Start=time.Now()
					m.ReduceTable[k]=v
					reply.TaskId=k
					reply.Stage=1
					reply.MapNum=int(m.MapNum)
					break
				}
			}
		}
		m.mutexReduceTable.Unlock()	
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
	//fmt.Println("rpc服务开启了")
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if atomic.LoadInt32(&m.Finished)==m.ReduceNum {
		ret=true
	}
	return ret
}

//任务分发
func (m *Master) TaskDistribute() {
	//先分发map
	//fmt.Println("开始分发map任务了")
	atomic.StoreInt32(&m.Finished,0)//置0
	value := atomic.LoadInt32(&m.Finished)
	for value<m.MapNum {
		//扫描map任务表找到未分配的任务
		label:=true
		for label {
			isFind:=false
			m.mutexMapTable.Lock()
			for i:=0;i<int(m.MapNum);i++{
				if m.MapTable[i].State==0{
					isFind=true
					break
				}
			}
			m.mutexMapTable.Unlock()
			if isFind==true{
				//给一个空闲worker发信号但别一直持有锁
				m.mutexWorker.Lock()
				for k,v:= range m.WorkerState {
					if v == 0 {
						SendSignal(k,0)
						break
					}
				}
				m.mutexWorker.Unlock()
			} else {
				label=false
			}
			isFind=false		
		}
		//可以sleep一下等待任务完成
		time.Sleep(time.Second)
		value = atomic.LoadInt32(&m.Finished)
		//fmt.Println("已完成map任务",value)	
	}
	//reduce分发
	//fmt.Println("开始分发reduce任务了")
	atomic.StoreInt32(&m.Stage,1)//设置为reduce阶段
	atomic.StoreInt32(&m.Finished,0)//置0
	value = atomic.LoadInt32(&m.Finished)
	for value<m.ReduceNum {
		//扫描reduce任务表找到未分配的任务
		label:=true
		for label {
			isFind:=false
			m.mutexReduceTable.Lock()
			for i:=0;i<int(m.ReduceNum);i++{
				if m.ReduceTable[i].State==0{
					isFind=true
					break
				}
			}
			m.mutexReduceTable.Unlock()
			if isFind==true{
				//给一个空闲worker发信号但别一直持有锁
				m.mutexWorker.Lock()
				for k,v:= range m.WorkerState {
					if v == 0 {
						SendSignal(k,0)
						break
					}
				}
				m.mutexWorker.Unlock()
			} else {
				label=false
			}
			isFind=false		
		}
		//可以sleep一下等待任务完成	
		time.Sleep(time.Second)
		value = atomic.LoadInt32(&m.Finished)
		//fmt.Println("已完成reduce任务",value)
	}
	//给worker发送信号所有任务已完成
	for k,_ := range m.WorkerState {
		SendSignal(k,1)
	}
}

//给进程发送信号
func SendSignal(pid int,sig int){
	process, err := os.FindProcess(pid)
	if sig==0{
		err = process.Signal(os.Signal(syscall.SIGCONT))
		for err != nil{
			err = process.Signal(os.Signal(syscall.SIGCONT))
		}
	} else {
		err = process.Signal(os.Signal(syscall.SIGKILL))
		for err != nil{
			err = process.Signal(os.Signal(syscall.SIGKILL))
		}
	}
}

//检查当前的执行的任务是否超时
func (m*Master) Check(){
	//fmt.Println("开始检查超时了")
	for{
		if atomic.LoadInt32(&m.Stage)==0{
			m.mutexMapTable.Lock()
			for _,v1 := range m.MapTable{
				if v1.State ==1 {
					end := time.Now()
					duration := end.Sub(v1.Start)
					if duration >= m.Timeout {
						//fmt.Println(i,"号任务超时了")
						//找到一个空闲worker
						m.mutexWorker.Lock()
						for k,v2:= range m.WorkerState {
							if v2 == 0 {
								SendSignal(k,0)
								break
							}
						}	
						m.mutexWorker.Unlock()
					}
				} 
			}
			m.mutexMapTable.Unlock()
			time.Sleep(2*time.Second)
		} else {
			m.mutexReduceTable.Lock()
			for _,v := range m.ReduceTable{
				if v.State ==1 {
					end := time.Now()
					duration := end.Sub(v.Start)
					if duration >= m.Timeout {
						//找到一个空闲worker
						m.mutexWorker.Lock()
						for k,v:= range m.WorkerState {
							if v == 0 {
								SendSignal(k,0)
								break
							}
						}	
						m.mutexWorker.Unlock()
					}
				} 
			}
			m.mutexReduceTable.Unlock()
			time.Sleep(2*time.Second)
		}
	}
}
//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	//初始化Master
	m := Master{}
	m.ReduceNum = int32(nReduce)
	m.MapTable = make(map[int]Mapstate)
	m.ReduceTable = make(map[int]Reducestate)
	m.WorkerState = make(map[int]int)
	m.Stage = 0
	m.MapNum = int32(len(files))
	m.Timeout = 10 * time.Second
	// Your code here.
	ms := Mapstate{}
	for i,file := range files[0:] {
		m.InputFiles = append(m.InputFiles,file)
		ms.InputFile = file
		ms.State = 0
		m.MapTable[i] = ms
	}
	/*
	fmt.Println("this is the input files:")
	for _,file := range m.InputFiles {
		
		fmt.Println(file)
	}
	*/
	//初始化reduce任务表
	rs := Reducestate{}
	for i:=0;i<nReduce;i++{
		rs.State=0
		m.ReduceTable[i]=rs
	}
	
	//开启rpc服务线程	
	m.server()
	//开启check线程检查超时
	go m.Check()
	//开始任务分发可以单独开一个线程
	m.TaskDistribute()
	return &m
}




















