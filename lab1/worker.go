package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "sort"
import "os"
import "os/signal"
import "syscall"
import "encoding/json"
import "strconv"
import "time"


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


//向master注册
func LoginToMaster(pid int,islogin *LoginReply) {
	args := LoginArgs{}
	args.Pid=pid
	call("Master.Login",&args,islogin)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	
	// uncomment to send the Example RPC to the master.
	//CallExample()
	//先注册worker
	islogin := LoginReply{}
	pid:=os.Getpid()
	LoginToMaster(pid,&islogin)
	if islogin.Ack == true {
		//fmt.Println("已注册")
	} else {
		return
	}
	// 创建一个信号接收器
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGCONT)
	for{
		//阻塞等待master信号
		sig := <-signalChan
		if sig==os.Signal(syscall.SIGCONT) {
			//向master发送任务请求
			//fmt.Println("收到任务信号")
			requestArgs := RequestTaskArgs{}
			requestArgs.Pid=pid
			taskInfo := TaskReply{}
			call("Master.RequestTask",&requestArgs,&taskInfo)
			//fmt.Println("已发送请求")
			if taskInfo.TaskId!=-1 {
				//fmt.Println("已获取任务",taskInfo.TaskId)
				if taskInfo.Stage==0{
					Map(mapf,&taskInfo,pid)
					time.Sleep(time.Second)
				} else {
					Reduce(reducef,&taskInfo,pid)
					time.Sleep(time.Second)	
				}
				
			} else {
				//向master提交任务
				//fmt.Println("无效任务")
				subargs:= SubmitTaskArgs{}
				subargs.Pid=pid
				subargs.TaskId=taskInfo.TaskId
				subreply:=SubmitReply{}
				subreply.Ack=false
				call("Master.SubmitTask",&subargs,&subreply)
				if subreply.Ack==false {
					return
				}
			}
			
		} else {
			//结束进程
			return
		}
	}
	
}

func Reduce(reducef func(string, []string) string,TaskInfo *TaskReply,pid int){
	//打开文件mr-X-Y
	Y := TaskInfo.TaskId
	mMap := TaskInfo.MapNum
	f:=[]*os.File{}
	for i:=0;i<mMap;i++ {
		file,err := os.Open("mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(Y)+".json")
		if err!=nil {
			fmt.Println("文件打开失败")
		}
		f=append(f,file)
	}
	//读所有文件内容到intermediate
	intermediate := []KeyValue{}
	for i:=0;i<len(f);i++ {
		dec:=json.NewDecoder(f[i])
		for{
			var kv KeyValue
			if err := dec.Decode(&kv);err!=nil{
				break
			}
			intermediate = append(intermediate,kv)
		}
	}
	
	oname := "mr-out-"+strconv.Itoa(Y)
	ofile, _ := os.Create(oname)
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	//向master提交任务
	subargs:= SubmitTaskArgs{}
	subargs.Pid=pid
	subargs.Stage=1
	subargs.TaskId=TaskInfo.TaskId
	subreply:=SubmitReply{}
	call("Master.SubmitTask",&subargs,&subreply)
}

//
func Map(mapf func(string, string) []KeyValue,TaskInfo *TaskReply,pid int){
	//fmt.Println("正在执行map任务")
	intermediate := []KeyValue{}
	filename := TaskInfo.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	//fmt.Println("已打开",filename)
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	
	//将intermediate分成nReduce份,先创建mReduce个临时文件
	s := "mr-tem-"
	f := []*os.File{}
	nReduce := TaskInfo.ReduceNum
	for i := 0;i<nReduce;i++ {
		tmp:=""
		tmp=s
		tmp=tmp+strconv.Itoa(i)+".json"
		file,err := ioutil.TempFile("",tmp)
		if err != nil{
			panic(err)
		}
		f=append(f,file)		
	}
	enc := []*json.Encoder{}
	for i := 0;i<len(f);i++ {
		tmp:=json.NewEncoder(f[i])
		enc=append(enc,tmp)
	} 
	for i := 0;i<len(intermediate);i++{
		err := enc[ihash(intermediate[i].Key)%nReduce].Encode(&intermediate[i])
		if err !=nil {
			
		}
	}
	//执行完后修改文件名
	name:="mr-"
	for i:=0;i<len(enc);i++ {
		err:=os.Rename(f[i].Name(),name+strconv.Itoa(TaskInfo.TaskId)+"-"+strconv.Itoa(i)+".json")
		if err!=nil{
		
		}
	}
	//向master提交任务
	subargs:= SubmitTaskArgs{}
	subargs.Pid=pid
	subargs.Stage=0
	subargs.TaskId=TaskInfo.TaskId
	subreply:=SubmitReply{}
	call("Master.SubmitTask",&subargs,&subreply)
	
}	

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	} 

	fmt.Println(err)
	return false
}
