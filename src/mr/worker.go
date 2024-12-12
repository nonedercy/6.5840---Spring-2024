package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type tFile struct {
	name string
	f    *os.File
	enc  *json.Encoder
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for t, id := TASK_IDLE, -1; t != TASK_DONE; t, id = CallTask(mapf, reducef, t, id) {
	}
}

func CallTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string, t, id int) (int, int) {
	args := TaskArgs{T: t, Id: id}
	reply := TaskReply{}
	ok := call("Coordinator.Task", &args, &reply)
	if !ok {
		log.Fatalf("RPC call failed")
	}
	switch reply.T {
	case TASK_MAP:
		doMap(mapf, reply.Id, reply.Filename, reply.NReduce)
	case TASK_REDUCE:
		doReduce(reducef, reply.Id)
	case TASK_IDLE:
		time.Sleep(1 * time.Second)
	case TASK_DONE:
	}
	return reply.T, reply.Id
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	fmt.Println(err)
	return false
}

func doMap(mapf func(string, string) []KeyValue, id int, filename string, nReduce int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("cannot getwd")
	}
	tfs := make([]*tFile, nReduce)
	for i := 0; i < nReduce; i += 1 {
		name := fmt.Sprintf("mr-%v-%v", id, i)
		f, err := os.CreateTemp(pwd, name)
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		tfs[i] = &tFile{name, f, enc}
	}
	for _, kv := range kva {
		i := ihash(kv.Key) % nReduce
		err := tfs[i].enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode kv to file")
		}
	}
	for _, tf := range tfs {
		tf.f.Close()
		err := os.Rename(tf.f.Name(), tf.name)
		if err != nil {
			log.Fatalf("cannot rename file")
		}
	}
}

func doReduce(reducef func(string, []string) string, id int) {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("cannot getwd")
	}

	pattern := fmt.Sprintf(`^mr-[0-9]+-%v$`, id)
	//fmt.Println(pattern)
	re := regexp.MustCompile(pattern)

	kva := []KeyValue{}
	err = filepath.Walk(pwd, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		//fmt.Printf("try to match %v for id %v\n", info.Name(), id)
		if re.MatchString(info.Name()) {
			//fmt.Printf("%v matched for id %v\n", info.Name(), id)
			ifile, err := os.Open(info.Name())
			if err != nil {
				return err
			}
			dec := json.NewDecoder(ifile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			ifile.Close()
		}
		return nil
	})

	if err != nil {
		log.Fatal("cannot read file", err)
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%v", id)
	ofile, err := os.CreateTemp(pwd, oname)
	if err != nil {
		log.Fatalf("cannot create temp file")
	}
	defer ofile.Close()

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
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
	err = os.Rename(ofile.Name(), oname)
	if err != nil {
		log.Fatalf("cannot rename file")
	}
}
