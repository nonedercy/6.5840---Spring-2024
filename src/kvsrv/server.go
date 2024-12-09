package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	m       map[string]string
	taskIds map[int64]int
	save    map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.m[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.TaskId > kv.taskIds[args.Id] {
		kv.m[args.Key] = args.Value
		kv.taskIds[args.Id] = args.TaskId
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.TaskId > kv.taskIds[args.Id] {
		kv.save[args.Id] = kv.m[args.Key]
		reply.Value = kv.m[args.Key]
		kv.m[args.Key] += args.Value
		kv.taskIds[args.Id] = args.TaskId
	} else {
		reply.Value = kv.save[args.Id]
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.m = map[string]string{}
	kv.taskIds = map[int64]int{}
	kv.save = map[int64]string{}
	// You may need initialization code here.

	return kv
}
