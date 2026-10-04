package kvsrv

import (
	"log"
	"sync"

	kvrpc "6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersion struct {
	Value   string
	Version kvrpc.Tversion
}

type KVServer struct {
	mu   sync.Mutex
	maps map[string]ValueVersion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{mu: sync.Mutex{}, maps: make(map[string]ValueVersion)}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *kvrpc.GetArgs, reply *kvrpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.maps[args.Key]; ok {
		reply.Value = v.Value
		reply.Err = kvrpc.OK
		reply.Version = v.Version
		return
	}
	reply.Err = kvrpc.ErrNoKey
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *kvrpc.PutArgs, reply *kvrpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.maps[args.Key]; ok {
		if v.Version == args.Version {
			v.Value = args.Value
			v.Version++
			kv.maps[args.Key] = v
			reply.Err = kvrpc.OK
		} else {
			reply.Err = kvrpc.ErrVersion
		}
	} else if args.Version == 0 {
		m := ValueVersion{Value: args.Value, Version: args.Version + 1}
		kv.maps[args.Key] = m
		reply.Err = kvrpc.OK
	} else {
		reply.Err = kvrpc.ErrNoKey
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
