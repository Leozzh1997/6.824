package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Opcode   string
	Key      string
	Value    string
	ClientId int64
	ReqId    int
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	killCh  chan bool
	dead    int32 // set by Kill()
	db      map[string]string
	opCh    map[int]chan Op
	reqMap  map[int64]int

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, ok := kv.rf.GetState()
	if !ok {
		reply.Msg = ErrWrongLeader
		return
	}
	op := Op{
		Opcode:   "Get",
		Key:      args.Key,
		ReqId:    args.ReqId,
		ClientId: args.ClientId,
	}
	index, term, _ := kv.rf.Start(op)
	ch := kv.getChan(index)
	select {
	case <-ch:
		nowTerm, _ := kv.rf.GetState()
		if nowTerm != term {
			reply.Msg = ErrWrongLeader
		} else {
			reply.Msg = OK
			kv.mu.Lock()
			reply.Value = kv.db[args.Key]
			kv.mu.Unlock()
		}
	case <-time.After(time.Duration(300) * time.Millisecond):
		reply.Msg = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Opcode:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ReqId:    args.ReqId,
		ClientId: args.ClientId,
	}
	_, ok := kv.rf.GetState()
	if !ok {
		reply.Msg = ErrWrongLeader
		return
	}
	index, term, _ := kv.rf.Start(op)
	DPrintf("find leader? %v index:%d ,%v", kv.me, index, op)
	ch := kv.getChan(index)
	select {
	case <-ch:
		Newterm, _ := kv.rf.GetState()
		if Newterm != term {
			reply.Msg = ErrWrongLeader
		} else {
			reply.Msg = OK
		}
	case <-time.After(time.Duration(300) * time.Millisecond):
		reply.Msg = ErrWrongLeader
	}
}

func (kv *KVServer) getChan(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.opCh[index]
	if !ok {
		kv.opCh[index] = make(chan Op)
	}
	DPrintf("chan is exist? %v", ok)
	return kv.opCh[index]
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	kv.killCh <- true
	return z == 1
}

func (kv *KVServer) ticker() {
	for {
		select {
		case msg := <-kv.applyCh:
			//DPrintf("msg info %v", msg)
			if msg.CommandValid {
				kv.mu.Lock()
				op := msg.Command.(Op)
				preReqId, ok := kv.reqMap[op.ClientId]
				if !ok || preReqId < op.ReqId {
					kv.reqMap[op.ClientId] = op.ReqId
					switch op.Opcode {
					case "Put":
						kv.db[op.Key] = op.Value
					case "Append":
						kv.db[op.Key] += op.Value
					}
				}
				kv.mu.Unlock()
				if term, isLeader := kv.rf.GetState(); isLeader && term == msg.CommandTerm {
					DPrintf("op success leader server %d index %d %v", kv.me, msg.CommandIndex, op)
					ch := kv.getChan(msg.CommandIndex)
					select {
					case ch <- op:
					default:
					}
					DPrintf("send ok")
				}
			}
		case <-kv.killCh:
			return
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.db = make(map[string]string)
	kv.reqMap = make(map[int64]int)
	kv.opCh = make(map[int]chan Op)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.killCh = make(chan bool)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ticker()
	// You may need initialization code here.

	return kv
}
