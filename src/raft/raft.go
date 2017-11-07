package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "fmt"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// Msg to append caching log
// also signal a heartbeat
//
type AppendEntriesArg struct {
	Index int
	Term  int
}

type AppendEntriesReply struct {
}

//
// Status of current Raft
//
type Status int

const (
	Leader    Status = iota
	Follower         = iota
	Candidate        = iota
	Undefined        = iota
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term               int
	leader             int
	applyindex         int
	timeoutMilliSecond time.Duration
	applyMsgChan       chan AppendEntriesArg
	quitChan           chan int
	status             Status
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here.
	var isLeader = false
	if rf.status == Leader {
		isLeader = true
	}
	return rf.term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	RequesterId int
	Term        int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	VoteMe bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.term < args.Term {
		fmt.Printf("peer %d release leadership to %d in term %d\n", rf.me, args.RequesterId, args.Term)
		rf.term = args.Term
		rf.leader = args.RequesterId
		reply.VoteMe = true
		if rf.me != rf.leader {
			rf.status = Follower
			rf.applyMsgChan <- AppendEntriesArg{}
		}
	} else {
		reply.VoteMe = false
	}
	return
}

//
// AppendEntries RPC handler
// 1. append caching log
// 2. reset timeout
func (rf *Raft) AppendEntries(args AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term == rf.term {
		rf.applyMsgChan <- args
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArg, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.quitChan <- 1
	rf.status = Undefined
}

func (rf *Raft) SendHeartBeat() {
	rf.mu.Lock()
	fmt.Printf("%d peer got mastership for term %d\n", rf.me, rf.term)
	rf.status = Leader
	rf.leader = rf.me
	rf.mu.Unlock()
	go func(rf *Raft) {
		for {
			if rf.status == Undefined {
				break
			}
			// send heartbeat
			for ii, _ := range rf.peers {
				appendEntriesReply := AppendEntriesReply{}
				rf.sendAppendEntries(ii, AppendEntriesArg{0, rf.term}, &appendEntriesReply)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}(rf)
	fmt.Printf("%d peer request mastership for term %d end\n", rf.me, rf.term)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.

	// setup election timeout in [300, 500]
	rf.applyMsgChan = make(chan AppendEntriesArg)
	rf.quitChan = make(chan int)
	rf.status = Undefined
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.timeoutMilliSecond = time.Duration(150 + r.Intn(150))
	fmt.Printf("%d peer select timeout %d ms\n", rf.me, rf.timeoutMilliSecond)

	go func(rf *Raft) {
		for {
			select {
			case <-rf.quitChan:
				rf.status = Undefined
				break
			case args := <-rf.applyMsgChan:
				rf.mu.Lock()
				fmt.Printf(time.Now().Format("15:04:05.999999 "))
				fmt.Printf("peer %d received appendEntries for term%d\n", rf.me, args.Term)
				rf.mu.Unlock()
				// receive heartbeat
			case <-time.After(rf.timeoutMilliSecond * time.Millisecond):
				fmt.Printf(time.Now().Format("15:04:05.999999 "))
				rf.mu.Lock()
				rf.leader = rf.me
				rf.status = Candidate
				rf.term++
				// request mastership
				requestArg := RequestVoteArgs{rf.me, rf.term}
				replyArg := RequestVoteReply{false}
				votesSum := 0
				fmt.Printf("%d peer request mastership for term %d\n", rf.me, rf.term)
				rf.mu.Unlock()

				var mm sync.Mutex
				for ii, _ := range peers {
					if ii != rf.me {
						go func(ii int) {
							ok := rf.sendRequestVote(ii, requestArg, &replyArg)
							fmt.Printf("%d -> %d reply mastership for term %d %d\n", rf.me, ii, rf.term, replyArg.VoteMe)
							if ok && replyArg.VoteMe {
								mm.Lock()
								votesSum++
								if votesSum == len(rf.peers)/2 {
									rf.SendHeartBeat()
								}
								mm.Unlock()
							}
						}(ii)
					}
				}
			}
		}
	}(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
