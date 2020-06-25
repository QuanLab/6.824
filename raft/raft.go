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

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuanLab/6.824/labrpc"
)

// import "bytes"
// import "labgob"

func getRandom(min int64, max int64) int64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Int63n(max-min+1) + min
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	Follower           int = 0
	Candidate          int = 1
	Leader             int = 2
	HEART_BEAT_TIMEOUT     = 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	state     int
	appendCh  chan bool
	timer     *time.Timer
	timeout time.Duration
	voteCh    chan bool
	voteCount int

	//Persistent state
	currentTerm int
	votedFor    int
	log         []string

	//Volatile state
	commitIndex int
	lastApplied int //initialized to 0, increases monotonically

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader = false
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		// if higher term found, switch to follower
		if args.Term > rf.currentTerm {
			rf.state = Follower
			rf.votedFor = -1
			rf.currentTerm = args.Term
		}

		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.state = Follower
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
		}
	}
	reply.Term = rf.currentTerm
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	Entries      []string
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("leader[raft%v][term:%v] beat term:%v [raft%v][%v]\n", args.LeaderId, args.Term, rf.currentTerm, rf.me, rf.state)
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		reply.Success = true
		reply.Term = args.Term
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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

	// Your code here (2B).
	term, isLeader = rf.GetState()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("create raft%v...", me)
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCh = make(chan bool)
	rf.appendCh = make(chan bool)

	electionTimeout := HEART_BEAT_TIMEOUT*3 + rand.Intn(HEART_BEAT_TIMEOUT)
	rf.timeout = time.Duration(electionTimeout) * time.Millisecond
	DPrintf("raft%v's election timeout is:%v\n", rf.me, rf.timeout)
	rf.timer = time.NewTimer(rf.timeout)
	go func() {
		for {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			electionTimeout := HEART_BEAT_TIMEOUT*3 + rand.Intn(HEART_BEAT_TIMEOUT)
			rf.timeout = time.Duration(electionTimeout) * time.Millisecond

			switch state {
				case Follower:
					select {
						case <-rf.appendCh:
							rf.timer.Reset(rf.timeout)
						case <-rf.voteCh:
							rf.timer.Reset(rf.timeout)
						case <-rf.timer.C:
							rf.mu.Lock()
							rf.switchStateTo(Candidate)
							rf.mu.Unlock()
							rf.startElection()
					}

				case Candidate:
					select {
						case <-rf.appendCh:
							rf.timer.Reset(rf.timeout)
							rf.mu.Lock()
							rf.switchStateTo(Follower)
							rf.mu.Unlock()
						case <-rf.timer.C:
							rf.startElection()
						default:
							rf.mu.Lock()
							if rf.voteCount > len(rf.peers)/2 {
								rf.switchStateTo(Leader)
							}
							rf.mu.Unlock()
					}
				case Leader:
					rf.heartBeats()
			}
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}


func (rf *Raft) startElection() {
    rf.mu.Lock()
    // DPrintf("raft%v is starting election\n", rf.me)
    rf.currentTerm += 1
    rf.votedFor = rf.me
    rf.timer.Reset(rf.timeout)
    rf.voteCount = 1
    rf.mu.Unlock()
    for peer, _ := range rf.peers {
        if peer != rf.me {
            go func(peer int) {
                rf.mu.Lock()
                args := RequestVoteArgs{}
                args.Term = rf.currentTerm
                args.CandidateId = rf.me
                // DPrintf("raft%v[%v] is sending RequestVote RPC to raft%v\n", rf.me, rf.state, peer)
                rf.mu.Unlock()
                reply := RequestVoteReply{}
                if rf.sendRequestVote(peer, &args, &reply) {
                    rf.mu.Lock()
                    if reply.VoteGranted {
                        rf.voteCount += 1
                    } else if reply.Term > rf.currentTerm {
                        //If RPC request or response contains term T > currentTerm:set currentTerm = T, convert to follower (§5.1)
                        rf.currentTerm = reply.Term
                        rf.switchStateTo(Follower)
                    }
                    rf.mu.Unlock()
                } else {
                    // DPrintf("raft%v[%v] vote:raft%v no reply, currentTerm:%v\n", rf.me, rf.state, peer, rf.currentTerm)
                }
            }(peer)
        }
    }
}

func (rf *Raft) switchStateTo(state int) {
	if rf.state == state {
		return
	}
	if state == Follower {
		rf.state = Follower
		DPrintf("raft%v become follower in term:%v\n", rf.me, rf.currentTerm)
		rf.votedFor = -1
	}
	if state == Candidate {
		DPrintf("raft%v become candidate in term:%v\n", rf.me, rf.currentTerm)
		rf.state = Candidate
	}
	if state == Leader {
		rf.state = Leader
		DPrintf("raft%v become leader in term:%v\n", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) heartBeats() {
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				args := AppendEntriesArgs{}
				rf.mu.Lock()
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(peer, &args, &reply) {
					//If RPC request or response contains term T > currentTerm:set currentTerm = T, convert to follower (§5.1)
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.switchStateTo(Follower)
					}
					rf.mu.Unlock()
				}
			}(peer)
		}
	}
	time.Sleep(HEART_BEAT_TIMEOUT * time.Millisecond)
}
