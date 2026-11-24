package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"log"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Log struct {
	Term int
	Cmd  interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state           State
	electionTimeout time.Duration
	lastElectedAt   time.Time
	currentTerm     int
	votedFor        int
	logs            []Log

	commitIndex int
	lastApplied int

	nextIndexs  []int
	matchIndexs []int

	applyChan chan raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	From        int
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[rf.getLastLogIndex()].Term
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.From = rf.me
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log.Printf("[%v] RequestVote Term %v Args %v", rf.me, rf.currentTerm, args)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	lli := rf.getLastLogIndex()
	llt := rf.getLastLogTerm()

	canVote := rf.votedFor == -1
	candidateVoted := rf.votedFor == args.CandidateId
	laterTerm := args.LastLogTerm > llt
	UTDLogs := args.LastLogTerm == llt && args.LastLogIndex >= lli

	if (canVote || candidateVoted) && (laterTerm || UTDLogs) {
		rf.votedFor = args.CandidateId
		rf.lastElectedAt = time.Now()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	reply.Term = args.Term

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, repl *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("[%v] AppendEntries Term %v Args %v", rf.me, rf.currentTerm, args)

	if args.Term < rf.currentTerm {
		repl.Term = rf.currentTerm
		repl.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	repl.Term = args.Term
	repl.Success = true

	rf.lastElectedAt = time.Now()

	if !hasLog(args.PrevLogIndex, rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		repl.Success = false
		return
	}

	for k, v := range args.Entries {
		idx := args.PrevLogIndex + 1 + k
		if hasLog(idx, rf.logs) {
			if rf.logs[idx].Term == v.Term {
				continue
			}

			rf.logs = rf.logs[:idx]
		}

		// logs are truncated, we break
		rf.logs = append(rf.logs, args.Entries[k:]...)
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		//  index of last new entry
		lri := args.PrevLogIndex + len(args.Entries)
		newIdx := min(args.LeaderCommit, lri)
		if newIdx > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, newIdx)
		}
	}

}

func hasLog(idx int, logs []Log) bool {
	return idx < len(logs)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", &args, &reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, false
	}

	rf.mu.Lock()
	l := Log{Term: term, Cmd: command}
	rf.logs = append(rf.logs, l)
	index = rf.getLastLogIndex()
	rf.mu.Unlock()

	// log.Printf("[%v] accepting command %v", rf.me, command)
	for k := range rf.peers {
		if k == rf.me {
			continue
		}

		go func(serverIdx int) {
			rf.mu.Lock()
			nextIndex := rf.nextIndexs[serverIdx]
			lli := rf.getLastLogIndex()
			rf.mu.Unlock()
			if lli >= nextIndex {
				rf.mu.Lock()

				entries := rf.logs[nextIndex:]
				pli := nextIndex - 1
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: pli,
					PrevLogTerm:  rf.logs[pli].Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				repl := AppendEntriesReply{}
				rf.mu.Unlock()
				_ = rf.sendAppendEntries(serverIdx, &args, &repl)
				// log.Printf("[%v] -> [%v] Start args %v repl %v", rf.me, serverIdx, args, repl)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if repl.Term > rf.currentTerm {
					rf.toFollower(repl.Term)
					// log.Printf("[%v] [Start] back to follower", rf.me)
					return
				}

				if repl.Term != rf.currentTerm || rf.state != Leader {
					return
				}

				if repl.Success {
					lri := args.PrevLogIndex + len(entries)
					if lri > rf.matchIndexs[serverIdx] {
						rf.matchIndexs[serverIdx] = lri
					}
					rf.nextIndexs[serverIdx] = rf.matchIndexs[serverIdx] + 1
					// log.Printf("[%v] -> [%v] mi %v ni %v", rf.me, serverIdx, rf.matchIndexs[serverIdx], rf.nextIndexs[serverIdx])
				}
				for i := rf.getLastLogIndex(); i > rf.commitIndex; i-- {
					if rf.logs[i].Term != rf.currentTerm {
						//never commits log entries from previous terms by counting replicas
						break
					}
					c := 1
					for k := range rf.peers {
						if k != rf.me && rf.matchIndexs[k] >= i {
							c++
						}
					}
					if c >= rf.getMajority() {
						rf.commitIndex = i
						// log.Printf("Found commitIndex %v", rf.commitIndex)
						break
					}
				}
			}

		}(k)

	}

	return index, term, isLeader
}

func (rf *Raft) getMajority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) ticker() {
	for true {
		rf.mu.Lock()
		timeout := time.Since(rf.lastElectedAt) >= rf.electionTimeout
		isLeader := rf.state == Leader
		rf.mu.Unlock()
		if !isLeader && timeout {
			rf.electLeader()
		}
		rf.mu.Lock()
		isLeader = rf.state == Leader
		rf.mu.Unlock()
		if isLeader {
			rf.sendHeartBeat()
		}

		time.Sleep(time.Duration(50+(rand.Int63()%300)) * time.Millisecond)
	}
}

func (rf *Raft) electLeader() {
	rf.mu.Lock()
	rf.currentTerm = rf.currentTerm + 1
	rf.state = Candidate
	rf.electionTimeout = getElectionTimeout()
	rf.lastElectedAt = time.Now()
	rf.votedFor = rf.me
	term := rf.currentTerm
	lli := rf.getLastLogIndex()
	llt := rf.getLastLogTerm()
	rf.mu.Unlock()

	replyCh := make(chan RequestVoteReply, len(rf.peers))
	for k := range rf.peers {
		if k == rf.me {
			continue
		}
		go func(serverIndex int) {
			args := RequestVoteArgs{Term: term, CandidateId: rf.me, LastLogIndex: lli, LastLogTerm: llt}
			repl := RequestVoteReply{}
			if ok := rf.sendRequestVote(serverIndex, &args, &repl); !ok {
				repl.From = serverIndex
			}
			replyCh <- repl

		}(k)

	}
	go func() {
		votes := 1
		for range len(rf.peers) - 1 {
			repl := <-replyCh
			if repl.Term < term {
				continue
			}

			rf.mu.Lock()

			if repl.Term > term {
				rf.toFollower(repl.Term)
				rf.mu.Unlock()
				break
			}

			if rf.state == Candidate {
				if repl.VoteGranted {
					votes++
				}

				if votes >= rf.getMajority() {
					rf.state = Leader
					log.Printf("Leader is %v", rf.me)
					rf.mu.Unlock()
					break
				}
			}

			rf.mu.Unlock()

		}
	}()
}

func (rf *Raft) sendHeartBeat() {
	for k := range rf.peers {
		if k == rf.me {
			continue
		}

		go func(serverIdx int) {
			rf.mu.Lock()
			ni := rf.nextIndexs[serverIdx]
			pli := ni - 1
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: pli,
				PrevLogTerm:  rf.logs[pli].Term,
				Entries:      nil,
				LeaderCommit: rf.commitIndex}
			repl := AppendEntriesReply{}
			rf.mu.Unlock()
			_ = rf.sendAppendEntries(serverIdx, &args, &repl)
			// log.Printf("[%v] -> [%v] sendHeartBeat args %v reply %v", rf.me, serverIdx, args, repl)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if repl.Term > rf.currentTerm {
				rf.toFollower(repl.Term)
				// log.Printf("[%v] back to follower", rf.me)
			}
		}(k)

	}
}

func (rf *Raft) toFollower(currentTerm int) {
	rf.state = Follower
	rf.currentTerm = currentTerm
	rf.votedFor = -1
}

// respond after entry applied to state machine
func (rf *Raft) applyCmd() {
	for true {
		rf.mu.Lock()
		var msgs []raftapi.ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied].Cmd,
				CommandIndex: rf.lastApplied,
			})
		}
		rf.mu.Unlock()

		for _, v := range msgs {
			rf.applyChan <- v
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.lastElectedAt = time.Now()
	rf.electionTimeout = getElectionTimeout()
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0

	dumpLog := Log{Term: 0, Cmd: "DummyCmd"}
	rf.logs = append(rf.logs, dumpLog)

	rf.lastApplied = 0
	rf.commitIndex = 0

	l := len(rf.peers)
	lli := rf.getLastLogIndex()
	rf.nextIndexs = make([]int, l)
	rf.matchIndexs = make([]int, l)
	rf.applyChan = applyCh

	for k := range rf.peers {
		rf.nextIndexs[k] = lli + 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCmd()

	return rf
}

func getElectionTimeout() time.Duration {
	t := 400 + rand.Int63()%400
	return time.Duration(t) * time.Millisecond
}
