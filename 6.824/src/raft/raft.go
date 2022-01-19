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
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"fmt"
	"log"
	"math/rand"
	"time"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type Role int

const (
	FOLLOWER  Role = 0
	CANDIDATE Role = 1
	LEADER    Role = 2
)

type RoleChangedInfo struct {
	role     Role
	term     int
	votedFor int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

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
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role              Role
	term              int
	log               []LogEntry
	commitIndex       int
	nextIndex         []int
	votedFor          int
	tmpEntries        []LogEntry
	tmpPrevLogIndex   int
	receivers         []int
	roleChanged       chan RoleChangedInfo
	applyCh           chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	term := rf.term
	role := rf.role
	rf.mu.Unlock()
	rf.Log("raft:%d, term:%d, role:%d, votedFor:%d", rf.me, term, role, rf.votedFor)
	return term, role == LEADER
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Entries      []LogEntry
	PrevLogIndex int
	PrevLogTerm  int
	LeaderId     int
	LeaderCommit int
	Term         int
}

type AppendEntriesReply struct {
	Success bool
	Term    int
}

type CommitArgs struct {

}

type CommitReply struct {

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.term
	rf.mu.Unlock()
	if args.Term <= reply.Term ||
		(len(rf.log) > 0 && (args.LastLogTerm < rf.log[len(rf.log) - 1].Term ||
								  (args.LastLogTerm == rf.log[len(rf.log) - 1].Term && args.LastLogIndex <  len(rf.log) - 1))) {
		reply.VoteGranted = false
		rf.Log("vote not granted")
		return
	}

	rf.roleChanged <- RoleChangedInfo{
		role:     FOLLOWER,
		term:     args.Term,
		votedFor: args.CandidateId,
	}
	reply.VoteGranted = true
	rf.Log("vote granted")
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Log("here entries: %v", rf.log)
	reply.Term = rf.term
	if args.Term < rf.term {
		rf.Log("rf %d term %d reject appending from rf %d term %d because the term is stale. Entries in args: %v",
		        rf.me, rf.term, args.LeaderId, args.Term, args.Entries)
		reply.Success = false
		return
	}

	rf.roleChanged <- RoleChangedInfo{
		role: FOLLOWER,
		term: args.Term,
	}
	rf.Log("args entries: %v", args.Entries)
	if len(args.Entries) == 0 {
		reply.Success = true
		return
	}

	if len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		return
	}

	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.log) - 1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	rf.tmpPrevLogIndex = args.PrevLogIndex
	rf.tmpEntries = args.Entries
	reply.Success = true
}

func (rf *Raft) CommitEntries(args *CommitArgs, reply *CommitReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) > rf.tmpPrevLogIndex+1 {
		rf.log = rf.log[:rf.tmpPrevLogIndex+1]
	}

	rf.log = append(rf.log, rf.tmpEntries...)
	rf.commitIndex += len(rf.tmpEntries)
	for i, entry := range rf.tmpEntries {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rf.tmpPrevLogIndex + i + 1,
		}
	}
	return
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendCommit(server int, args *CommitArgs, reply *CommitReply) bool {
	ok := rf.peers[server].Call("Raft.CommitEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		isLeader = false
		return index, term, isLeader
	}
	// TODO
	newEntry := LogEntry{
		Command: command,
		Term:    rf.term,
	}

	rf.Log("Number of peers: %v\n", len(rf.peers))
	rf.Log("Leader's log entries %v\n", rf.log)
	progress := 1
	for serverInd, _ := range rf.peers {
		if serverInd == rf.me {
			continue
		}

		ok := false
		rf.Log("rf.nextIndex : %v", rf.nextIndex)
		nInd := rf.nextIndex[serverInd]
		// if nInd == -1, that iteration must succeed
		for {
			if nInd == -1 {
				break
			}
			var prevLogIndex int
			var prevLogTerm int
			prevLogIndex = nInd - 1
			if prevLogIndex < 0 {
				prevLogTerm--
			} else {
				prevLogTerm = rf.log[prevLogIndex].Term
			}
			rf.Log("nInd : %d, len : %d", nInd, len(rf.log))
			rf.Log("log entries: %v\n, from nInd %v", rf.log, rf.log[nInd:])

			// 1st phase
			args := AppendEntriesArgs{
				Entries:      append(rf.log[nInd:], newEntry),
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
				LeaderId:     rf.me,
				Term:         rf.term,
			}

			rf.Log("args:%v", args)
			reply := AppendEntriesReply{}
			ok = rf.sendAppendEntries(serverInd, &args, &reply)
			if ok {
				progress++
				rf.Log("1st phase succeed:%v", args)
				rf.receivers = append(rf.receivers, serverInd)
				break
			} else {
				nInd--
			}
		}
	}

	if progress <= len(rf.peers) / 2 {
		index = len(rf.log)
		term = rf.term
		isLeader = true
		goto END
	}

	// 2nd phrase
	for _, serverInd := range rf.receivers {
		ok := rf.sendCommit(serverInd, &CommitArgs{}, &CommitReply{})
		if ok {
			rf.Log("2nd phase succeed")
			rf.nextIndex[serverInd] = len(rf.log)
		}
	}

	rf.log = append(rf.log, newEntry)
	rf.commitIndex += 1

	index = len(rf.log) - 1
	term = rf.term
	rf.applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: index,
	}

END:
	rf.receivers = []int{}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var hbRecvTimeout, hbSendTimeout time.Duration
	rand.Seed(time.Now().UnixNano())
	hbRecvTimeout = time.Duration(300+rand.Intn(200)) * time.Millisecond
	toChan := time.After(hbRecvTimeout)
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-toChan:
			switch rf.role {
			case LEADER:
				rf.Log("Send out hbs.")
				for serverInd, _ := range rf.peers {
					if serverInd == rf.me {
						continue
					}
					go func(rf *Raft, term int, leaderInd int, serverInd int) {
						args := AppendEntriesArgs{
							LeaderCommit: leaderInd,
							LeaderId:     rf.me,
							Term:         term,
						}
						reply := AppendEntriesReply{}
						rf.sendAppendEntries(serverInd, &args, &reply)
						if reply.Term > term {
							rf.roleChanged <- RoleChangedInfo{
								role: FOLLOWER,
								term: reply.Term,
							}
						}
					}(rf, rf.term, rf.me, serverInd)
				}
				toChan = time.After(hbSendTimeout)
			case FOLLOWER:
				rf.roleChanged <- RoleChangedInfo{
					role:     CANDIDATE,
					term:     rf.term + 1,
					votedFor: rf.me,
				}
			case CANDIDATE:
				if rf.election() {
					rf.Log("Election success")
					rf.roleChanged <- RoleChangedInfo{
						role: LEADER,
						term: rf.term,
					}
				} else {
					rf.Log("Election failure")
					rf.roleChanged <- RoleChangedInfo{
						role: FOLLOWER,
					}
				}
			}
		case rcInfo := <-rf.roleChanged:
			rf.Log("rc : %v", rcInfo)
			rf.mu.Lock()
			rf.term = rcInfo.term
			if rf.role != rcInfo.role {
				rf.role = rcInfo.role
				if rf.role == LEADER {
					rf.Log("I am a leader.")
					rf.log = append(rf.log, LogEntry{
						Command: "no-op",
						Term:    rf.term,
					})
				}
			}
			if rcInfo.votedFor != -1 {
				rf.votedFor = rcInfo.votedFor
			}
			rf.mu.Unlock()

			switch rf.role {
			case FOLLOWER:
				hbRecvTimeout = time.Duration(400+rand.Intn(200)) * time.Millisecond
				rf.Log("Use random hb recieve time, time: %d ms", hbRecvTimeout)
				toChan = time.After(hbRecvTimeout)
			case LEADER:
				hbSendTimeout = time.Duration(100+rand.Intn(100)) * time.Millisecond
				rf.Log("Use random hb send out time, time: %d ms", hbSendTimeout)
				toChan = time.After(hbSendTimeout)
			case CANDIDATE:
				rf.Log("Use candidate timeout.")
				toChan = time.After(10 * time.Millisecond)
			}
		}
	}
}

func (rf *Raft) election() bool {
	replyBox := make(chan RequestVoteReply, len(rf.peers)-1)
	for serverInd, _ := range rf.peers {
		if serverInd == rf.me {
			continue
		}
		go func(rf *Raft, term int, candidateId int, server int) {
			lastLogIndex := len(rf.log) - 1
			var lastLogTerm int
			if lastLogIndex >= 0 {
				lastLogTerm = rf.log[lastLogIndex].Term
			} else {
				lastLogTerm = -1
			}
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			rf.sendRequestVote(server, &args, &reply)
			replyBox <- reply
		}(rf, rf.term, rf.me, serverInd)
	}

	electTimeOut := time.After(300 * time.Millisecond)
	reps, votes := 0, 1
	for reps < len(rf.peers)-1 {
		select {
		case reply := <-replyBox:
			reps += 1
			if reply.VoteGranted {
				votes += 1
				break
			}
			if reply.Term >= rf.term {
				rf.roleChanged <- RoleChangedInfo{
					role: FOLLOWER,
					term: reply.Term,
				}
				return false
			}
		case <-electTimeOut:
			goto FINISH
		}
	}

FINISH:
	if votes > len(rf.peers)/2 {
		return true
	}
	return false
}

// Add Log for debugable log
func (rf *Raft) Log(format string, args ...interface{}) {
	raftFmt := fmt.Sprintf("[rfId=%d role=%s] %s", rf.me, RaftRoleToString(rf.role), format)
	if len(args) > 0 {
		log.Printf(raftFmt, args...)
	} else {
		log.Println(raftFmt)
	}
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
	rf := &Raft{
		role:        FOLLOWER,
		term:        0,
		votedFor:    -1,
		roleChanged: make(chan RoleChangedInfo, 10),
		nextIndex:   make([]int, len(peers)),
		applyCh:     applyCh,
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.receivers = []int{}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
