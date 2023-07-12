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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
  //"log"
  //"sort"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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
  Term     int
  ClientId int64
  Seq      int64
}

type NodeState string

const (
  Candidate NodeState = "candidate"
  Follower            = "follower"
  Leader              = "leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

  state       NodeState
  voteCount   int
  applyCh     chan ApplyMsg
  wonElectCh  chan bool
  stepDownCh  chan bool
  grantVoteCh chan bool
  heartbeatCh chan bool
  snapshot    []byte

  //Persistent state on all servers
  currentTerm      int        // last term server has been (initialized to 0 on first boot, increases monotonically)
  votedFor         int        // condidateId that received vote in current term (or null if none)
  logs             []LogEntry // each entry contains command for state machine, and term when entry was received by leader (first index is 1)
  lastIncludeIndex int
  lastIncludeTerm  int

  // Volatile states on all servers
  commitIndex int // index of highest log entry known to be commited (initialized to 0, increases monotonically)
  lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

  // Volatile states on leaders
  nextIndex   []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
  matchIndex  []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).a
  rf.mu.Lock()
  term = rf.currentTerm
  isleader = rf.state == Leader
  rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
  w := new(bytes.Buffer)
  e := labgob.NewEncoder(w)
  e.Encode(rf.currentTerm) 
  e.Encode(rf.votedFor)
  e.Encode(rf.logs)
  e.Encode(rf.lastIncludeIndex)
  e.Encode(rf.lastIncludeTerm)
  
  rf.persister.Save(w.Bytes(), rf.snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
  d.Decode(&rf.votedFor)
  d.Decode(&rf.logs)
  d.Decode(&rf.lastIncludeIndex)
  d.Decode(&rf.lastIncludeTerm)

  rf.lastApplied, rf.commitIndex = rf.lastIncludeIndex, rf.lastIncludeIndex
}

type InstallSnapshotArgs struct {
  Term             int
  LeaderId         int
  LastIncludeIndex int
  LastIncludeTerm  int
  Data             []byte
}

type InstallSnapshotReply struct {
  Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
  rf.mu.Lock()
  defer rf.mu.Unlock()
  defer rf.persist()

  
  // 1. Reply immediately if term < currentTerm
  if args.Term < rf.currentTerm {
    reply.Term = rf.currentTerm
    return
  }

  reply.Term = args.Term

  if args.Term > rf.currentTerm {
    rf.stepDownToFollower(args.Term)
  }
  
  rf.sendToChan(rf.heartbeatCh, true)
  
  //log.Printf("[%d] received a snapshot from %d with lastIncludeIndex at %d, local: %d", rf.me, args.LeaderId, args.LastIncludeIndex, rf.lastIncludeIndex)

  if args.LastIncludeIndex <= rf.lastIncludeIndex {
    return
  }
   
  // 6. If existing log entry has same index and term as snapshot's last included entry, retain log entries following it and reply
  if args.LastIncludeIndex < rf.getLastIndex() {
    rf.logs = append(make([]LogEntry, 0), rf.logs[args.LastIncludeIndex-rf.lastIncludeIndex:]...)
    //return
  } else {
    // 7. Discard the entire log
    rf.logs = []LogEntry{{Term: args.LastIncludeTerm}}
  }
  
  rf.lastIncludeIndex, rf.lastIncludeTerm = args.LastIncludeIndex, args.LastIncludeTerm
  
  //if rf.lastApplied >= rf.lastIncludeIndex {
    //return
  //}
  
  rf.commitIndex = max(rf.commitIndex, args.LastIncludeIndex)
  rf.lastApplied = max(rf.lastApplied, args.LastIncludeIndex)
  
  //log.Printf("[%d] applying snapshot from [%d] with index %d", rf.me, args.LeaderId, rf.lastIncludeTerm)

  // 8. Reset state machine using snapshot contents (and load snapshot's cluster configuration)
  rf.snapshot = args.Data

  rf.applyCh <- ApplyMsg {
    SnapshotValid: true,
    Snapshot: args.Data,
    SnapshotTerm: rf.lastIncludeTerm,
    SnapshotIndex: rf.lastIncludeIndex,
  }
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) { 
  ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
  
  rf.mu.Lock()
  defer rf.mu.Unlock()


  if !ok || rf.state != Leader || rf.currentTerm != args.Term {
    return
  }
  
  if reply.Term > rf.currentTerm {
    rf.stepDownToFollower(args.Term)
    rf.persist()
    return
  }
  
  rf.nextIndex[server] = min(rf.lastIncludeIndex + 1, rf.getLastIndex() + 1)
  rf.matchIndex[server] = rf.nextIndex[server] - 1
  //log.Printf("[%d] sent a snapshot to %d with lastIncludeIndex at %d, local: %d", rf.me, server, args.LastIncludeIndex, rf.lastIncludeIndex)
  rf.updateCommitIndex()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
  rf.mu.Lock()
  defer rf.mu.Unlock()

	// Your code here (2D).
  if index <= rf.lastIncludeIndex || index > rf.commitIndex {
    return
  }

  newLog := append(make([]LogEntry, 0), rf.logs[index-rf.lastIncludeIndex:]...)
  rf.lastIncludeIndex = index
  rf.lastIncludeTerm = rf.getLogAt(index).Term
  rf.logs = newLog
  rf.commitIndex = max(index, rf.commitIndex)
  rf.lastApplied = max(index, rf.lastApplied)
  rf.snapshot = snapshot
  rf.persist()
}

// send to un-buffered channel without blocking
func (rf *Raft) sendToChan(ch chan bool, value bool) {
  select {
  case ch <- value:
  default:
  }
}

func max(a, b int) int {
  if a > b {
    return a
  }
  return b
}

func min(a, b int) int {
  if a < b {
    return a
  }
  return b
}

func (rf *Raft) getLogAt(i int) LogEntry {
  return rf.logs[i - rf.lastIncludeIndex]
}

func (rf *Raft) logLen() int {
  return rf.lastIncludeIndex + len(rf.logs)
}

func (rf *Raft) getLastIndex() int {
  return rf.logLen() - 1
}

func (rf *Raft) getLastTerm() int{
  return rf.getLogAt(rf.getLastIndex()).Term
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
  Term         int  // candiate's term
  CandidateId  int  // candidate requesting vote
  LastLogIndex int  // index of candidate's last log entry
  LastLogTerm  int  // term of candidate's last log entry
}

type RequestVoteReply struct {
	// Your data here (2A).
  Term        int  // currentTerm, for candidate to update itself
  VoteGranted bool // true means candiate received vote
}

// Invoked by candidates to gather votes
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
  rf.mu.Lock()
  defer rf.mu.Unlock()
  defer rf.persist()

  // 1. Reply false if term < currentTerm
  if args.Term < rf.currentTerm {
    reply.Term = rf.currentTerm
    reply.VoteGranted = false
    return
  }
  
  if args.Term > rf.currentTerm {
    rf.stepDownToFollower(args.Term)
  }

  reply.Term = rf.currentTerm
  reply.VoteGranted = false
  // 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
  if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
    reply.VoteGranted = true
    rf.votedFor = args.CandidateId
    rf.sendToChan(rf.grantVoteCh, true)
  }
}

func (rf *Raft) isLogUpToDate(argLastIndex int, argLastTerm int) bool {
  if argLastTerm == rf.getLastTerm() {
    return argLastIndex >= rf.getLastIndex()
  }

  return argLastTerm > rf.getLastTerm()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  
  if ok == false {
    return ok
  }

  rf.mu.Lock()
  defer rf.mu.Unlock()
  
  if rf.state != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
    return ok
  }

  if reply.Term > rf.currentTerm {
    rf.stepDownToFollower(args.Term)
    rf.persist()
    return ok
  } 
  
  if reply.VoteGranted {
    rf.voteCount++
    if rf.voteCount == len(rf.peers) / 2 + 1 {
      rf.sendToChan(rf.wonElectCh, true)
    } 
  }
  
	return ok
}

// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
func (rf *Raft) applyEntries() {
  rf.mu.Lock()
  rf.lastApplied = max(rf.lastApplied, rf.lastIncludeIndex)
  rf.commitIndex = min(max(rf.commitIndex, rf.lastIncludeIndex), rf.getLastIndex())
  for rf.lastApplied < rf.commitIndex && rf.commitIndex <= rf.getLastIndex() {
    rf.lastApplied++
    //log.Printf("[%d] applying entry %d with lastIncludeIndex at %d and commitIndex at %d", rf.me, rf.lastApplied, rf.lastIncludeIndex, rf.commitIndex)
    msg := ApplyMsg{
      CommandValid: true,
      Command: rf.getLogAt(rf.lastApplied).Command,
      CommandIndex: rf.lastApplied,
    }
    rf.applyCh <- msg
  }
  rf.mu.Unlock()
}

type AppendEntriesArgs struct {
  Term         int  // leader's term
  LeaderId     int  // so followers can redirect clients
  PrevLogIndex int  // term of prevLogIndex entry
  PrevLogTerm  int  // term of prevLogIndexEntry 
  Entries      []LogEntry  // log entries to store (empty for heartbeat; may send more than one for efficiency)
  LeaderCommit int  // leader's commitIndex
}

type AppendEntriesReply struct {
  Term    int  // currentTerm, for leader to update itself
  Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
  XTerm   int
  XIndex  int
  XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
  rf.mu.Lock()
  defer rf.mu.Unlock() 
  defer rf.persist()

  reply.Success = false
  // 1. Reply false if term < currentTerm
  if args.Term < rf.currentTerm {
    reply.Term = rf.currentTerm
    return
  }
   
  // If RPC requests or response contains term T > currentTerm, set currentTerm = T, convert to follower
  if args.Term > rf.currentTerm {
    rf.stepDownToFollower(args.Term)
  }
 
  rf.sendToChan(rf.heartbeatCh, true)

  reply.Term = rf.currentTerm

  // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
  if rf.logLen() <= args.PrevLogIndex {
    reply.XTerm = -1
    reply.XLen = args.PrevLogIndex - rf.logLen() + 1
    return
  }

  prevLogIndexTerm := rf.lastIncludeTerm
  if args.PrevLogIndex >= rf.lastIncludeIndex && args.PrevLogTerm < rf.logLen() {
    prevLogIndexTerm = rf.getLogAt(args.PrevLogIndex).Term 
  }
  if prevLogIndexTerm != args.PrevLogTerm {
    reply.XTerm = prevLogIndexTerm
    for i := args.PrevLogIndex; i >= rf.lastIncludeIndex && rf.getLogAt(i).Term == reply.XTerm; i-- {
      reply.XIndex = i
    }
    return
  } 
  
  reply.Success = true
  // 3. If an existing entry conflict with a new one (same index but different terms), delete the existing entry and all that folow it
  i, j := args.PrevLogIndex + 1, 0
  for ; i <= rf.getLastIndex() && j < len(args.Entries); i, j = i+1, j+1 {
    if rf.getLogAt(i).Term != args.Entries[j].Term {
      break
    }
  }
  rf.logs = rf.logs[:i-rf.lastIncludeIndex]
  args.Entries = args.Entries[j:]
  // 4. Append any new entries not already in the log
  rf.logs = append(rf.logs, args.Entries...)

  // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
  if args.LeaderCommit > rf.commitIndex {
    rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
    go rf.applyEntries()
  } 
}

func(rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
  ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
 
  if ok == false {
    return
  }

  rf.mu.Lock()
  defer rf.mu.Unlock()
  defer rf.persist()

  if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
    return
  }

  // If a leader is rejected not because of log inconsistency (this can only happen if our term has passed),
  // the leader should immediately step down and not update nextIndex
  if reply.Term > rf.currentTerm {
    rf.stepDownToFollower(args.Term)
    return
  } 

  if reply.Success == true {
    rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
  } else if reply.XTerm < 0 {
    rf.nextIndex[server] = args.PrevLogIndex + 1 - reply.XLen
  } else {
    hasReplyTerm := false
    lastIndex := rf.nextIndex[server] - 1 
    for i := lastIndex; i >= rf.lastIncludeIndex; i-- {
      if rf.getLogAt(i).Term == reply.Term {
        hasReplyTerm = true
        break
      }
    }
    if hasReplyTerm {
      rf.nextIndex[server] = reply.XIndex + 1
    } else{
      rf.nextIndex[server] = reply.XIndex
    }
  }
  rf.nextIndex[server] = min(rf.nextIndex[server], rf.getLastIndex() + 1)
  rf.matchIndex[server] = rf.nextIndex[server] - 1
  rf.updateCommitIndex()
}

func (rf *Raft) updateCommitIndex() {
  for n := rf.getLastIndex(); n > rf.commitIndex; n-- {
    count := 1
    // a leader is not allowed to update commitIndex to somewhere in a previous term (or, for that mater, a future term)
    // This is because Raft leaders cannot be sure an entry is actually commited (and will not ever be changed in the future) if it's not from their current term. 
    // This is illustrated by Figure 8 in the paper
    if rf.getLogAt(n).Term == rf.currentTerm {
      for i := 0; i < len(rf.peers); i++ {
        if i != rf.me && rf.matchIndex[i] >= n {
          count++
        }
      }
    }
    if count > len(rf.peers) / 2 {
      rf.commitIndex = n
      //log.Printf("[%d] changed commitIndex to %d", rf.me, n)
      go rf.applyEntries()
      break
    }
  }
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
  rf.mu.Lock()
  defer rf.mu.Unlock()

  if rf.state != Leader {
    return -1, rf.currentTerm, false
  }  
  
  term := rf.currentTerm
  logEntry := LogEntry{
    Command: command,
    Term:    term,
  }
  rf.logs = append(rf.logs, logEntry)
  rf.broadcastAppendEntries()
  rf.persist()
  
  return rf.getLastIndex(), term, true
}

// the tester do esn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
  atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
  return z == 1
}

func (rf *Raft) broadcastRequestVote() {  
  if rf.state != Candidate {
    return
  }

  args := RequestVoteArgs {
    Term:         rf.currentTerm,
    CandidateId:  rf.me,
    LastLogIndex: rf.getLastIndex(),
    LastLogTerm:  rf.getLastTerm(),
  }
  
  for server, _ := range rf.peers {
    if server == rf.me {
      continue
    }
    go rf.sendRequestVote(server, &args, &RequestVoteReply{})
  }
}

func (rf *Raft) broadcastAppendEntries() {
  if rf.state != Leader  {
    return
  }

  for server, _ := range rf.peers {
    if server == rf.me {
      continue
    }
    if rf.lastIncludeIndex > rf.nextIndex[server] - 1 {
      args := InstallSnapshotArgs{
        Term:             rf.currentTerm,
        LeaderId:         rf.me,
        LastIncludeIndex: rf.lastIncludeIndex,
        LastIncludeTerm:  rf.lastIncludeTerm,
        Data:             rf.snapshot,
      }
      go rf.sendSnapshot(server, &args, &InstallSnapshotReply{})
    } else {
      args := AppendEntriesArgs{
        Term:         rf.currentTerm,
        LeaderId:     rf.me,
        PrevLogIndex: rf.nextIndex[server] - 1, 
        PrevLogTerm:  rf.getLogAt(rf.nextIndex[server]-1).Term,
        LeaderCommit: rf.commitIndex,
      } 
      args.Entries = append(make([]LogEntry, 0), rf.logs[rf.nextIndex[server]-rf.lastIncludeIndex:]...)
      go rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
    }
  }
}

func (rf *Raft) stepDownToFollower(term int) {
  state := rf.state
  rf.state = Follower
  rf.currentTerm = term
  rf.votedFor = -1
  if state != Follower {
    rf.sendToChan(rf.stepDownCh, true)
  }
}

func (rf *Raft) attemptElection(fromState NodeState) {
  rf.mu.Lock()
  defer rf.mu.Unlock()

  if rf.state != fromState {
    return
  }
  
  rf.resetChans()
  rf.state = Candidate
  rf.currentTerm++
  rf.votedFor = rf.me
  rf.voteCount = 1
  rf.persist() 

  rf.broadcastRequestVote()
}

func (rf *Raft) convertToLeader() {
  rf.mu.Lock()
  defer rf.mu.Unlock()
  
  if rf.state != Candidate {
    return
  }

  rf.resetChans()
  rf.state = Leader
  rf.nextIndex = make([]int, len(rf.peers))
  rf.matchIndex = make([]int, len(rf.peers))
  nextIndex := rf.getLastIndex() + 1
  for i := range rf.peers {
    rf.nextIndex[i] = nextIndex
    rf.matchIndex[i] = -1
  }

  rf.broadcastAppendEntries()
}

func (rf *Raft) resetChans() {
  rf.wonElectCh = make(chan bool)
  rf.stepDownCh = make(chan bool)
  rf.grantVoteCh = make(chan bool)
  rf.heartbeatCh = make(chan bool)
}

func (rf *Raft) getElectionTimeout() time.Duration {
  return time.Duration(360 + rand.Intn(240))
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and DELAY
		// milliseconds.
    rf.mu.Lock()
    state := rf.state
    rf.mu.Unlock()
    switch state {
    case Leader:
      select {
      case <-rf.stepDownCh:
        //
      case <-time.After(120 * time.Millisecond):
        rf.mu.Lock()
        rf.broadcastAppendEntries()
        rf.mu.Unlock()
      }
    case Follower:
      select {
      case <-rf.grantVoteCh:
        //
      case <-rf.heartbeatCh:
      case <-time.After(rf.getElectionTimeout() * time.Millisecond):
        rf.attemptElection(Follower)
      }
    case Candidate:
      select {
      case <-rf.stepDownCh:
        //
      case <-rf.wonElectCh:
        rf.convertToLeader()
      case <-time.After(rf.getElectionTimeout() * time.Millisecond):
        rf.attemptElection(Candidate)
      }
    }
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
  rf.state = Candidate
  rf.currentTerm = 0
  rf.votedFor = -1
  rf.voteCount = 0
  rf.logs = append(rf.logs, LogEntry{Term: 0})
  rf.commitIndex = 0
  rf.lastApplied = 0
  rf.lastIncludeIndex = 0
  rf.applyCh = applyCh
  
  rf.wonElectCh = make(chan bool)
  rf.stepDownCh = make(chan bool)
  rf.grantVoteCh = make(chan bool)
  rf.heartbeatCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
  rf.snapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()
  
	return rf
}
