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
	"cs350/labgob"
	"cs350/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	voteNil                                = -1
	electionTimeoutMin, electionTimeoutMax = 800, 1200 // to ensure on average 10 heartbeats is tried before new election
	HeartBeatInterval                      = 100 * time.Millisecond
	maxLagAmount                           = 20
)

type State string

const (
	Leader    State = "RaftLeader"
	Follower  State = "RaftFollower"
	Candidate State = "RaftCandidate"
)

const (
	LogInConsistency = "LogInConsistency"
)

func RandRange(from, to int) int {
	return rand.Intn(to-from) + from
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func RandomTimeOutDuration() time.Duration {
	return time.Duration(RandRange(electionTimeoutMin, electionTimeoutMax)) * time.Millisecond
}

func (rf *Raft) snoozeAlarm() {
	rf.electionAlarm = time.Now().Add(time.Duration(RandomTimeOutDuration()))
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // CurrentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

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

// Raft log entry
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // CurrentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // index of first entry with that ConflictTerm term (if any)
	ConflictTerm  int  // term in the conflicting entry (if any)
	FailReason    string
}

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
	// persistent state on all servers, updated on stable storage before responding to RPCs

	CurrentTerm       int        // latest term server has seen, increases monotonically
	VotedFor          int        // candidateId that received vote in current term (or null if none)
	Log               []LogEntry // log entries
	LastIncludedIndex int        // index of the log entry that before the start of log, increases monotonically
	LastIncludedTerm  int        // term of the log entry that before the start of log
	// volatile state on all servers
	commitIndex   int       // index of highest log entry known to be committed, increases monotonically
	lastApplied   int       // index of highest log entry applied to state machine, increases monotonically
	state         State     // the server state, follower, candidate, or leader.
	electionAlarm time.Time // the election Alarm. Rings if election
	// volatile state on leaders, reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server. If not leader should be nil.
	matchIndex []int // for each server, index of hightest log entry known to be replicated on server If not leader should be nil.

	// others
	applyCh chan<- ApplyMsg
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan<- ApplyMsg) *Raft {
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		VotedFor:          voteNil, // initially voted for nobody
		Log:               []LogEntry{},
		commitIndex:       0, // initialized to 0
		lastApplied:       0, // initialized to 0
		state:             Follower,
		nextIndex:         nil,
		matchIndex:        nil,
		electionAlarm:     time.Now(),
		CurrentTerm:       0, // initialized to 0 on first boot
		LastIncludedIndex: 0, // initialized to 0
		LastIncludedTerm:  0, // initialized to 0
		applyCh:           applyCh,
	}

	// My logger
	// file, err := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.SetOutput(file)
	rf.snoozeAlarm()
	rf.applyCh <- ApplyMsg{}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	data := rf.persister.ReadSnapshot()
	if data != nil && rf.LastIncludedIndex != 0 && rf.LastIncludedTerm != 0 {
		applyMsg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      data,
			SnapshotIndex: rf.LastIncludedIndex,
			SnapshotTerm:  rf.LastIncludedTerm,
		}
		applyCh <- applyMsg
	}

	// set commitIndex and lastApplied to snapshot's LastIncludedIndex
	rf.commitIndex = rf.LastIncludedIndex
	rf.lastApplied = rf.LastIncludedIndex

	// log.Printf("Server %v starts with term %v, lastIncludedIndex %v, lastIncludedTerm %v, \nlogs %v\n", me, rf.CurrentTerm, rf.LastIncludedIndex, rf.LastIncludedTerm, rf.Log)
	go rf.ticker()
	go rf.commitMonitor(applyCh)
	return rf
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
	// Your code here (2B).
	rf.mu.Lock()
	if rf.state != Leader || rf.killed() {
		rf.mu.Unlock()
		return 0, 0, false
	}
	index := rf.nextIndex[rf.me]
	term := rf.CurrentTerm
	// append entry to local log
	rf.Log = append(rf.Log, LogEntry{
		Index:   rf.nextIndex[rf.me],
		Term:    rf.CurrentTerm,
		Command: command,
	})
	// modified persistent storage, persist.
	rf.persist()
	rf.nextIndex[rf.me] += 1
	rf.matchIndex[rf.me] = index
	rf.mu.Unlock()
	// Send AppendEntries RPCs to all followers, ask for agreement
	// This is not necessary but it makes more sense to me
	// turns out to be an optimization because I don't have to rely on Heartbeats to send appendentries.
	// heartbeats can still append entries if a follower fails and rejoins
	// reduces time from 47s -> 34s
	for i := range rf.peers {
		if i != rf.me {
			go rf.appendEntries(i, term)
		}
	}
	return index, term, true
}

// return CurrentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	term := rf.CurrentTerm
	isLeader := rf.state == Leader
	rf.mu.Unlock()
	return term, isLeader
}

// with mutex
func (rf *Raft) getLastLogIndex() int {
	length := len(rf.Log)
	if length == 0 {
		return rf.LastIncludedIndex
	}
	return rf.Log[length-1].Index
}

// with mutex
func (rf *Raft) getLastLogTerm() int {
	length := len(rf.Log)
	if length == 0 {
		return rf.LastIncludedTerm
	}
	return rf.Log[length-1].Term
}

// the tester doesn't halt goroutines created by Raft after each test,
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

// with mutex
func (rf *Raft) goBackToFollower(term int) {
	rf.state = Follower
	rf.CurrentTerm = term
	rf.VotedFor = voteNil
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.persist()
}

// with mutex
func (rf *Raft) candidateMoreUpToDate(index int, term int) bool {
	myLastLogIndex := rf.getLastLogIndex()
	myLastLogTerm := rf.getLastLogTerm()
	if term > myLastLogTerm {
		return true
	} else if term == myLastLogTerm && index >= myLastLogIndex {
		return true
	} else {
		return false
	}
}

// with mutex
func (rf *Raft) becomeLeader() {

	// state to leader
	rf.state = Leader
	// initialize nextIndex[] to leader last log index + 1
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	//matchIndex (initialized to 0, increases monotonically)
	rf.matchIndex = make([]int, len(rf.peers))
	// initialize to 0
	for i := range rf.matchIndex {
		// the leader knows its last log index.
		if i == rf.me {
			rf.matchIndex[i] = rf.getLastLogIndex()
		} else {
			rf.matchIndex[i] = 0
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	reply.VoteGranted = false
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm

	// return false if term < current term
	if args.CandidateId == rf.me || args.Term < rf.CurrentTerm || rf.killed() {
		// if args.CandidateId == rf.me {
		// 	fmt.Println("Goodbye: candidate is myself")
		// } else if args.Term < rf.CurrentTerm {
		// 	fmt.Println("Goodbye: candidate outdated term")
		// } else if rf.killed() {
		// 	fmt.Println("Goodbye: candidate killed")
		// }
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.CurrentTerm {
		// fmt.Printf("Correcting server term: %v -> %v\n", rf.CurrentTerm, args.Term)
		rf.goBackToFollower(args.Term)
		reply.Term = rf.CurrentTerm
	}

	// if never voted, or already voted for that candidate, then we vote for that candidate

	if rf.VotedFor == voteNil || rf.VotedFor == args.CandidateId {
		if rf.candidateMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			// fmt.Printf("Server %v voted for candidate %v\n", rf.me, rf.VotedFor)
			// reset the election alarm
			rf.snoozeAlarm()
			rf.persist()
		}
	}
	rf.mu.Unlock()
}

// make RequestVote RPC args, with mutex held
func (rf *Raft) makeRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
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
	return ok
}

func (rf *Raft) requestVote(server int, term int, votes chan bool, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)
	if !ok {
		// fmt.Println("RequestVote failed")
		return
	}
	rf.mu.Lock()

	// check assumptions after lock
	if rf.state != Candidate || args.Term < rf.CurrentTerm || rf.killed() {
		rf.mu.Unlock()
		return
	}

	if reply.Term > rf.CurrentTerm {
		rf.goBackToFollower(reply.Term)
		rf.mu.Unlock()
		return
	}

	if rf.CurrentTerm != term {
		rf.mu.Unlock()
		return
	}

	// fmt.Printf("Grant Vote: %v\n", reply.VoteGranted)
	rf.mu.Unlock()
	granted := reply.VoteGranted
	votes <- granted
}

func (rf *Raft) countVotes(term int, votes <-chan bool) {
	// fmt.Println("counting votes...")
	count := 1 // because initially candidate vote for itself
	for range rf.peers {
		if <-votes {
			// fmt.Printf("%v votes so far\n", count)
			count += 1
		}
		// this block should only be triggered once
		if count > len(rf.peers)/2 {
			rf.mu.Lock()
			if rf.state != Candidate || rf.CurrentTerm != term || rf.killed() {
				rf.mu.Unlock()
				return
			}
			// fmt.Printf("☆  Server %v wins the election!\n", rf.me)
			rf.becomeLeader()
			rf.mu.Unlock()
			for member := range rf.peers {
				if member != rf.me {
					go rf.sendHeartbeat(member, term)
				}
			}
			break
		}
	}
}

func (rf *Raft) sendHeartbeat(server int, term int) {
	for !rf.killed() {
		rf.mu.Lock()
		// fmt.Printf("Server %v Sending heartbeat of term %v\n", rf.me, rf.CurrentTerm)
		if rf.state != Leader || rf.CurrentTerm != term || rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		go rf.appendEntries(server, term)
		time.Sleep(HeartBeatInterval)
	}
}

// The ticker go routine starts a new election if this peer hasn't received heartbeats recently.
func (rf *Raft) ticker() {
	var electionAlarm time.Time
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 1. If it is a leader then no need to start a new election
		// it resets its ticker
		rf.mu.Lock()
		electionAlarm = rf.electionAlarm
		if rf.state == Leader {
			// set the alarm time to the next time
			// by using current time + the random duration
			// acheived by snoozeAlarm()
			rf.snoozeAlarm()
			rf.mu.Unlock()
			time.Sleep(time.Until(electionAlarm))
			continue
		} else {
			// if the alarm is not triggered then sleep until the alarm
			// many people find it hard to understand the other way around
			if time.Now().Before(electionAlarm) {
				rf.mu.Unlock()
				time.Sleep(time.Until(electionAlarm))
				continue
			} else {
				// fmt.Printf("Server %v starts a new election for term %v.\n", rf.me, rf.CurrentTerm+1)
				// else start a new election. Process to be followed.
				// Increment current term

				//////////////////////////////////////////////////////////////
				// become candidate part
				rf.CurrentTerm += 1
				// Switch to candidate state
				rf.state = Candidate
				// Vote for itself
				rf.VotedFor = rf.me
				// reset the alarm
				rf.snoozeAlarm()

				rf.persist()
				// request and collect votes from other servers
				// fmt.Println("sending vote request")
				args := rf.makeRequestVoteArgs()
				term := rf.CurrentTerm
				rf.mu.Unlock()
				votes := make(chan bool, len(rf.peers)-1)
				for member := range rf.peers {
					if member != rf.me {
						go rf.requestVote(member, term, votes, args)
					}
				}
				go rf.countVotes(args.Term, votes)
				//////////////////////////////////////////////////////////////
				time.Sleep(time.Until(electionAlarm))
				continue
			}

		}
	}
}

// with mutex
func (rf *Raft) processEntries(args *AppendEntriesArgs) {
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// obtain the existing entries, align the index
	myEntriesSincePrevLogIndex := rf.Log[args.PrevLogIndex-rf.LastIncludedIndex:]
	var i int
	iterAmount := min(len(myEntriesSincePrevLogIndex), len(args.Entries))
	// compare one to one each entry in existing entries with args entries.
	for i = 0; i < iterAmount; i++ {
		// if we find a conflict
		// delete the existing entry and all that follow it
		if myEntriesSincePrevLogIndex[i].Term != args.Entries[i].Term {
			rf.Log = rf.Log[:args.PrevLogIndex-rf.LastIncludedIndex+i]
			rf.persist()
			break
		}
	}
	if i < len(args.Entries) {
		// Append any new entries not already in the log
		// log.Printf("%v %v appending Entries %v.\n", rf.state, rf.me, args.Entries[i:])
		rf.Log = append(rf.Log, args.Entries[i:]...)
		rf.persist()
	}
}

// with mutex. Will check consistency and set the conflict index and term. Will also trim args.Entries.
// True if follower contained entry matching prevLogIndex and prevLogTerm ()
func (rf *Raft) checkLogConsistency(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	// 	We believe the protocol the authors probably want you to follow is:
	// If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
	if rf.getLastLogIndex() < args.PrevLogIndex {
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		return false
	}

	// trim entries.

	// default arg.prevLogTerm == rf.LastIncludedTerm
	// log[prevLogIndex].Term
	var myPrevLogTerm int = rf.LastIncludedTerm

	// if lasInclueIndex contains part of the prev Log Index.
	if args.PrevLogIndex < rf.LastIncludedIndex && len(args.Entries) > 0 {
		i := rf.LastIncludedIndex - args.PrevLogIndex - 1
		args.PrevLogIndex = rf.LastIncludedIndex
		// trim args.Entries to start after LastIncludedIndex
		// 	see if entry is at lastIncludedIndex.
		if len(args.Entries) > i+1 &&
			args.Entries[i].Index == rf.LastIncludedIndex &&
			args.Entries[i].Term == rf.LastIncludedTerm {
			// trim it. We don't want duplicates.
			args.Entries = args.Entries[i+1:]
		} else {
			// the follower installed a snapshot that contains the entries already.
			// args.Entries are all committed (already in the snapshot)
			// nothing to append.
			// or conflict at lastlogindex or lastlogterm. reject the whole entry.(impossible because snapshot is coordinated)
			args.Entries = make([]LogEntry, 0)
		}
		// normal append.
	} else if args.PrevLogIndex > rf.LastIncludedIndex {
		myPrevLogTerm = rf.Log[args.PrevLogIndex-rf.LastIncludedIndex-1].Term
	}

	// If a follower does have prevLogIndex in its log, but the term does not match,
	// it should return conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has term equal to conflictTerm.
	if myPrevLogTerm != args.PrevLogTerm {
		reply.ConflictTerm = myPrevLogTerm
		for _, entry := range rf.Log {
			if entry.Term == reply.ConflictTerm {
				reply.ConflictIndex = entry.Index
				break
			}
		}
		return false
	}
	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	reply.Term = rf.CurrentTerm

	// Reply false if term < CurrentTerm
	if args.Term < rf.CurrentTerm || args.LeaderId == rf.me || rf.killed() {
		rf.mu.Unlock()
		return
	}

	// if received a bigger term then it means this server is outdated, roll back.
	// if a candidate receives the heartbeat claiming it is the leader
	// then check if the leader is the leader of the current term.
	// if so, go back to follower, else ignore.
	if args.Term > rf.CurrentTerm || (args.Term >= rf.CurrentTerm && rf.state == Candidate) {
		// fmt.Printf("Server %v rolls back to follower because a new term starts. %v -> %v\n", rf.me, rf.CurrentTerm, args.Term)
		rf.goBackToFollower(args.Term)
		reply.Term = args.Term
	}

	// fmt.Printf("Server %v snooze\n", rf.me)
	rf.snoozeAlarm()

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if !rf.checkLogConsistency(args, reply) {
		reply.FailReason = LogInConsistency
		rf.mu.Unlock()
		return
	}
	// By this time we have trimmed args.entries.

	// it means the RPC is accepted
	reply.Success = true

	if len(args.Entries) > 0 {
		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it
		// obtain the existing entries into args.Entries
		rf.processEntries(args)
	}

	if args.LeaderCommit > rf.commitIndex {
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		lastLogIndex := rf.getLastLogIndex()
		rf.commitIndex = min(args.LeaderCommit, lastLogIndex)
	}
	rf.mu.Unlock()
}

// with mutex
func (rf *Raft) makeAppendEntriesArgs(server int) *AppendEntriesArgs {
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.LastIncludedTerm

	// index is not out of bounds
	if prevLogIndex-rf.LastIncludedIndex-1 > -1 {
		prevLogTerm = rf.Log[prevLogIndex-rf.LastIncludedIndex-1].Term
	}

	var entries []LogEntry = nil
	lastLogIndex := rf.getLastLogIndex()
	nextIndex := rf.nextIndex[server]

	// send lagging entries that are in leader's log.
	// if not in leader's log then may need a snapshot.
	var replica []LogEntry
	if lastLogIndex >= nextIndex {
		if nextIndex > rf.LastIncludedIndex {
			entries = rf.Log[nextIndex-rf.LastIncludedIndex-1:]
			// DATA RACE COMES HERE!!!!!!!!!!!!
			// SOMEONE TOLD ME TO DO THIS BEFORE BUT I DELETED IT
			// I AM SO DUMB
			// so that every time when we send to the server we send a different deep copy
			// instead of a pointer to the log?
			// so we don't race with rpcs sent to other servers to access the log
			// because slide internally are shared?
			replica = make([]LogEntry, len(entries))
			copy(replica, entries)
		}
	}

	return &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      replica,
		LeaderCommit: rf.commitIndex,
	}
}

// send a AppendEntries RPC to a server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) appendEntries(server int, term int) {
	reply := &AppendEntriesReply{}
	rf.mu.Lock()
	if rf.state != Leader || rf.CurrentTerm != term || rf.killed() {
		rf.mu.Unlock()
		return
	}
	args := rf.makeAppendEntriesArgs(server)
	// log.Printf("%v %v sending logs %v to server%v.\n", rf.state, rf.me, args.Entries, server)
	rf.mu.Unlock()
	r := rf.sendAppendEntries(server, args, reply)
	if !r {
		return
	}
	rf.mu.Lock()
	// give it chance to step down
	if rf.state != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}
	// If RPC response contains term T > CurrentTerm: set CurrentTerm = T, convert to follower
	if reply.Term > rf.CurrentTerm {
		rf.goBackToFollower(reply.Term)
		rf.snoozeAlarm()
		rf.mu.Unlock()
		return
	}
	// if not the same as the original term, then stop processing the reply.
	if rf.CurrentTerm != term {
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		oldMatchIndex := rf.matchIndex[server]
		// never decrease nextIndex and matchIndex
		rf.nextIndex[server] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[server])

		rf.matchIndex[server] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
		if rf.matchIndex[server] > oldMatchIndex {
			rf.mu.Unlock()
			// matchIndex updated, maybe some log entries commitable, going to check
			go rf.checkMajorityAndCommit(term)
			rf.mu.Lock()
		}
		if rf.state != Leader || rf.CurrentTerm != term || rf.killed() {
			rf.mu.Unlock()
			return
		}
		// if follower lag too much install snapshot
		if rf.matchIndex[server] < rf.LastIncludedIndex {
			rf.mu.Unlock()
			go rf.installSnapshot(server, term)
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		return
	}

	// If AppendEntries fails because of log inconsistency:
	// decrement nextIndex and retry (replaced by fast roll back optimization)
	if reply.FailReason == LogInConsistency {
		if reply.ConflictIndex != 0 && reply.ConflictTerm == 0 {
			rf.nextIndex[server] = reply.ConflictIndex
			rf.matchIndex[server] = reply.ConflictIndex - 1
		} else {
			//Upon receiving a conflict response, the leader should first search its log for conflictTerm.
			// If it finds an entry in its log with that term, it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
			// If it does not find an entry with that term, it should set nextIndex = conflictIndex.
			var index, term int
			for i := len(rf.Log) - 1; i >= -1; i-- {
				if i < 0 {
					index, term = rf.getLastLogIndex(), rf.getLastLogTerm()
				} else {
					index, term = rf.Log[i].Index, rf.Log[i].Term
				}
				if term == reply.ConflictTerm {
					// it finds an entry in its log with that term
					rf.nextIndex[server] = index + 1
					rf.matchIndex[server] = index
					break
				}
				if term < reply.ConflictTerm {
					// it does not find an entry with that term
					rf.nextIndex[server] = reply.ConflictIndex
					rf.matchIndex[server] = reply.ConflictIndex - 1
					break
				}
				if len(rf.Log) == 0 {
					// leader's log length == 0, unable to append Entries. Need snapshot.
					rf.mu.Unlock()
					go rf.installSnapshot(server, term)
					return
				}
			}
		}

		if rf.matchIndex[server] < rf.LastIncludedIndex {
			// need to install snapshot to follower
			rf.mu.Unlock()
			go rf.installSnapshot(server, term)
			return
		} else {
			// retry with new index
			rf.mu.Unlock()
			go rf.appendEntries(server, term)
			return
		}

	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) checkMajorityAndCommit(term int) {
	rf.mu.Lock()

	// check assumptions
	if rf.state != Leader || rf.CurrentTerm != term || rf.killed() {
		rf.mu.Unlock()
		return
	}

	if rf.commitIndex >= rf.getLastLogIndex() {
		// nothing to commit
		rf.mu.Unlock()
		return
	}
	// If there exists an N such that
	// - N > commitIndex
	// - a majority of matchIndex[i] ≥ N
	// - log[N].term == CurrentTerm:
	// set commitIndex = N
	for n := rf.commitIndex + 1; n < len(rf.Log)+rf.LastIncludedIndex+1; n++ {
		count := 1 // count the number of matches
		if rf.Log[n-rf.LastIncludedIndex-1].Term == term {
			for peer := range rf.peers {
				if rf.matchIndex[peer] >= n && peer != rf.me {
					count += 1
					// fmt.Printf("Server %v matched with matchindex: %v.\n", peer, rf.matchIndex[peer])
				}
			}
			if count > len(rf.peers)/2 {
				// it means we can commit
				// fmt.Printf("Reached majority, commitIndex = %v\n", n)
				rf.commitIndex = n
			}
		}
	}
	rf.mu.Unlock()
}

// Monitors reduce number of RPCs
// Also, if we don't use monitor, the apply <- part is not locked, when we have concurrent apply we can't make sure the order is correct.
func (rf *Raft) commitMonitor(applyCh chan<- ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		// If commitIndex > lastApplied:
		// increment lastApplied,
		// apply log[lastApplied] to state machine
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			logEntry := rf.Log[rf.lastApplied-rf.LastIncludedIndex-1]
			rf.mu.Unlock()
			msg := ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: logEntry.Index,
			}
			applyCh <- msg
			rf.mu.Lock()
			// log.Printf("%v %v applied log index %v, log: %v\n.", rf.state, rf.me, logEntry.Index, logEntry)
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// get raft instance state as bytes, with mutex held
func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.CurrentTerm) != nil ||
		e.Encode(rf.VotedFor) != nil ||
		e.Encode(rf.Log) != nil ||
		e.Encode(rf.LastIncludedIndex) != nil ||
		e.Encode(rf.LastIncludedTerm) != nil {
		// encode failed?
		return nil
	} else {
		return w.Bytes()
	}
}

// save Raft's persistent state to stable storage, with mutex held
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	if data := rf.encodeRaftState(); data != nil {
		rf.persister.SaveRaftState(data)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		rf.LastIncludedIndex = lastIncludedIndex
		rf.LastIncludedTerm = lastIncludedTerm
		rf.mu.Unlock()
	}
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at Offset
}

type InstallSnapshotReply struct {
	Term int // CurrentTerm, for leader to update itself
}

// A service wants to switch to snapshot. Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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
	go rf.triggerSnapshot(index, snapshot)
}

// with mutex
func (rf *Raft) saveStateAndSnapshot(snapshotData []byte) {
	if state := rf.encodeRaftState(); state != nil {
		rf.persister.SaveStateAndSnapshot(state, snapshotData)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// with mutex
func (rf *Raft) makeInstallSnapshotArgs() *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	// Reply immediately if term < CurrentTerm
	if args.Term < rf.CurrentTerm || args.LeaderId == rf.me || rf.killed() {
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.CurrentTerm

	if args.Term > rf.CurrentTerm {
		// If RPC request contains term T > CurrentTerm: set CurrentTerm = T, convert to follower
		rf.goBackToFollower(args.Term)
		reply.Term = rf.CurrentTerm
	}

	rf.snoozeAlarm()

	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		// old snapshot
		rf.mu.Unlock()
		return
	}

	// install
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = rf.LastIncludedIndex
	rf.lastApplied = rf.LastIncludedIndex
	// log.Printf("%v %v received snapshot.\n", rf.state, rf.me)

	// if existing log entry has same index and term as snapshot's last included entry,
	// retain log entries following it and reply
	if args.LastIncludedIndex-rf.LastIncludedIndex-1 > 0 {
		existingEntry := rf.Log[args.LastIncludedIndex-rf.LastIncludedIndex-1]
		if existingEntry.Index == args.LastIncludedIndex && existingEntry.Term == args.LastIncludedTerm {
			rf.Log = rf.Log[existingEntry.Index-rf.LastIncludedIndex:]
			rf.mu.Unlock()
			return
		}
	}

	// discard the entire log
	rf.Log = make([]LogEntry, 0)
	rf.saveStateAndSnapshot(args.Data)
	rf.mu.Unlock()

	// read and apply snapshot
	// Reset state machine using snapshot contents

	if !rf.killed() {
		rf.mu.Lock()
		data := rf.persister.ReadSnapshot()
		// snapshot data invalid
		if rf.LastIncludedIndex == 0 || len(data) < 1 {
			rf.mu.Unlock()
			return
		}
		// send snapshot back to upper-level service
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      data,
			SnapshotIndex: rf.LastIncludedIndex,
			SnapshotTerm:  rf.LastIncludedTerm,
		}
		// log.Printf("%v %v installing snapshot. LastIncludedIndex=%v, LastIncludedTerm=%v, commitIndex=%v, lastApplied=%v \n", rf.state, rf.me, rf.LastIncludedIndex, rf.LastIncludedTerm, rf.commitIndex, rf.lastApplied)
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) installSnapshot(server int, term int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.CurrentTerm != term || rf.killed() {
		rf.mu.Unlock()
		return
	}
	args := rf.makeInstallSnapshotArgs()
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}
	r := rf.sendInstallSnapshot(server, args, reply)

	rf.mu.Lock()

	if rf.state != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}

	if !r {
		// retry when no reply from the server
		// go rf.installSnapshot(server, term)
		rf.mu.Unlock()
		return
	}

	if reply.Term > rf.CurrentTerm {
		// If RPC response contains term T > CurrentTerm: set CurrentTerm = T, convert to follower
		rf.goBackToFollower(reply.Term)
		rf.snoozeAlarm()
		// no longer leader, return
		rf.mu.Unlock()
		return
	}

	if rf.CurrentTerm != term {
		rf.mu.Unlock()
		return
	}
	rf.nextIndex[server] = max(args.LastIncludedIndex+1, rf.nextIndex[server])
	rf.matchIndex[server] = max(args.LastIncludedIndex, rf.matchIndex[server])
	rf.mu.Unlock()
}

func (rf *Raft) triggerSnapshot(snapshotIndex int, snapshotData []byte) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader && rf.isFollowerLagging(snapshotIndex) {
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		} else if snapshotIndex > rf.LastIncludedIndex {
			// snapshot is valid
			// save and trim info
			rf.LastIncludedTerm = rf.Log[snapshotIndex-rf.LastIncludedIndex-1].Term
			rf.Log = rf.Log[snapshotIndex-rf.LastIncludedIndex:]
			rf.LastIncludedIndex = snapshotIndex
			rf.saveStateAndSnapshot(snapshotData)
			// log.Printf("%v %v saved snapshot. LastIncludedTerm=%v, LastIncludedIndex=%v, Log=%v\n", rf.state, rf.me, rf.LastIncludedTerm, rf.LastIncludedIndex, rf.Log)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		return
	}
}

// if any follower lagging, leader keeps a small amount of trailing log for a while
// because they may not have applied some logs yet
// only install snapshot when:
// 1. the service asks for it
// AND
// 2. we are not lagging too much.
// if every time the service asks us we will do a snapshot despite some followers only lags 1 or 2 logs behind, we are really doing state transfer..
// The installSnapshot RPC handler will directly set the commitIndex... may result in a jump in commitIndex & lastApplied.
// So sometime I get applied 51 but expected 61 error... because of that.
// By giving time for the follower to catch up we can avoid the error...
// the follower will commit all of them when it catches up. And it is fast because we send as more entries as we can in on RPC...
// this is really a buffer, without this it will pass, but sometimes fails..
// with this it is a lot better.
// kind of cheat but it works.
// I think a buffer size of 20 is good enough.

// *** without buffer and this lagging check, a leaader may apply a snapshot, and send the snapshot to a follower who is still applying entries.
// This will mix up the commitIndex, and result in a failure.
// If we wait, then when the leader saves the snapshot, we know:
// 1. all followers have lastIncludedIndex in their logs. So the leader will not try to install snapshots when followers are still applying entries.
// 2. the follower lags too much behind. Then it is probabiliy crashed or disconnected. It will finish applying entries quickly after disconnecting because applying is pretty fast.
// if we don't set a upper bound we could wait forever.

// case: a follower 2 disconnected, and reconnects. Two followers, 1 and 2.
// the leader 0 tries to send entires, but it will reject because of log inconsistency. Say the index is now 5. The leader is at 12.
// the leader sends 5 to 12 to the follower. The follower is still processing and committing from 5. The reply is on the way.
// then the leader received many commands. It reaches majority because 1 replied fast. New commands gets committed. Say the leader is now at 15.
// The leader received from service that it should snapshot at index 13.
// Then the leader received reply from follower 2, but decided to install a snapshot since matchIndex < lastIncludedIndex.
// When the snapshot arrives at follower 2, follower 2's commitMonitor is still commiting index 9.
// Follower 2 installs the snapshot, and sets the lastApplied to 12. Sends a message to service, cfg.nextIndex[2] gets 13.
// CommitMonitor sends the commit message at 9.
// Error: server 2 applied index 9 but expected 13.

// Ivan made a good comment on 2023/4/5: When the leader wants to apply the snapshot at 7, it should make sure it is coordinated: everyone has 7.

func (rf *Raft) isFollowerLagging(index int) bool {
	for i := range rf.peers {
		distance := index - rf.matchIndex[i]
		if distance > 0 {
			if distance <= maxLagAmount {
				return true
			}
		}

	}
	return false
}
