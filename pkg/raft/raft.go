package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func NewConsensusModule(id int, peerIds []int, server *Server, storage Storage, ready <-chan interface{}, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := &ConsensusModule{
		id:                 id,
		peerIds:            peerIds,
		server:             server,
		storage:            storage,
		state:              Follower,
		commitChan:         commitChan,
		triggerChan:        make(chan struct{}, 1),
		newCommitReadyChan: make(chan struct{}, 16),
		votedFor:           -1,
		commitIndex:        -1,
		lastApplied:        -1,
		nextIndex:          make(map[int]int),
		matchIndex:         make(map[int]int),
	}
	if cm.storage.HasData() {
		cm.restoreFromState(cm.storage)
	}
	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()
	go cm.commitChanSender()
	return cm
}

type ConsensusModule struct {
	mu      sync.Mutex
	id      int
	peerIds []int
	server  *Server

	// persistent
	storage Storage

	// commitChan is the channel where this Cm is going to report commited log
	// entries. It's passed in by the client during construction
	commitChan chan<- CommitEntry
	// newCommitReadyChan is an internal notification channel used by goroutines
	// that commitn new entries to the log to notify that these entries may be sent
	// on commitChan
	newCommitReadyChan chan struct{}

	// triggerChan is an internal notification channel used to triggerChan
	// sending new AEs to followers when interesting changes occurred
	triggerChan chan struct{}

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time
	// Volatile Raft state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.dlog("becomes Dead")
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}
	if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.dlog("AppendEntries %+v", args)
	if args.Term >= cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}
	reply.Success = false
	if args.Term != cm.currentTerm {
		reply.Term = cm.currentTerm
		cm.dlog("AppendEntries reply: %+v", reply)
		return nil
	}
	if cm.state != Follower {
		cm.becomeFollower(args.Term)
	}
	cm.electionResetEvent = time.Now()
	if args.PrevLogIndex == -1 || (args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
		reply.Success = true
		logInsertIndex := args.PrevLogIndex + 1
		newEntriesIndex := 0
		for {
			if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
				break
			}
			if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
				break
			}
			logInsertIndex++
			newEntriesIndex++
		}
		if newEntriesIndex < len(args.Entries) {
			cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
			cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
			cm.dlog("... log is now: %v", cm.log)
		}
		if args.LeaderCommit > cm.commitIndex {
			cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
			cm.dlog("... setting commitIndex=%d", cm.commitIndex)
			cm.newCommitReadyChan <- struct{}{}
		}
	}
	return nil
}

// implement command and log replication
func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()
	cm.dlog("Submit received by %v:%v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.persistToStorage()
		cm.dlog("... log=%v", cm.log)
		cm.mu.Unlock()
		cm.triggerChan <- struct{}{}
		return true
	}
	cm.mu.Unlock()
	return false
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election time started (%v), term=%d", timeoutDuration, termStarted)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("in election timer state=%s, balling out", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.dlog("in election timer, term changed from %d to %d, balling out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}
		// start an election if we haven't heard from a leader or voted for
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	}
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// startElection starts new election with this cm as a candidate
// Expects cm.mu be locked
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)
	var votesRedeived int32 = 1

	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}
			var reply RequestVoteReply
			cm.dlog("sending RequestVote to %d: %+v", peerId, args)
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err != nil {
				return
			}
			cm.mu.Lock()
			defer cm.mu.Unlock()
			cm.dlog("received RequestVoteReply %+v", reply)
			if cm.state != Candidate {
				cm.dlog("while waiting for reply, state changed to %v", cm.state)
				return
			}
			if reply.Term > savedCurrentTerm {
				cm.dlog("term out of date in RequestVoteReply")
				cm.becomeFollower(reply.Term)
				return
			}
			if reply.Term == savedCurrentTerm && reply.VoteGranted {
				votes := int(atomic.AddInt32(&votesRedeived, 1))
				if votes*2 > len(cm.peerIds)+1 {
					cm.dlog("wins election with %d votes", votes)
					cm.startLeader()
					return
				}
			}
		}(peerId)
	}
}

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	}
	return -1, -1
}

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}
	cm.dlog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v, log=%v", cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)
	// start heart beat
	go func(heartBeatTimeout time.Duration) {
		cm.leaderSendAEs()
		ticker := time.NewTimer(heartBeatTimeout)
		defer ticker.Stop()
		for {
			doSend := false
			select {
			case <-ticker.C:
				doSend = true
				ticker.Stop()
				ticker.Reset(heartBeatTimeout)
			case _, ok := <-cm.triggerChan:
				if ok {
					doSend = true
				} else {
					return
				}
				// reset for heartbeat
				if !ticker.Stop() {
					<-ticker.C
				}
				ticker.Reset(heartBeatTimeout)
			}

			if !doSend {
				continue
			}
			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
			cm.leaderSendAEs()
		}
	}(50 * time.Millisecond)
}

func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	sendFn := func(peerId int) {
		cm.mu.Lock()
		ni := cm.nextIndex[peerId]
		prevLogIndex := ni - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = cm.log[prevLogIndex].Term
		}
		entries := cm.log[ni:]
		args := AppendEntriesArgs{
			Term:         savedCurrentTerm,
			LeaderId:     cm.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: cm.commitIndex,
		}
		cm.mu.Unlock()
		cm.dlog("sending AppendEntries to %v, ni=%d, args=%+v", peerId, 0, args)
		var reply AppendEntriesReply
		if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err != nil {
			return
		}
		cm.mu.Lock()
		defer cm.mu.Unlock()
		if reply.Term > savedCurrentTerm {
			cm.dlog("term out of date in heartbeat reply")
			cm.becomeFollower(reply.Term)
			return
		}
		if cm.state != Leader || savedCurrentTerm != reply.Term {
			cm.nextIndex[peerId] = ni - 1
			cm.dlog("AppendEntries reply from %d !success, nextIndex := %d", peerId, ni-1)
			return
		}
		if !reply.Success {
			// resolve conflict
			if reply.ConflictTerm < 0 {
				cm.nextIndex[peerId] = reply.ConflictIndex
				return
			}
			lastIndexOfTerm := -1
			for i := len(cm.log) - 1; i >= 0; i-- {
				if cm.log[i].Term == reply.ConflictTerm {
					lastIndexOfTerm = i
					break
				}
			}
			if lastIndexOfTerm >= 0 {
				cm.nextIndex[peerId] = lastIndexOfTerm + 1
			} else {
				cm.nextIndex[peerId] = reply.ConflictIndex
			}
		}

		cm.nextIndex[peerId] = ni + len(entries)
		cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
		cm.dlog(
			"AppendEntries reply from %d success: nextIndex := %v, matchIndex:= %v",
			peerId,
			cm.nextIndex,
			cm.matchIndex,
		)
		savedCommitIndex := cm.commitIndex
		for i := cm.commitIndex + 1; i < len(cm.log); i++ {
			if cm.log[i].Term == cm.currentTerm {
				matchCount := 1
				for _, peerId := range cm.peerIds {
					if cm.matchIndex[peerId] >= i {
						matchCount++
					}
				}
				if matchCount*2 > len(cm.peerIds)+1 {
					cm.commitIndex = i
				}
			}
		}
		if cm.commitIndex != savedCommitIndex {
			cm.dlog("leader sets commitIndex := %d", cm.commitIndex)
			cm.newCommitReadyChan <- struct{}{}
			cm.triggerChan <- struct{}{}
		}
	}

	for _, peerId := range cm.peerIds {
		go sendFn(peerId)
	}
}

func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		cm.mu.Lock()
		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied
		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)
		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	cm.dlog("commitChanSender done")
}

func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	format = fmt.Sprintf("[%d] ", cm.id) + format
	log.Printf(format, args...)
}

// restoreFromState restores the persistent stat of this CM from storage.
// its should be call during constructor, before any concurrency converts
func (cm *ConsensusModule) restoreFromState(storage Storage) {
	if termData, err := cm.storage.Get("currentTerm"); err != nil {
		log.Fatal(err)
	} else if err = gob.NewDecoder(bytes.NewBuffer(termData)).Decode(*&cm.currentTerm); err != nil {
		log.Fatal(err)
	}

	if votedFor, err := cm.storage.Get("votedFor"); err != nil {
		log.Fatal(err)
	} else if err = gob.NewDecoder(bytes.NewBuffer(votedFor)).Decode(&cm.votedFor); err != nil {
		log.Fatal(err)
	}

	if logData, err := cm.storage.Get("log"); err != nil {
		log.Fatal(err)
	} else if err = gob.NewDecoder(bytes.NewBuffer(logData)).Decode(&cm.log); err != nil {
		log.Fatal(err)
	}
}

func (cm *ConsensusModule) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var votedFor bytes.Buffer
	if err := gob.NewEncoder(&votedFor).Encode(cm.votedFor); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("votedFor", votedFor.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("log", logData.Bytes())
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
