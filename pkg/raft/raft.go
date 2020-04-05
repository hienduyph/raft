package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := &ConsensusModule{
		id:       id,
		peerIds:  peerIds,
		server:   server,
		state:    Follower,
		votedFor: -1,
	}
	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()
	return cm
}

type ConsensusModule struct {
	mu      sync.Mutex
	id      int
	peerIds []int
	server  *Server

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time
	nextIndex          map[int]int
	matchIndex         map[int]int
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
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}
	if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
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
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}
	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", reply)
	return nil
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
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
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

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.dlog("becomes Leader; term=%d, log=%v", cm.currentTerm, cm.log)
	// start heart beat
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			cm.leaderSendHeartbeats()
			<-ticker.C
			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	args := AppendEntriesArgs{
		Term:     savedCurrentTerm,
		LeaderId: cm.id,
	}
	sendFn := func(peerId int) {
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
		}
	}
	for _, peerId := range cm.peerIds {
		go sendFn(peerId)
	}
}

func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	format = fmt.Sprintf("[%d] ", cm.id) + format
	log.Printf(format, args...)
}
