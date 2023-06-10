package cluster

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/lock"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const slotCount int = 16384

type raftState int

func init() {
	rand.Seed(time.Now().UnixNano())
}

const (
	nodeFlagLeader uint32 = 1 << iota
	nodeFlagCandidate
	nodeFlagLearner
)

const (
	follower raftState = iota
	leader
	candidate
	learner
)

var stateNames = map[raftState]string{
	follower:  "follower",
	leader:    "leader",
	candidate: "candidate",
	learner:   "learner",
}

func (node *Node) setState(state raftState) {
	node.Flags &= ^uint32(0x7) // clean
	switch state {
	case follower:
		break
	case leader:
		node.Flags |= nodeFlagLeader
	case candidate:
		node.Flags |= nodeFlagCandidate
	case learner:
		node.Flags |= nodeFlagLearner
	}
}

func (node *Node) getState() raftState {
	if node.Flags&nodeFlagLeader > 0 {
		return leader
	}
	if node.Flags&nodeFlagCandidate > 0 {
		return candidate
	}
	if node.Flags&nodeFlagLearner > 0 {
		return learner
	}
	return follower
}

type logEntry struct {
	Term  int
	Index int
	Event int
	wg    *sync.WaitGroup
	// payload
	SlotIDs []uint32
	NodeID  string
	Addr    string
}

func (e *logEntry) marshal() []byte {
	bin, _ := json.Marshal(e)
	return bin
}

func (e *logEntry) unmarshal(bin []byte) error {
	err := json.Unmarshal(bin, e)
	if err != nil {
		return fmt.Errorf("illegal message: %v", err)
	}
	return nil
}

type Raft struct {
	cluster        *Cluster
	mu             sync.RWMutex
	selfNodeID     string
	slots          []*Slot
	leaderId       string
	nodes          map[string]*Node
	log            []*logEntry // log index begin from 0
	baseIndex      int         // baseIndex + 1 == log[0].Index, it can be considered as the previous log index
	baseTerm       int         // baseTerm is the term of the previous log entry
	state          raftState
	term           int
	votedFor       string
	voteCount      int
	committedIndex int // index of the last committed logEntry
	proposedIndex  int // index of the last proposed logEntry
	heartbeatChan  chan *heartbeat
	persistFile    string
	electionAlarm  time.Time
	closeChan      chan struct{}
	closed         bool

	// for leader
	nodeIndexMap map[string]*nodeStatus
	nodeLock     *lock.Locks
}

func newRaft(cluster *Cluster, persistFilename string) *Raft {
	return &Raft{
		cluster:     cluster,
		persistFile: persistFilename,
		closeChan:   make(chan struct{}),
	}
}

type heartbeat struct {
	sender   string
	term     int
	entries  []*logEntry
	commitTo int
}

type nodeStatus struct {
	receivedIndex int // received log index, not committed index
}

func (raft *Raft) GetNodes() []*Node {
	raft.mu.RLock()
	defer raft.mu.RUnlock()
	result := make([]*Node, 0, len(raft.nodes))
	for _, v := range raft.nodes {
		result = append(result, v)
	}
	return result
}

func (raft *Raft) GetNode(nodeID string) *Node {
	raft.mu.RLock()
	defer raft.mu.RUnlock()
	return raft.nodes[nodeID]
}

func (raft *Raft) getLogEntries(beg, end int) []*logEntry {
	if beg <= raft.baseIndex || end > raft.baseIndex+len(raft.log)+1 {
		return nil
	}
	i := beg - raft.baseIndex - 1
	j := end - raft.baseIndex - 1
	return raft.log[i:j]
}

func (raft *Raft) getLogEntriesFrom(beg int) []*logEntry {
	if beg <= raft.baseIndex {
		return nil
	}
	i := beg - raft.baseIndex - 1
	return raft.log[i:]
}

func (raft *Raft) getLogEntry(idx int) *logEntry {
	if idx < raft.baseIndex || idx >= raft.baseIndex+len(raft.log) {
		return nil
	}
	return raft.log[idx-raft.baseIndex]
}

func (raft *Raft) initLog(baseTerm, baseIndex int, entries []*logEntry) {
	raft.baseIndex = baseIndex
	raft.baseTerm = baseTerm
	raft.log = entries
}

const (
	electionTimeoutMaxMs = 4000
	electionTimeoutMinMs = 2800
)

func randRange(from, to int) int {
	return rand.Intn(to-from) + from
}

// nextElectionAlarm generates normal election timeout, with randomness
func nextElectionAlarm() time.Time {
	return time.Now().Add(time.Duration(randRange(electionTimeoutMinMs, electionTimeoutMaxMs)) * time.Millisecond)
}

func compareLogIndex(term1, index1, term2, index2 int) int {
	if term1 != term2 {
		return term1 - term2
	}
	return index1 - index2
}

func (cluster *Cluster) asRaft() *Raft {
	return cluster.topology.(*Raft)
}

// StartAsSeed starts cluster as seed node
func (raft *Raft) StartAsSeed(listenAddr string) protocol.ErrorReply {
	selfNodeID := listenAddr
	raft.mu.Lock()
	defer raft.mu.Unlock()
	raft.slots = make([]*Slot, slotCount)
	// claim all slots
	for i := range raft.slots {
		raft.slots[i] = &Slot{
			ID:     uint32(i),
			NodeID: selfNodeID,
		}
	}
	raft.selfNodeID = selfNodeID
	raft.leaderId = selfNodeID
	raft.nodes = make(map[string]*Node)
	raft.nodes[selfNodeID] = &Node{
		ID:    selfNodeID,
		Addr:  listenAddr,
		Slots: raft.slots,
	}
	raft.nodes[selfNodeID].setState(leader)
	raft.nodeIndexMap = map[string]*nodeStatus{
		selfNodeID: {
			receivedIndex: raft.proposedIndex,
		},
	}
	raft.start(leader)
	raft.cluster.self = selfNodeID
	return nil
}

func (raft *Raft) GetSlots() []*Slot {
	return raft.slots
}

// GetSelfNodeID returns node id of current node
func (raft *Raft) GetSelfNodeID() string {
	return raft.selfNodeID
}

const raftClosed = "ERR raft has closed"

func (raft *Raft) start(state raftState) {
	raft.state = state
	raft.heartbeatChan = make(chan *heartbeat, 1)
	raft.electionAlarm = nextElectionAlarm()
	//raft.nodeIndexMap = make(map[string]*nodeStatus)
	go func() {
		for {
			if raft.closed {
				logger.Info("quit raft job")
				return
			}
			switch raft.state {
			case follower:
				raft.followerJob()
			case candidate:
				raft.candidateJob()
			case leader:
				raft.leaderJob()
			}
		}
	}()
}

func (raft *Raft) Close() error {
	raft.closed = true
	close(raft.closeChan)
	return raft.persist()
}

func (raft *Raft) followerJob() {
	electionTimeout := time.Until(raft.electionAlarm)
	select {
	case hb := <-raft.heartbeatChan:
		raft.mu.Lock()
		nodeId := hb.sender
		raft.nodes[nodeId].lastHeard = time.Now()
		// todo: drop duplicate entry
		raft.log = append(raft.log, hb.entries...)
		raft.proposedIndex += len(hb.entries)
		raft.applyLogEntries(raft.getLogEntries(raft.committedIndex+1, hb.commitTo+1))
		raft.committedIndex = hb.commitTo
		raft.electionAlarm = nextElectionAlarm()
		raft.mu.Unlock()
	case <-time.After(electionTimeout):
		// change to candidate
		logger.Info("raft leader timeout")
		raft.mu.Lock()
		raft.electionAlarm = nextElectionAlarm()
		if raft.votedFor != "" {
			// received request-vote and has voted during waiting timeout
			raft.mu.Unlock()
			logger.Infof("%s has voted for %s, give up being a candidate", raft.selfNodeID, raft.votedFor)
			return
		}
		logger.Info("change to candidate")
		raft.state = candidate
		raft.mu.Unlock()
	case <-raft.closeChan:
		return
	}
}

func (raft *Raft) getLogProgressWithinLock() (int, int) {
	var lastLogTerm, lastLogIndex int
	if len(raft.log) > 0 {
		lastLog := raft.log[len(raft.log)-1]
		lastLogTerm = lastLog.Term
		lastLogIndex = lastLog.Index
	} else {
		lastLogTerm = raft.baseTerm
		lastLogIndex = raft.baseIndex
	}
	return lastLogTerm, lastLogIndex
}

func (raft *Raft) candidateJob() {
	raft.mu.Lock()

	raft.term++
	raft.votedFor = raft.selfNodeID
	raft.voteCount++
	currentTerm := raft.term
	lastLogTerm, lastLogIndex := raft.getLogProgressWithinLock()
	req := &voteReq{
		nodeID:       raft.selfNodeID,
		lastLogTerm:  lastLogTerm,
		lastLogIndex: lastLogIndex,
		term:         raft.term,
	}
	raft.mu.Unlock()
	args := append([][]byte{
		[]byte("raft"),
		[]byte("request-vote"),
	}, req.marshal()...)
	conn := connection.NewFakeConn()
	wg := sync.WaitGroup{}
	elected := make(chan struct{}, len(raft.nodes)) // may receive many elected message during an election, only handle the first one
	voteFinished := make(chan struct{})
	for nodeID := range raft.nodes {
		if nodeID == raft.selfNodeID {
			continue
		}
		nodeID := nodeID
		wg.Add(1)
		go func() {
			defer wg.Done()
			rawResp := raft.cluster.relay(nodeID, conn, args)
			if err, ok := rawResp.(protocol.ErrorReply); ok {
				logger.Info(fmt.Sprintf("cannot get vote response from %s, %v", nodeID, err))
				return
			}
			respBody, ok := rawResp.(*protocol.MultiBulkReply)
			if !ok {
				logger.Info(fmt.Sprintf("cannot get vote response from %s, not a multi bulk reply", nodeID))
				return
			}
			resp := &voteResp{}
			err := resp.unmarshal(respBody.Args)
			if err != nil {
				logger.Info(fmt.Sprintf("cannot get vote response from %s, %v", nodeID, err))
				return
			}

			raft.mu.Lock()
			defer raft.mu.Unlock()
			logger.Info("received vote response from " + nodeID)
			// check-lock-check
			if currentTerm != raft.term || raft.state != candidate {
				// vote has finished during waiting lock
				logger.Info("vote has finished during waiting lock, current term " + strconv.Itoa(raft.term) + " state " + strconv.Itoa(int(raft.state)))
				return
			}
			if resp.term > raft.term {
				logger.Infof(fmt.Sprintf("vote response from %s has newer term %d", nodeID, resp.term))
				raft.term = resp.term
				raft.state = follower
				raft.votedFor = ""
				raft.leaderId = resp.voteFor
				return
			}

			if resp.voteFor == raft.selfNodeID {
				logger.Infof(fmt.Sprintf("get vote from %s", nodeID))
				raft.voteCount++
				if raft.voteCount >= len(raft.nodes)/2+1 {
					logger.Info("elected to be the leader")
					raft.state = leader
					elected <- struct{}{} // notify the main goroutine to stop waiting
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		voteFinished <- struct{}{}
	}()

	// wait vote finished or elected
	select {
	case <-voteFinished:
		raft.mu.Lock()
		if raft.term == currentTerm && raft.state == candidate {
			logger.Infof("%s failed to be elected, back to follower", raft.selfNodeID)
			raft.state = follower
			raft.votedFor = ""
			raft.voteCount = 0
		}
		raft.mu.Unlock()
	case <-elected:
		raft.votedFor = ""
		raft.voteCount = 0
		logger.Info("win election, take leader of  term " + strconv.Itoa(currentTerm))
	case <-raft.closeChan:
		return
	}
}

// getNodeIndexMap ask offset of each node and init nodeIndexMap as new leader
// invoker provide lock
func (raft *Raft) getNodeIndexMap() {
	// ask node index
	nodeIndexMap := make(map[string]*nodeStatus)
	for _, node := range raft.nodes {
		status := raft.askNodeIndex(node)
		if status != nil {
			nodeIndexMap[node.ID] = status
		}
	}
	logger.Info("got offsets of nodes")
	raft.nodeIndexMap = nodeIndexMap
}

// askNodeIndex ask another node for its log index
// return nil if failed
func (raft *Raft) askNodeIndex(node *Node) *nodeStatus {
	if node.ID == raft.selfNodeID {
		return &nodeStatus{
			receivedIndex: raft.proposedIndex,
		}
	}
	logger.Debugf("ask %s for offset", node.ID)
	c := connection.NewFakeConn()
	reply := raft.cluster.relay(node.Addr, c, utils.ToCmdLine("raft", "get-offset"))
	if protocol.IsErrorReply(reply) {
		logger.Infof("ask node %s index failed: %v", node.ID, reply)
		return nil
	}
	return &nodeStatus{
		receivedIndex: int(reply.(*protocol.IntReply).Code),
	}
}

func (raft *Raft) leaderJob() {
	raft.mu.Lock()
	if raft.nodeIndexMap == nil {
		// getNodeIndexMap with lock, because leader cannot work without nodeIndexMap
		raft.getNodeIndexMap()
	}
	if raft.nodeLock == nil {
		raft.nodeLock = lock.Make(1024)
	}
	var recvedIndices []int
	for _, status := range raft.nodeIndexMap {
		recvedIndices = append(recvedIndices, status.receivedIndex)
	}
	sort.Slice(recvedIndices, func(i, j int) bool {
		return recvedIndices[i] > recvedIndices[j]
	})
	// more than half of the nodes received entries, can be committed
	commitTo := 0
	if len(recvedIndices) > 0 {
		commitTo = recvedIndices[len(recvedIndices)/2]
	}
	// new node (received index is 0) may cause commitTo less than raft.committedIndex
	if commitTo > raft.committedIndex {
		toCommit := raft.getLogEntries(raft.committedIndex+1, commitTo+1) // left inclusive, right exclusive
		raft.applyLogEntries(toCommit)
		raft.committedIndex = commitTo
		for _, entry := range toCommit {
			if entry.wg != nil {
				entry.wg.Done()
			}
		}
	}
	// save receivedIndex in local variable in case changed by other goroutines
	proposalIndex := raft.proposedIndex
	snapshot := raft.makeSnapshot() // the snapshot is consistent with the committed log
	for _, node := range raft.nodes {
		if node.ID == raft.selfNodeID {
			continue
		}
		node := node
		status := raft.nodeIndexMap[node.ID]
		go func() {
			raft.nodeLock.Lock(node.ID)
			defer raft.nodeLock.UnLock(node.ID)
			var cmdLine [][]byte
			if status == nil {
				logger.Debugf("node %s offline", node.ID)
				status = raft.askNodeIndex(node)
				if status != nil {
					// get status, node has back online
					raft.mu.Lock()
					raft.nodeIndexMap[node.ID] = status
					raft.mu.Unlock()
				} else {
					// node still offline
					return
				}
			}
			if status.receivedIndex < raft.baseIndex {
				// some entries are missed due to change of leader, send full snapshot
				cmdLine = utils.ToCmdLine(
					"raft",
					"load-snapshot",
					raft.selfNodeID,
				)
				// see makeSnapshotForFollower
				cmdLine = append(cmdLine, []byte(node.ID), []byte(strconv.Itoa(int(follower))))
				cmdLine = append(cmdLine, snapshot[2:]...)
			} else {
				// leader has all needed entries, send normal heartbeat
				req := &heartbeatRequest{
					leaderId: raft.leaderId,
					term:     raft.term,
					commitTo: commitTo,
				}
				// append new entries to heartbeat payload
				if proposalIndex > status.receivedIndex {
					req.prevLogTerm = raft.getLogEntry(status.receivedIndex).Term
					req.prevLogIndex = status.receivedIndex
					req.entries = raft.getLogEntriesFrom(status.receivedIndex + 1)
				}
				cmdLine = utils.ToCmdLine(
					"raft",
					"heartbeat",
				)
				cmdLine = append(cmdLine, req.marshal()...)
			}

			conn := connection.NewFakeConn()
			resp := raft.cluster.relay(node.ID, conn, cmdLine)
			switch respPayload := resp.(type) {
			case *protocol.MultiBulkReply:
				term, _ := strconv.Atoi(string(respPayload.Args[0]))
				recvedIndex, _ := strconv.Atoi(string(respPayload.Args[1]))
				if term > raft.term {
					// todo: rejoin as follower
					return
				}
				raft.mu.Lock()
				raft.nodeIndexMap[node.ID].receivedIndex = recvedIndex
				raft.mu.Unlock()
			case protocol.ErrorReply:
				if respPayload.Error() == prevLogMismatch {
					cmdLine = utils.ToCmdLine(
						"raft",
						"load-snapshot",
						raft.selfNodeID,
					)
					cmdLine = append(cmdLine, []byte(node.ID), []byte(strconv.Itoa(int(follower))))
					cmdLine = append(cmdLine, snapshot[2:]...)
					resp := raft.cluster.relay(node.ID, conn, cmdLine)
					if err, ok := resp.(protocol.ErrorReply); ok {
						logger.Errorf("heartbeat to %s failed: %v", node.ID, err)
						return
					}
				} else if respPayload.Error() == nodeNotReady {
					logger.Infof("%s is not ready yet", node.ID)
					return
				} else {
					logger.Errorf("heartbeat to %s failed: %v", node.ID, respPayload.Error())
					return
				}

			}
		}()
	}
	raft.mu.Unlock()
	time.Sleep(time.Millisecond * 1000)
}

func init() {
	registerCmd("raft", execRaft)
}

func execRaft(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	raft := cluster.asRaft()
	if raft.closed {
		return protocol.MakeErrReply(raftClosed)
	}
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("raft")
	}
	subCmd := strings.ToLower(string(args[1]))
	switch subCmd {
	case "request-vote":
		// command line: raft request-vote nodeId index term
		// Decide whether to vote when other nodes solicit votes
		return execRaftRequestVote(cluster, c, args[2:])
	case "heartbeat":
		// execRaftHeartbeat handles heartbeat from leader as follower or learner
		// command line: raft heartbeat nodeID term number-of-log-log log log
		return execRaftHeartbeat(cluster, c, args[2:])
	case "load-snapshot":
		// execRaftLoadSnapshot load snapshot from leader
		// command line: raft load-snapshot leaderId snapshot(see raft.makeSnapshot)
		return execRaftLoadSnapshot(cluster, c, args[2:])
	case "propose":
		// execRaftPropose handles event proposal as leader
		// command line: raft propose <logEntry>
		return execRaftPropose(cluster, c, args[2:])
	case "join":
		// execRaftJoin handles requests from a new node to join raft group as leader
		// command line: raft join <address>
		return execRaftJoin(cluster, c, args[2:])
	case "get-leader":
		// execRaftGetLeader returns leader id and address
		return execRaftGetLeader(cluster, c, args[2:])
	case "get-offset":
		// execRaftGetOffset returns log offset of current leader
		return execRaftGetOffset(cluster, c, args[2:])
	}
	return protocol.MakeErrReply(" ERR unknown raft sub command '" + subCmd + "'")
}

type voteReq struct {
	nodeID       string
	term         int
	lastLogIndex int
	lastLogTerm  int
}

func (req *voteReq) marshal() [][]byte {
	lastLogIndexBin := []byte(strconv.Itoa(req.lastLogIndex))
	lastLogTermBin := []byte(strconv.Itoa(req.lastLogTerm))
	termBin := []byte(strconv.Itoa(req.term))
	return [][]byte{
		[]byte(req.nodeID),
		termBin,
		lastLogIndexBin,
		lastLogTermBin,
	}
}

func (req *voteReq) unmarshal(bin [][]byte) error {
	req.nodeID = string(bin[0])
	term, err := strconv.Atoi(string(bin[1]))
	if err != nil {
		return fmt.Errorf("illegal term %s", string(bin[2]))
	}
	req.term = term
	logIndex, err := strconv.Atoi(string(bin[2]))
	if err != nil {
		return fmt.Errorf("illegal index %s", string(bin[1]))
	}
	req.lastLogIndex = logIndex
	logTerm, err := strconv.Atoi(string(bin[3]))
	if err != nil {
		return fmt.Errorf("illegal index %s", string(bin[1]))
	}
	req.lastLogTerm = logTerm
	return nil
}

type voteResp struct {
	voteFor string
	term    int
}

func (resp *voteResp) unmarshal(bin [][]byte) error {
	if len(bin) != 2 {
		return errors.New("illegal vote resp length")
	}
	resp.voteFor = string(bin[0])
	term, err := strconv.Atoi(string(bin[1]))
	if err != nil {
		return fmt.Errorf("illegal term: %s", string(bin[1]))
	}
	resp.term = term
	return nil
}

func (resp *voteResp) marshal() [][]byte {
	return [][]byte{
		[]byte(resp.voteFor),
		[]byte(strconv.Itoa(resp.term)),
	}
}

// execRaftRequestVote command line: raft request-vote nodeID index term
// Decide whether to vote when other nodes solicit votes
func execRaftRequestVote(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 4 {
		return protocol.MakeArgNumErrReply("raft request-vote")
	}
	req := &voteReq{}
	err := req.unmarshal(args)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	raft := cluster.asRaft()
	raft.mu.Lock()
	defer raft.mu.Unlock()
	logger.Info("recv request vote from " + req.nodeID + ", term: " + strconv.Itoa(req.term))
	resp := &voteResp{}
	if req.term < raft.term {
		resp.term = raft.term
		resp.voteFor = raft.leaderId // tell candidate the new leader
		logger.Info("deny request vote from " + req.nodeID + " for earlier term")
		return protocol.MakeMultiBulkReply(resp.marshal())
	}
	// todo: if req.term > raft.term step down as leader?
	lastLogTerm, lastLogIndex := raft.getLogProgressWithinLock()
	if compareLogIndex(req.lastLogTerm, req.lastLogIndex, lastLogTerm, lastLogIndex) < 0 {
		resp.term = raft.term
		resp.voteFor = raft.votedFor
		logger.Info("deny request vote from " + req.nodeID + " for log progress")
		logger.Info("request vote proposal index " + strconv.Itoa(req.lastLogIndex) + " self index " + strconv.Itoa(raft.proposedIndex))
		return protocol.MakeMultiBulkReply(resp.marshal())
	}
	if raft.votedFor != "" && raft.votedFor != raft.selfNodeID {
		resp.term = raft.term
		resp.voteFor = raft.votedFor
		logger.Info("deny request vote from " + req.nodeID + " for voted")
		return protocol.MakeMultiBulkReply(resp.marshal())
	}
	if raft.votedFor == raft.selfNodeID &&
		raft.voteCount == 1 {
		// cancel vote for self to avoid live lock
		raft.votedFor = ""
		raft.voteCount = 0
	}
	logger.Info("accept request vote from " + req.nodeID)
	raft.votedFor = req.nodeID
	raft.term = req.term
	raft.electionAlarm = nextElectionAlarm()
	resp.voteFor = req.nodeID
	resp.term = raft.term
	return protocol.MakeMultiBulkReply(resp.marshal())
}

type heartbeatRequest struct {
	leaderId     string
	term         int
	commitTo     int
	prevLogTerm  int
	prevLogIndex int
	entries      []*logEntry
}

func (req *heartbeatRequest) marshal() [][]byte {
	cmdLine := utils.ToCmdLine(
		req.leaderId,
		strconv.Itoa(req.term),
		strconv.Itoa(req.commitTo),
	)
	if len(req.entries) > 0 {
		cmdLine = append(cmdLine,
			[]byte(strconv.Itoa(req.prevLogTerm)),
			[]byte(strconv.Itoa(req.prevLogIndex)),
		)
		for _, entry := range req.entries {
			cmdLine = append(cmdLine, entry.marshal())
		}
	}
	return cmdLine
}

func (req *heartbeatRequest) unmarshal(args [][]byte) protocol.ErrorReply {
	if len(args) < 6 && len(args) != 3 {
		return protocol.MakeArgNumErrReply("raft heartbeat")
	}
	req.leaderId = string(args[0])
	var err error
	req.term, err = strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("illegal term: " + string(args[1]))
	}
	req.commitTo, err = strconv.Atoi(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply("illegal commitTo: " + string(args[2]))
	}
	if len(args) > 3 {
		req.prevLogTerm, err = strconv.Atoi(string(args[3]))
		if err != nil {
			return protocol.MakeErrReply("illegal commitTo: " + string(args[3]))
		}
		req.prevLogIndex, err = strconv.Atoi(string(args[4]))
		if err != nil {
			return protocol.MakeErrReply("illegal commitTo: " + string(args[4]))
		}
		for _, bin := range args[5:] {
			entry := &logEntry{}
			err = entry.unmarshal(bin)
			if err != nil {
				return protocol.MakeErrReply(err.Error())
			}
			req.entries = append(req.entries, entry)
		}
	}
	return nil
}

const prevLogMismatch = "prev log mismatch"
const nodeNotReady = "not ready"

// execRaftHeartbeat receives heartbeat from leader
// command line: raft heartbeat nodeID term commitTo prevTerm prevIndex [log entry]
// returns term and received index
func execRaftHeartbeat(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	raft := cluster.asRaft()
	req := &heartbeatRequest{}
	unmarshalErr := req.unmarshal(args)
	if unmarshalErr != nil {
		return unmarshalErr
	}
	if req.term < raft.term {
		return protocol.MakeMultiBulkReply(utils.ToCmdLine(
			strconv.Itoa(req.term),
			strconv.Itoa(raft.proposedIndex), // new received index
		))
	} else if req.term > raft.term {
		logger.Info("accept new leader " + req.leaderId)
		raft.mu.Lock()
		// todo: if current node is not at follower state
		raft.term = req.term
		raft.votedFor = ""
		raft.leaderId = req.leaderId
		raft.mu.Unlock()
	}
	raft.mu.RLock()
	// heartbeat may arrive earlier than follower ready
	if raft.heartbeatChan == nil {
		raft.mu.RUnlock()
		return protocol.MakeErrReply(nodeNotReady)
	}
	if len(req.entries) > 0 && compareLogIndex(req.prevLogTerm, req.prevLogIndex, raft.baseTerm, raft.baseIndex) != 0 {
		raft.mu.RUnlock()
		return protocol.MakeErrReply(prevLogMismatch)
	}
	raft.mu.RUnlock()

	raft.heartbeatChan <- &heartbeat{
		sender:   req.leaderId,
		term:     req.term,
		entries:  req.entries,
		commitTo: req.commitTo,
	}
	return protocol.MakeMultiBulkReply(utils.ToCmdLine(
		strconv.Itoa(req.term),
		strconv.Itoa(raft.proposedIndex+len(req.entries)), // new received index
	))
}

// execRaftLoadSnapshot load snapshot from leader
// command line: raft load-snapshot leaderId snapshot(see raft.makeSnapshot)
func execRaftLoadSnapshot(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	// leaderId snapshot
	if len(args) < 5 {
		return protocol.MakeArgNumErrReply("raft load snapshot")
	}
	raft := cluster.asRaft()
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if errReply := raft.loadSnapshot(args[1:]); errReply != nil {
		return errReply
	}
	sender := string(args[0])
	raft.heartbeatChan <- &heartbeat{
		sender:   sender,
		term:     raft.term,
		entries:  nil,
		commitTo: raft.committedIndex,
	}
	return protocol.MakeMultiBulkReply(utils.ToCmdLine(
		strconv.Itoa(raft.term),
		strconv.Itoa(raft.proposedIndex),
	))
}

var wgPool = sync.Pool{
	New: func() interface{} {
		return &sync.WaitGroup{}
	},
}

// execRaftGetLeader returns leader id and address
func execRaftGetLeader(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	raft := cluster.asRaft()
	raft.mu.RLock()
	leaderNode := raft.nodes[raft.leaderId]
	raft.mu.RUnlock()
	return protocol.MakeMultiBulkReply(utils.ToCmdLine(
		leaderNode.ID,
		leaderNode.Addr,
	))
}

// execRaftGetOffset returns log offset of current leader
func execRaftGetOffset(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	raft := cluster.asRaft()
	raft.mu.RLock()
	proposalIndex := raft.proposedIndex
	//committedIndex := raft.committedIndex
	raft.mu.RUnlock()
	return protocol.MakeIntReply(int64(proposalIndex))
}

// invoker should provide with raft.mu lock
func (raft *Raft) persist() error {
	if raft.persistFile == "" {
		return nil
	}
	tmpFile, err := os.CreateTemp(config.Properties.Dir, "tmp-cluster-conf-*.conf")
	if err != nil {
		return err
	}
	snapshot := raft.makeSnapshot()
	buf := bytes.NewBuffer(nil)
	for _, line := range snapshot {
		buf.Write(line)
		buf.WriteByte('\n')
	}
	_, err = tmpFile.Write(buf.Bytes())
	if err != nil {
		return err
	}
	err = os.Rename(tmpFile.Name(), raft.persistFile)
	if err != nil {
		return err
	}
	return nil
}

// execRaftPropose handles requests from other nodes (follower or learner) to propose a change
// command line: raft propose <logEntry>
func execRaftPropose(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	raft := cluster.asRaft()
	if raft.state != leader {
		leaderNode := raft.nodes[raft.leaderId]
		return protocol.MakeErrReply("NOT LEADER " + leaderNode.ID + " " + leaderNode.Addr)
	}
	if len(args) != 1 {
		return protocol.MakeArgNumErrReply("raft propose")
	}

	e := &logEntry{}
	err := e.unmarshal(args[0])
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if errReply := raft.propose(e); errReply != nil {
		return errReply
	}
	return protocol.MakeOkReply()
}

func (raft *Raft) propose(e *logEntry) protocol.ErrorReply {
	switch e.Event {
	case eventNewNode:
		raft.mu.Lock()
		_, ok := raft.nodes[e.Addr]
		raft.mu.Unlock()
		if ok {
			return protocol.MakeErrReply("node exists")
		}
	}
	wg := wgPool.Get().(*sync.WaitGroup)
	defer wgPool.Put(wg)
	e.wg = wg
	raft.mu.Lock()
	raft.proposedIndex++
	raft.log = append(raft.log, e)
	raft.nodeIndexMap[raft.selfNodeID].receivedIndex = raft.proposedIndex
	e.Term = raft.term
	e.Index = raft.proposedIndex
	raft.mu.Unlock()
	e.wg.Add(1)
	e.wg.Wait() // wait for the raft group to reach a consensus
	return nil
}

func (raft *Raft) Join(seed string) protocol.ErrorReply {
	cluster := raft.cluster

	/* STEP1: get leader from seed */
	seedCli, err := cluster.clientFactory.GetPeerClient(seed)
	if err != nil {
		return protocol.MakeErrReply("connect with seed failed: " + err.Error())
	}
	defer cluster.clientFactory.ReturnPeerClient(seed, seedCli)
	ret := seedCli.Send(utils.ToCmdLine("raft", "get-leader"))
	if protocol.IsErrorReply(ret) {
		return ret.(protocol.ErrorReply)
	}
	leaderInfo, ok := ret.(*protocol.MultiBulkReply)
	if !ok || len(leaderInfo.Args) != 2 {
		return protocol.MakeErrReply("ERR get-leader returns wrong reply")
	}
	leaderAddr := string(leaderInfo.Args[1])

	/* STEP2: join raft group */
	leaderCli, err := cluster.clientFactory.GetPeerClient(leaderAddr)
	if err != nil {
		return protocol.MakeErrReply("connect with seed failed: " + err.Error())
	}
	defer cluster.clientFactory.ReturnPeerClient(leaderAddr, leaderCli)
	ret = leaderCli.Send(utils.ToCmdLine("raft", "join", cluster.addr))
	if protocol.IsErrorReply(ret) {
		return ret.(protocol.ErrorReply)
	}
	snapshot, ok := ret.(*protocol.MultiBulkReply)
	if !ok || len(snapshot.Args) < 4 {
		return protocol.MakeErrReply("ERR gcluster join returns wrong reply")
	}
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if errReply := raft.loadSnapshot(snapshot.Args); errReply != nil {
		return errReply
	}
	cluster.self = raft.selfNodeID
	raft.start(follower)
	return nil
}

func (raft *Raft) LoadConfigFile() protocol.ErrorReply {
	f, err := os.Open(raft.persistFile)
	if err == os.ErrNotExist {
		return errConfigFileNotExist
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.Errorf("close cloud config file error: %v", err)
		}
	}()
	scanner := bufio.NewScanner(f)
	var snapshot [][]byte
	for scanner.Scan() {
		line := append([]byte{}, scanner.Bytes()...) // copy the line...
		snapshot = append(snapshot, line)
	}
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if errReply := raft.loadSnapshot(snapshot); errReply != nil {
		return errReply
	}
	raft.cluster.self = raft.selfNodeID
	raft.start(raft.state)
	return nil
}
