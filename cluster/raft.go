package cluster

import (
	"errors"
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/lock"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
	"hash/crc32"
	"math/rand"
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

// Slot represents a hash slot,  used in cluster internal messages
type Slot struct {
	// ID is uint between 0 and 16383
	ID uint32
	// NodeID is id of the hosting node
	// If the slot is migrating, NodeID is the id of the node importing this slot (target node)
	NodeID string
	// Flags stores more information of slot
	Flags uint32
}

func getSlot(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key)) % uint32(slotCount)
}

// Node represents a node and its slots, used in cluster internal messages
type Node struct {
	ID        string
	Addr      string
	Slots     []*Slot // ascending order by slot id
	Flags     uint32
	lastHeard time.Time
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

type Raft struct {
	cluster        *Cluster
	mu             sync.RWMutex
	selfNodeID     string
	slots          []*Slot
	leaderId       string
	nodes          map[string]*Node
	log            []*logEntry
	beginIndex     int
	state          raftState
	term           int
	votedFor       string
	voteCount      int
	committedIndex int // index of the last committed logEntry
	proposalIndex  int // index of the last proposed logEntry
	heartbeatChan  chan *heartbeat

	// for leader
	nodeIndexMap map[string]*nodeStatus
	nodeLock     *lock.Locks
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

// PickNode returns the node id hosting the given slot.
// If the slot is migrating, return the node which is importing the slot
func (raft *Raft) PickNode(slotID uint32) *Node {
	slot := raft.slots[int(slotID)]
	return raft.nodes[slot.NodeID]
}

func (raft *Raft) GetTopology() map[string]*Node {
	return raft.nodes
}

func (raft *Raft) getLogEntries(beg, end int) []*logEntry {
	if beg < raft.beginIndex || end > raft.beginIndex+len(raft.log) {
		return nil
	}
	i := beg - raft.beginIndex
	j := end - raft.beginIndex
	return raft.log[i:j]
}

func (raft *Raft) getLogEntriesFrom(beg int) []*logEntry {
	if beg < raft.beginIndex {
		return nil
	}
	i := beg - raft.beginIndex
	return raft.log[i:]
}

func (raft *Raft) getLogEntry(idx int) *logEntry {
	if idx < raft.beginIndex || idx >= raft.beginIndex+len(raft.log) {
		return nil
	}
	return raft.log[idx-raft.beginIndex]
}

func (raft *Raft) initLog(beginIndex int, entries []*logEntry) {
	raft.beginIndex = beginIndex
	raft.log = entries
}

// StartAsSeed starts cluster as seed node
func (raft *Raft) StartAsSeed(addr string) (string, error) {
	selfNodeID := config.Properties.AnnounceAddress()
	raft.slots = make([]*Slot, slotCount)
	raft.nodeLock = lock.Make(1024)
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
		Addr:  addr,
		Slots: raft.slots,
	}
	raft.nodes[selfNodeID].setState(leader)
	raft.start(leader)
	return selfNodeID, nil
}

func (raft *Raft) GetSlots() []*Slot {
	return raft.slots
}

// GetSelfNodeID returns node id of current node
func (raft *Raft) GetSelfNodeID() string {
	return raft.selfNodeID
}

func (raft *Raft) start(state raftState) {
	raft.state = state
	raft.heartbeatChan = make(chan *heartbeat, 1)
	//raft.nodeIndexMap = make(map[string]*nodeStatus)
	go func() {
		for {
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

const heartbeatTimeout = 3 * time.Second

func (raft *Raft) followerJob() {
	select {
	case hb := <-raft.heartbeatChan:
		raft.mu.Lock()
		nodeId := hb.sender
		raft.nodes[nodeId].lastHeard = time.Now()
		// todo: drop duplicate entry
		raft.log = append(raft.log, hb.entries...)
		raft.proposalIndex += len(hb.entries)
		raft.applyLogEntries(raft.getLogEntries(raft.committedIndex+1, hb.commitTo+1))
		raft.committedIndex = hb.commitTo
		raft.mu.Unlock()
	case <-time.After(heartbeatTimeout):
		// change to candidate
		logger.Info("raft leader timeout")
		timeoutMs := rand.Intn(500) + int(float64(300)/float64(raft.proposalIndex/10+1))
		logger.Info("sleep " + strconv.Itoa(timeoutMs) + "ms before change to candidate")
		time.Sleep(time.Duration(timeoutMs) * time.Millisecond)
		raft.mu.Lock()
		if raft.votedFor != "" {
			logger.Info("has voted, give up being a candidate")
			return
		}
		logger.Info("change to candidate")
		raft.state = candidate
		raft.mu.Unlock()
	}
}

func (raft *Raft) candidateJob() {
	raft.mu.Lock()
	raft.term++
	raft.votedFor = raft.selfNodeID
	raft.voteCount++
	currentTerm := raft.term
	req := &voteReq{
		nodeID:   raft.selfNodeID,
		logIndex: raft.proposalIndex,
		term:     raft.term,
	}
	raft.mu.Unlock()
	args := append([][]byte{
		[]byte("raft"),
		[]byte("request-vote"),
	}, req.marshal()...)
	conn := connection.NewFakeConn()
	wg := sync.WaitGroup{}
	for nodeID := range raft.nodes {
		nodeID := nodeID
		wg.Add(1)
		go func() {
			defer wg.Done()
			if nodeID == raft.selfNodeID {
				return
			}
			rawResp := raft.cluster.relay2(nodeID, conn, args)
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
				logger.InfoF(fmt.Sprintf("vote response from %s has newer term %d", nodeID, resp.term))
				raft.term = resp.term
				raft.state = follower
				raft.votedFor = ""
				raft.leaderId = resp.voteFor
				return
			}

			if resp.voteFor == raft.selfNodeID {
				logger.InfoF(fmt.Sprintf("get vote from %s", nodeID))
				raft.voteCount++
				if raft.voteCount >= len(raft.nodes)/2+1 {
					logger.Info("elected to be the leader")
					raft.state = leader
					raft.slots = make([]*Slot, slotCount)
					raft.nodeLock = lock.Make(1024)
					return
				}
			}
		}()
	}

	wg.Wait()
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if currentTerm != raft.term || raft.state != candidate {
		logger.Info("vote has finished, continue working in new state " + strconv.Itoa(int(raft.state)))
		return
	}
	logger.Info("failed to be elected, back to follower")
	raft.state = follower
	raft.term = currentTerm // failed to be elected neither found newer term
}

// askNodeIndex ask offset of each node and init nodeIndexMap as new leader
// invoker provide lock
func (raft *Raft) askNodeIndex() map[string]*nodeStatus {
	nodeIndexMap := make(map[string]*nodeStatus)
	// todo: copy nodes to avoid concurrent read-write
	for _, node := range raft.nodes {
		c := connection.NewFakeConn()
		reply := raft.cluster.relay2(node.Addr, c, utils.ToCmdLine("raft", "get-offset"))
		if protocol.IsErrorReply(reply) {
			// todo: handle follower offline
			continue
		}
		proposalOffset := int(reply.(*protocol.IntReply).Code)
		nodeIndexMap[node.ID] = &nodeStatus{
			receivedIndex: proposalOffset,
		}
	}
	return nodeIndexMap
}

func (raft *Raft) leaderJob() {
	raft.mu.Lock()
	if raft.nodeIndexMap == nil {
		// todo: reduce lock time
		raft.nodeIndexMap = raft.askNodeIndex()
	}
	raft.nodeIndexMap[raft.selfNodeID].receivedIndex = raft.proposalIndex
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
	// todo: 排除掉未完成同步的新节点?
	// new node (received index is 0) may cause commitTo less than raft.committedIndex
	if commitTo > raft.committedIndex {
		toCommit := raft.getLogEntries(raft.committedIndex, commitTo)
		raft.applyLogEntries(toCommit)
		raft.committedIndex = commitTo
		for _, entry := range toCommit {
			if entry.wg != nil {
				entry.wg.Done()
			}
		}
	}
	// save receivedIndex in local variable in case changed by other goroutines
	proposalIndex := raft.proposalIndex
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
			if status.receivedIndex < raft.beginIndex {
				// some entries are missed due to change of leader, send full snapshot
				cmdLine = utils.ToCmdLine(
					"raft",
					"load-snapshot",
					raft.selfNodeID,
				)
				cmdLine = append(cmdLine, snapshot...)
			} else {
				// leader has all needed entries, send normal heartbeat
				cmdLine = utils.ToCmdLine(
					"raft",
					"heartbeat",
					raft.selfNodeID,
					strconv.Itoa(raft.term),
					strconv.Itoa(commitTo),
				)
				// append new entries to heartbeat payload
				if proposalIndex > status.receivedIndex {
					prevLogTerm := raft.getLogEntry(status.receivedIndex).Term
					entries := raft.getLogEntriesFrom(status.receivedIndex + 1)
					cmdLine = append(cmdLine,
						[]byte(strconv.Itoa(prevLogTerm)),
						[]byte(strconv.Itoa(status.receivedIndex)),
					)
					for _, entry := range entries {
						cmdLine = append(cmdLine, entry.marshal())
					}
				}
			}

			conn := connection.NewFakeConn()
			resp := raft.cluster.relay2(node.ID, conn, cmdLine)
			if err, ok := resp.(protocol.ErrorReply); ok {
				logger.Errorf("heartbeat to %s failed: %v", node.ID, err)
				return
			}
			if arrReply, ok := resp.(*protocol.MultiBulkReply); ok {
				term, _ := strconv.Atoi(string(arrReply.Args[0]))
				recvedIndex, _ := strconv.Atoi(string(arrReply.Args[1]))
				if term > raft.term {
					// todo: rejoin as follower
					return
				}
				raft.mu.Lock()
				raft.nodeIndexMap[node.ID].receivedIndex = recvedIndex
				raft.mu.Unlock()
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
	nodeID   string
	logIndex int
	term     int
}

func (req *voteReq) marshal() [][]byte {
	indexBin := []byte(strconv.Itoa(int(req.logIndex)))
	termBin := []byte(strconv.Itoa(req.term))
	return [][]byte{
		[]byte(req.nodeID),
		indexBin,
		termBin,
	}
}

func (req *voteReq) unmarshal(bin [][]byte) error {
	req.nodeID = string(bin[0])
	index, err := strconv.Atoi(string(bin[1]))
	if err != nil {
		return fmt.Errorf("illegal index %s", string(bin[1]))
	}
	req.logIndex = int(index)
	term, err := strconv.Atoi(string(bin[2]))
	if err != nil {
		return fmt.Errorf("illegal term %s", string(bin[2]))
	}
	req.term = term
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
	if len(args) != 3 {
		return protocol.MakeArgNumErrReply("raft request-vote")
	}
	req := &voteReq{}
	err := req.unmarshal(args)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	cluster.topology.mu.Lock()
	defer cluster.topology.mu.Unlock()
	logger.Info("recv request vote from " + req.nodeID + ", term: " + strconv.Itoa(req.term))
	resp := &voteResp{}
	if req.term < cluster.topology.term {
		resp.term = cluster.topology.term
		resp.voteFor = cluster.topology.leaderId // tell candidate the new leader
		logger.Info("deny request vote from " + req.nodeID + " for earlier term")
		return protocol.MakeMultiBulkReply(resp.marshal())
	}
	if req.logIndex < cluster.topology.proposalIndex {
		resp.term = cluster.topology.term
		resp.voteFor = cluster.topology.votedFor
		logger.Info("deny request vote from " + req.nodeID + " for  log progress")
		logger.Info("request vote proposal index " + strconv.Itoa(req.logIndex) + " self index " + strconv.Itoa(cluster.topology.proposalIndex))
		return protocol.MakeMultiBulkReply(resp.marshal())
	}
	if cluster.topology.votedFor != "" && cluster.topology.votedFor != cluster.topology.selfNodeID {
		resp.term = cluster.topology.term
		resp.voteFor = cluster.topology.votedFor
		logger.Info("deny request vote from " + req.nodeID + " for voted")
		return protocol.MakeMultiBulkReply(resp.marshal())
	}
	if cluster.topology.votedFor == cluster.topology.selfNodeID &&
		cluster.topology.voteCount == 1 {
		// cancel vote for self to avoid live lock
		cluster.topology.votedFor = ""
		cluster.topology.voteCount = 0
	}
	logger.Info("accept request vote from " + req.nodeID)
	cluster.topology.votedFor = req.nodeID
	cluster.topology.term = req.term
	resp.voteFor = req.nodeID
	resp.term = cluster.topology.term
	return protocol.MakeMultiBulkReply(resp.marshal())
}

// execRaftHeartbeat receives heartbeat from leader
// command line: raft heartbeat nodeID term commitTo prevTerm prevIndex [log entry]
// returns term and received index
func execRaftHeartbeat(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 6 && len(args) != 3 {
		return protocol.MakeArgNumErrReply("raft heartbeat")
	}
	sender := string(args[0])
	term, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("illegal term: " + string(args[1]))
	}
	commitTo, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply("illegal commitTo: " + string(args[1]))
	}
	if term < cluster.topology.term {
		return protocol.MakeIntReply(int64(cluster.topology.term))
	} else if term > cluster.topology.term {
		logger.Info("accept new leader " + sender)
		cluster.topology.mu.Lock()
		cluster.topology.term = term
		cluster.topology.votedFor = ""
		cluster.topology.leaderId = sender
		cluster.topology.mu.Unlock()
	}
	// todo: validate prevTerm prevIndex
	var entries []*logEntry
	if len(args) > 5 {
		for _, bin := range args[5:] {
			entry := &logEntry{}
			err = entry.unmarshal(bin)
			if err != nil {
				return protocol.MakeErrReply(err.Error())
			}
			entries = append(entries, entry)
		}
	}
	cluster.topology.heartbeatChan <- &heartbeat{
		sender:   sender,
		term:     term,
		entries:  entries,
		commitTo: commitTo,
	}
	return protocol.MakeMultiBulkReply(utils.ToCmdLine(
		strconv.Itoa(term),
		strconv.Itoa(cluster.topology.proposalIndex+len(entries)), // new received index
	))
}

// execRaftLoadSnapshot load snapshot from leader
// command line: raft load-snapshot leaderId snapshot(see raft.makeSnapshot)
func execRaftLoadSnapshot(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	// leaderId snapshot
	if len(args) < 5 {
		return protocol.MakeArgNumErrReply("raft load snapshot")
	}
	cluster.topology.mu.Lock()
	defer cluster.topology.mu.Unlock()
	if errReply := cluster.topology.loadSnapshot(args[1:]); errReply != nil {
		return errReply
	}
	sender := string(args[0])
	cluster.topology.heartbeatChan <- &heartbeat{
		sender:   sender,
		term:     cluster.topology.term,
		entries:  nil,
		commitTo: cluster.topology.committedIndex,
	}
	return protocol.MakeMultiBulkReply(utils.ToCmdLine(
		strconv.Itoa(cluster.topology.term),
		strconv.Itoa(cluster.topology.proposalIndex),
	))
}

var wgPool = sync.Pool{
	New: func() interface{} {
		return &sync.WaitGroup{}
	},
}

// execRaftGetLeader returns leader id and address
func execRaftGetLeader(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	cluster.topology.mu.RLock()
	leaderNode := cluster.topology.nodes[cluster.topology.leaderId]
	cluster.topology.mu.RUnlock()
	return protocol.MakeMultiBulkReply(utils.ToCmdLine(
		leaderNode.ID,
		leaderNode.Addr,
	))
}

// execRaftGetOffset returns log offset of current leader
func execRaftGetOffset(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	cluster.topology.mu.RLock()
	proposalIndex := cluster.topology.proposalIndex
	//committedIndex := cluster.topology.committedIndex
	cluster.topology.mu.RUnlock()
	return protocol.MakeIntReply(int64(proposalIndex))
}
