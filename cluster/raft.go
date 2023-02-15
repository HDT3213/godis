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

type Raft struct {
	cluster       *Cluster
	mu            sync.RWMutex
	selfNodeID    string
	slots         []*Slot
	leaderId      string
	nodes         map[string]*Node
	log           []*logEntry
	beginIndex    int
	state         raftState
	term          int
	votedFor      string
	voteCount     int
	commitIndex   int // index of the last committed logEntry
	proposalIndex int // index of the last proposed logEntry
	heartbeatChan chan *heartbeat

	// for leader
	nodeIndexMap map[string]*nodeIndex
	nodeLock     *lock.Locks
}

type heartbeat struct {
	sender   string
	term     int
	entries  []*logEntry
	commitTo int
}

type nodeIndex struct {
	recvedIndex int
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

// Load loads topology, used in join cluster
func (raft *Raft) Load(selfNodeId string, leaderId string, term int, commitIndex int, nodes map[string]*Node) {
	// make sure raft.slots and node.Slots is the same object
	raft.mu.Lock()
	defer raft.mu.Unlock()
	raft.selfNodeID = selfNodeId
	raft.leaderId = leaderId
	raft.term = term
	raft.commitIndex = commitIndex
	raft.proposalIndex = commitIndex
	raft.initLog(commitIndex, nil)
	raft.slots = make([]*Slot, slotCount)
	for _, node := range nodes {
		for _, slot := range node.Slots {
			raft.slots[int(slot.ID)] = slot
		}
		if node.getState() == leader {
			raft.leaderId = node.ID
		}
	}
	raft.nodes = nodes
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
	raft.nodeIndexMap = make(map[string]*nodeIndex)
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

const heartbeatTimeout = time.Second

func (raft *Raft) followerJob() {
	select {
	case hb := <-raft.heartbeatChan:
		raft.mu.Lock()
		nodeId := hb.sender
		raft.nodes[nodeId].lastHeard = time.Now()
		// todo: drop duplicate entry
		raft.log = append(raft.log, hb.entries...)
		raft.proposalIndex += len(hb.entries)
		raft.applyLogEntries(raft.getLogEntries(raft.commitIndex+1, hb.commitTo+1))
		raft.commitIndex = hb.commitTo
		raft.mu.Unlock()
	case <-time.After(heartbeatTimeout):
		// change to candidate
		logger.Info("raft leader timeout")
		timeoutMs := rand.Intn(200) + int(float64(300)/float64(raft.proposalIndex/10+1))
		time.Sleep(time.Duration(timeoutMs) * time.Millisecond)
		logger.Info("change to candidate")
		raft.mu.Lock()
		raft.state = candidate
		raft.mu.Unlock()
	}
}

func (raft *Raft) candidateJob() {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	raft.term++
	raft.votedFor = raft.selfNodeID
	raft.voteCount++

	req := &voteReq{
		nodeID:   raft.selfNodeID,
		logIndex: raft.proposalIndex,
		term:     raft.term,
	}
	args := append([][]byte{
		[]byte("raft"),
		[]byte("request-vote"),
	}, req.marshal()...)
	conn := connection.NewFakeConn()
	for nodeID := range raft.nodes {
		if nodeID == raft.selfNodeID {
			continue
		}
		rawResp := raft.cluster.relay(nodeID, conn, args)
		if err, ok := rawResp.(protocol.ErrorReply); ok {
			logger.Info(fmt.Sprintf("cannot get vote response from %s, %v", nodeID, err))
			continue
		}
		respBody, ok := rawResp.(*protocol.MultiBulkReply)
		if !ok {
			logger.Info(fmt.Sprintf("cannot get vote response from %s, not a multi bulk reply", nodeID))
			continue
		}
		resp := &voteResp{}
		err := resp.unmarshal(respBody.Args)
		if !ok {
			logger.Info(fmt.Sprintf("cannot get vote response from %s, %v", nodeID, err))
			continue
		}

		if resp.term > raft.term {
			raft.mu.Lock()
			raft.term = resp.term
			raft.state = follower
			raft.votedFor = ""
			raft.mu.Unlock()
		}

		if resp.voteFor == raft.selfNodeID {
			raft.mu.Lock()
			raft.voteCount++
			if raft.voteCount >= len(raft.nodes)/2+1 {
				raft.state = leader
				return
			}
			raft.mu.Unlock()
		}
	}
}

func (raft *Raft) leaderJob() {
	raft.mu.Lock()
	if raft.nodeIndexMap[raft.selfNodeID] == nil {
		raft.nodeIndexMap[raft.selfNodeID] = &nodeIndex{}
	}
	raft.nodeIndexMap[raft.selfNodeID].recvedIndex = raft.proposalIndex
	var recvedIndices []int
	for _, status := range raft.nodeIndexMap {
		recvedIndices = append(recvedIndices, status.recvedIndex)
	}
	sort.Slice(recvedIndices, func(i, j int) bool {
		return recvedIndices[i] > recvedIndices[j]
	})
	// more than half of the nodes received entries, can be committed
	commitTo := 0
	if len(recvedIndices) > 0 {
		commitTo = recvedIndices[len(recvedIndices)/2]
	}
	// new node (received index is 0) may cause commitTo less than commitTo
	if commitTo > raft.commitIndex {
		toCommit := raft.getLogEntries(raft.commitIndex, commitTo)
		raft.applyLogEntries(toCommit)
		raft.commitIndex = commitTo
		for _, entry := range toCommit {
			if entry.wg != nil {
				entry.wg.Done()
			}
		}
	}
	raft.mu.Unlock()
	// save proposalIndex in local variable in case changed by other goroutines
	proposalIndex := raft.proposalIndex
	for _, node := range raft.nodes {
		if node.ID == raft.selfNodeID {
			continue
		}
		node := node
		go func() {
			raft.nodeLock.Lock(node.ID)
			defer raft.nodeLock.UnLock(node.ID)
			cmdLine := utils.ToCmdLine(
				"raft",
				"heartbeat",
				raft.selfNodeID,
				strconv.Itoa(raft.term),
				strconv.Itoa(commitTo),
			)
			if raft.nodeIndexMap[node.ID] == nil {
				raft.nodeIndexMap[node.ID] = &nodeIndex{}
			}
			nodeStatus := raft.nodeIndexMap[node.ID]
			prevLogIndex := nodeStatus.recvedIndex
			if proposalIndex > prevLogIndex {
				prevLogTerm := raft.log[prevLogIndex].Term
				cmdLine = append(cmdLine,
					[]byte(strconv.Itoa(prevLogTerm)),
					[]byte(strconv.Itoa(prevLogIndex)),
				)
				for _, entry := range raft.log[prevLogIndex:] {
					cmdLine = append(cmdLine, entry.marshal())
				}
			}

			conn := connection.NewFakeConn()
			resp := raft.cluster.relay(node.ID, conn, cmdLine)
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
				raft.nodeIndexMap[node.ID].recvedIndex = recvedIndex
				raft.mu.Unlock()
			}
		}()
	}
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
	if len(bin) == 2 {
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
	resp := &voteResp{}
	if req.term < cluster.topology.term ||
		req.logIndex < cluster.topology.proposalIndex ||
		cluster.topology.votedFor != "" {
		resp.term = cluster.topology.term
		return protocol.MakeMultiBulkReply(resp.marshal())
	}
	resp.voteFor = req.nodeID
	cluster.topology.votedFor = req.nodeID
	cluster.topology.term = req.term
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
		strconv.Itoa(cluster.topology.proposalIndex+len(entries)),
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
