package raft

import (
	"encoding/json"
	"io"
	"sort"
	"sync"

	"github.com/hashicorp/raft"
)

// FSM implements raft.FSM
// It stores node-slots mapping to providing routing service
//
// A request pointing to a migrating slot will be routed to source node.
// After the command being executed, the updates will be relayed to the target node
//
// If the target node crashes during migrating, the migration will be canceled.
// All related commands will be routed to the source node
type FSM struct {
	mu         sync.RWMutex
	Node2Slot  map[string][]uint32       // nodeID -> slotIDs, slotIDs is in ascending order and distinct
	Slot2Node  map[uint32]string         // slotID -> nodeID
	Migratings map[string]*MigratingTask // taskId -> task
}

// MigratingTask
// It is immutable
type MigratingTask struct {
	ID         string
	SrcNode    string
	TargetNode string

	// Slots stores slots to migrate in this event
	Slots []uint32
}

// InitTask
type InitTask struct {
	Leader    string
	SlotCount int
}

// implements FSM.Apply after you created a new raft event
const (
	EventStartMigrate = iota + 1
	EventFinishMigrate
	EventSeedStart
)

// LogEntry is an entry in raft log, stores a change of cluster
type LogEntry struct {
	Event         int
	MigratingTask *MigratingTask `json:"MigratingTask,omitempty"`
	InitTask      *InitTask
}

// Apply is called once a log entry is committed by a majority of the cluster.
func (fsm *FSM) Apply(log *raft.Log) interface{} {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	entry := &LogEntry{}
	err := json.Unmarshal(log.Data, entry)
	if err != nil {
		panic(err)
	}

	/// event handler
	if entry.Event == EventStartMigrate {
		task := entry.MigratingTask
		fsm.Migratings[task.ID] = task
	} else if entry.Event == EventFinishMigrate {
		task := entry.MigratingTask
		delete(fsm.Migratings, task.ID)
		fsm.addSlots(task.TargetNode, task.Slots)
		fsm.removeSlots(task.SrcNode, task.Slots)
	} else if entry.Event == EventSeedStart {
		slots := make([]uint32, int(entry.InitTask.SlotCount))
		for i := 0; i < entry.InitTask.SlotCount; i++ {
			fsm.Slot2Node[uint32(i)] = entry.InitTask.Leader
			slots[i] = uint32(i)
		}
		fsm.Node2Slot[entry.InitTask.Leader] = slots
	}

	return nil
}

func (fsm *FSM) addSlots(nodeID string, slots []uint32) {
	for _, slotId := range slots {
		/// update node2Slot
		index := sort.Search(len(fsm.Node2Slot[nodeID]), func(i int) bool {
			return fsm.Node2Slot[nodeID][i] >= slotId
		})
		if !(index < len(fsm.Node2Slot[nodeID]) && fsm.Node2Slot[nodeID][index] == slotId) {
			// not found in node's slots, insert
			fsm.Node2Slot[nodeID] = append(fsm.Node2Slot[nodeID][:index],
				append([]uint32{slotId}, fsm.Node2Slot[nodeID][index:]...)...)
		}
		/// update slot2Node
		fsm.Slot2Node[slotId] = nodeID
	}
}

func (fsm *FSM) removeSlots(nodeID string, slots []uint32) {
	for _, slotId := range slots {
		/// update node2slot
		index := sort.Search(len(fsm.Node2Slot[nodeID]), func(i int) bool { return fsm.Node2Slot[nodeID][i] >= slotId })
		// found slot remove
		for index < len(fsm.Node2Slot[nodeID]) && fsm.Node2Slot[nodeID][index] == slotId {
			fsm.Node2Slot[nodeID] = append(fsm.Node2Slot[nodeID][:index], fsm.Node2Slot[nodeID][index+1:]...)
		}
		// update slot2node
		if fsm.Slot2Node[slotId] == nodeID {
			delete(fsm.Slot2Node, slotId)
		}
	}
}

func (fsm *FSM) GetMigratingTask(taskId string) *MigratingTask {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return fsm.Migratings[taskId]
}

// FSMSnapshot stores necessary data to restore FSM
type FSMSnapshot struct {
	Slot2Node  map[uint32]string // slotID -> nodeID
	Migratings map[string]*MigratingTask
}

func (snapshot *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		data, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		_, err = sink.Write(data)
		if err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
	}
	return err
}

func (snapshot *FSMSnapshot) Release() {}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	slot2Node := make(map[uint32]string)
	for k, v := range fsm.Slot2Node {
		slot2Node[k] = v
	}
	migratings := make(map[string]*MigratingTask)
	for k, v := range fsm.Migratings {
		migratings[k] = v
	}
	return &FSMSnapshot{
		Slot2Node:  slot2Node,
		Migratings: migratings,
	}, nil
}

func (fsm *FSM) Restore(src io.ReadCloser) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	data, err := io.ReadAll(src)
	if err != nil {
		return err
	}
	snapshot := &FSMSnapshot{}
	err = json.Unmarshal(data, snapshot)
	if err != nil {
		return err
	}
	fsm.Slot2Node = snapshot.Slot2Node
	fsm.Migratings = snapshot.Migratings
	fsm.Node2Slot = make(map[string][]uint32)
	for slot, node := range snapshot.Slot2Node {
		fsm.Node2Slot[node] = append(fsm.Node2Slot[node], slot)
	}
	return nil
}

// PickNode returns node hosting slot, ignore migrating
func (fsm *FSM) PickNode(slot uint32) string {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return fsm.Slot2Node[slot]
}

// WithReadLock allow invoker do something complicated with read lock
func (fsm *FSM) WithReadLock(fn func(fsm *FSM)) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	fn(fsm)
}
