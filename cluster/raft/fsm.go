package raft

import (
	"encoding/json"
	"io"
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
	mu           sync.RWMutex
	Node2Slot    map[string][]uint32       // nodeID -> slotIDs, slotIDs is in ascending order and distinct
	Slot2Node    map[uint32]string         // slotID -> nodeID
	Migratings   map[string]*MigratingTask // taskId -> task
	MasterSlaves map[string]*MasterSlave   // masterId -> MasterSlave
	SlaveMasters map[string]string         // slaveId -> masterId
	Failovers    map[string]*FailoverTask  // taskId -> task
	changed      func(*FSM)                // called while fsm changed, within readlock
}

type MasterSlave struct {
	MasterId string
	Slaves   []string
}

// MigratingTask is a running migrating task
// It is immutable
type MigratingTask struct {
	ID         string
	SrcNode    string
	TargetNode string

	// Slots stores slots to migrate in this event
	Slots []uint32
}

// InitTask assigns all slots to seed node while starting a new cluster
// It is designed to init the FSM for a new cluster
type InitTask struct {
	Leader    string
	SlotCount int
}

// FailoverTask represents a failover or joinery/quitting of slaves
type FailoverTask struct {
	ID          string
	OldMasterId string
	NewMasterId string
}

type JoinTask struct {
	NodeId string
	Master string
}

// implements FSM.Apply after you created a new raft event
const (
	EventStartMigrate = iota + 1
	EventFinishMigrate
	EventSeedStart
	EventStartFailover
	EventFinishFailover
	EventJoin
)

// LogEntry is an entry in raft log, stores a change of cluster
type LogEntry struct {
	Event         int
	MigratingTask *MigratingTask `json:"MigratingTask,omitempty"`
	InitTask      *InitTask
	FailoverTask  *FailoverTask
	JoinTask      *JoinTask
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
		fsm.addNode(entry.InitTask.Leader, "")
	} else if entry.Event == EventStartFailover {
		task := entry.FailoverTask
		fsm.Failovers[task.ID] = task
	} else if entry.Event == EventFinishFailover {
		task := entry.FailoverTask
		// change route
		fsm.failover(task.OldMasterId, task.NewMasterId)
		slots := fsm.Node2Slot[task.OldMasterId]
		fsm.addSlots(task.NewMasterId, slots)
		fsm.removeSlots(task.OldMasterId, slots)
		delete(fsm.Failovers, task.ID)
	} else if entry.Event == EventJoin {
		task := entry.JoinTask
		fsm.addNode(task.NodeId, task.Master)
	}
	if fsm.changed != nil {
		fsm.changed(fsm)
	}
	return nil
}

// FSMSnapshot stores necessary data to restore FSM
type FSMSnapshot struct {
	Slot2Node    map[uint32]string // slotID -> nodeID
	Migratings   map[string]*MigratingTask
	MasterSlaves map[string]*MasterSlave
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
	masterSlaves := make(map[string]*MasterSlave)
	for k, v := range fsm.MasterSlaves {
		masterSlaves[k] = v
	}
	return &FSMSnapshot{
		Slot2Node:    slot2Node,
		Migratings:   migratings,
		MasterSlaves: masterSlaves,
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
	fsm.MasterSlaves = snapshot.MasterSlaves
	fsm.Node2Slot = make(map[string][]uint32)
	for slot, node := range snapshot.Slot2Node {
		fsm.Node2Slot[node] = append(fsm.Node2Slot[node], slot)
	}
	for master, slaves := range snapshot.MasterSlaves {
		for _, slave := range slaves.Slaves {
			fsm.SlaveMasters[slave] = master
		}
	}
	if fsm.changed != nil {
		fsm.changed(fsm)
	}
	return nil
}
