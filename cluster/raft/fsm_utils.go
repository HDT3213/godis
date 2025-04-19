package raft

import (
	"errors"
	"sort"
)

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

func (fsm *FSM) GetMigratingTask(taskId string) *MigratingTask {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return fsm.Migratings[taskId]
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

func (fsm *FSM) failover(oldMasterId, newMasterId string) {
	oldSlaves := fsm.MasterSlaves[oldMasterId].Slaves
	newSlaves := make([]string, 0, len(oldSlaves))
	// change other slaves
	for _, slave := range oldSlaves {
		if slave != newMasterId {
			fsm.SlaveMasters[slave] = newMasterId
			newSlaves = append(newSlaves, slave)
		}
	}
	// change old master
	delete(fsm.MasterSlaves, oldMasterId)
	fsm.SlaveMasters[oldMasterId] = newMasterId
	newSlaves = append(newSlaves, oldMasterId)

	// change new master
	delete(fsm.SlaveMasters, newMasterId)
	fsm.MasterSlaves[newMasterId] = &MasterSlave{
		MasterId: newMasterId,
		Slaves:   newSlaves,
	}
}

// getMaster returns "" if id points to a master node
func (fsm *FSM) getMaster(id string) string {
	master := ""
	fsm.WithReadLock(func(fsm *FSM) {
		master = fsm.SlaveMasters[id]
	})
	return master
}

func (fsm *FSM) addNode(id, masterId string) error {
	if masterId == "" {
		fsm.MasterSlaves[id] = &MasterSlave{
			MasterId: id,
		}
	} else {
		master := fsm.MasterSlaves[masterId]
		if master == nil {
			return errors.New("master not found")
		}
		exists := false
		for _, slave := range master.Slaves {
			if slave == id {
				exists = true
				break
			}
		}
		if !exists {
			master.Slaves = append(master.Slaves, id)
		}
		fsm.SlaveMasters[id] = masterId
	}
	return nil
}