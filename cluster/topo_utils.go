package cluster

import (
	"github.com/hdt3213/godis/datastruct/set"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"sort"
)

func (cluster *Cluster) isImportedKey(key string) bool {
	slotId := getSlot(key)
	cluster.slotMu.RLock()
	slot := cluster.slots[slotId]
	cluster.slotMu.RUnlock()
	return slot.importedKeys.Has(key)
}

func (cluster *Cluster) setImportedKey(key string) {
	slotId := getSlot(key)
	cluster.slotMu.Lock()
	slot := cluster.slots[slotId]
	cluster.slotMu.Unlock()
	slot.importedKeys.Add(key)
}

// initSlot init a slot when start as seed or import slot from other node
func (cluster *Cluster) initSlot(slotId uint32, state uint32) {
	cluster.slotMu.Lock()
	defer cluster.slotMu.Unlock()
	cluster.slots[slotId] = &hostSlot{
		importedKeys: set.Make(),
		keys:         set.Make(),
		state:        state,
	}
}

func (cluster *Cluster) getHostSlot(slotId uint32) *hostSlot {
	cluster.slotMu.RLock()
	defer cluster.slotMu.RUnlock()
	return cluster.slots[slotId]
}

func (cluster *Cluster) finishSlotImport(slotID uint32) {
	cluster.slotMu.Lock()
	defer cluster.slotMu.Unlock()
	slot := cluster.slots[slotID]
	slot.state = slotStateHost
	slot.importedKeys = nil
	slot.oldNodeID = ""
}

func (cluster *Cluster) setLocalSlotImporting(slotID uint32, oldNodeID string) {
	cluster.slotMu.Lock()
	defer cluster.slotMu.Unlock()
	slot := cluster.slots[slotID]
	slot.state = slotStateImporting
	slot.oldNodeID = oldNodeID
}

func (cluster *Cluster) setSlotMovingOut(slotID uint32, newNodeID string) {
	cluster.slotMu.Lock()
	defer cluster.slotMu.Unlock()
	slot := cluster.slots[slotID]
	slot.state = slotStateMovingOut
	slot.newNodeID = newNodeID
}

// cleanDroppedSlot deletes keys when slot has moved out or failed to import
func (cluster *Cluster) cleanDroppedSlot(slotID uint32) {
	cluster.slotMu.RLock()
	keys := cluster.slots[slotID].importedKeys
	cluster.slotMu.RUnlock()
	c := connection.NewFakeConn()
	go func() {
		keys.ForEach(func(key string) bool {
			cluster.db.Exec(c, utils.ToCmdLine("DEL", key))
			return true
		})
	}()
}

// findSlotsForNewNode try to find slots for new node, but do not actually migrate
func (cluster *Cluster) findSlotsForNewNode() []*Slot {
	nodes := cluster.topology.GetNodes() // including the new node
	avgSlot := slotCount / len(nodes)
	sort.Slice(nodes, func(i, j int) bool {
		return len(nodes[i].Slots) > len(nodes[j].Slots)
	})
	result := make([]*Slot, 0, avgSlot)
	// there are always some nodes has more slots than avgSlot
	for _, node := range nodes {
		if len(node.Slots) <= avgSlot {
			// nodes are in decreasing order by len(node.Slots)
			// if len(node.Slots) < avgSlot, then all following nodes has fewer slots than avgSlot
			break
		}
		n := 2*avgSlot - len(result)
		if n < len(node.Slots) {
			// n - len(result) - avgSlot = avgSlot - len(result)
			// now len(result) == avgSlot
			result = append(result, node.Slots[avgSlot:n]...)
		} else {
			result = append(result, node.Slots[avgSlot:]...)
		}
		if len(result) >= avgSlot {
			break
		}
	}
	return result
}
