package cluster

import "github.com/hdt3213/godis/datastruct/set"

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
	slot.newNodeID = cluster.self
}
