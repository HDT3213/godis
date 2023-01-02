package cluster

import "github.com/hdt3213/godis/datastruct/set"

func (cluster *Cluster) setImportedKey(key string) {
	slotId := getSlot(key)
	slot := cluster.slots[slotId]
	slot.importedKeys.Add(key)
}

// setSlot init a slot when start as seed or import slot from other node
func (cluster *Cluster) setSlot(slotId uint32, state uint32) {
	cluster.slots[slotId] = &hostSlot{
		importedKeys: set.Make(),
		keys:         set.Make(),
		state:        state,
	}
}
