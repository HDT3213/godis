package core

import (
	"strconv"

	dbimpl "github.com/hdt3213/godis/database"
)

// MakeTestCluster creates a cluster for test, which communications are done through local function calls.
func MakeTestCluster(ids []string) map[string]*Cluster {
	nodes := make(map[string]*Cluster)
	connections := NewInMemConnectionFactory()
	connections.nodes = nodes
	for _, id := range ids {
		db := dbimpl.NewStandaloneServer()
		cluster := &Cluster{
			db:              db,
			config:          &Config{},
			connections:     connections,
			rebalanceManger: newRebalanceManager(),
			slotsManager:    newSlotsManager(),
			transactions:    newTransactionManager(),
			id_:             id,
		}
		cluster.pickNodeImpl = func(slotID uint32) string {
			// skip raft for test
			index := int(slotID) % len(ids)
			return ids[index]
		}
		cluster.getSlotImpl = func(key string) uint32 {
			// backdoor for test
			i, err := strconv.Atoi(key)
			if err == nil && i < SlotCount {
				return uint32(i)
			}
			return defaultGetSlotImpl(cluster, key)
		}
		cluster.injectInsertCallback()
		cluster.injectDeleteCallback()
		nodes[id] = cluster
	}
	return nodes
}
