package core

import (
	"sync/atomic"
	"time"

	"github.com/hdt3213/godis/cluster/raft"
)

func (cluster *Cluster) clusterCron() {
	if cluster.config.noCron {
		return
	}
	ticker := time.NewTicker(time.Second)
	var running int32
	for {
		select {
		case <-ticker.C:
			if cluster.raftNode.State() == raft.Leader {
				if atomic.CompareAndSwapInt32(&running, 0, 1) {
					// Disable parallelism
					go func() {
						cluster.doFailoverCheck()
						cluster.doRebalance()
						atomic.StoreInt32(&running, 0)
					}()
				}
			} else {
				cluster.sendHearbeat()
			}
		case <-cluster.closeChan:
			ticker.Stop()
			return
		}
	}
}
