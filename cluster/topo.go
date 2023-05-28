package cluster

import (
	"fmt"
	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
	"time"
)

func (cluster *Cluster) startAsSeed(listenAddr string) protocol.ErrorReply {
	err := cluster.topology.StartAsSeed(listenAddr)
	if err != nil {
		return err
	}
	for i := 0; i < slotCount; i++ {
		cluster.initSlot(uint32(i), slotStateHost)
	}
	return nil
}

// Join send `gcluster join` to node in cluster to join
func (cluster *Cluster) Join(seed string) protocol.ErrorReply {
	err := cluster.topology.Join(seed)
	if err != nil {
		return nil
	}
	/* STEP3: asynchronous migrating slots */
	go func() {
		time.Sleep(time.Second) // let the cluster started
		cluster.reBalance()
	}()
	return nil
}

var errConfigFileNotExist = protocol.MakeErrReply("cluster config file not exist")

// LoadConfig try to load cluster-config-file and re-join the cluster
func (cluster *Cluster) LoadConfig() protocol.ErrorReply {
	err := cluster.topology.LoadConfigFile()
	if err != nil {
		return err
	}
	selfNodeId := cluster.topology.GetSelfNodeID()
	selfNode := cluster.topology.GetNode(selfNodeId)
	if selfNode == nil {
		return protocol.MakeErrReply("ERR self node info not found")
	}
	for _, slot := range selfNode.Slots {
		cluster.initSlot(slot.ID, slotStateHost)
	}
	return nil
}

func (cluster *Cluster) reBalance() {
	slots := cluster.findSlotsForNewNode()
	var slotIds []uint32
	for _, slot := range slots {
		slotIds = append(slotIds, slot.ID)
	}
	err := cluster.topology.SetSlot(slotIds, cluster.self)
	if err != nil {
		logger.Errorf("set slot route failed: %v", err)
		return
	}
	// serial migrations to avoid overloading the cluster
	slotChan := make(chan *Slot, len(slots))
	for _, slot := range slots {
		slotChan <- slot
	}
	close(slotChan)
	for i := 0; i < 4; i++ {
		go func() {
			for slot := range slotChan {
				logger.Info("start import slot ", slot.ID)
				err := cluster.importSlot(slot)
				if err != nil {
					logger.Error(fmt.Sprintf("import slot %d error: %v", slot.ID, err))
					// delete all imported keys in slot
					cluster.cleanDroppedSlot(slot.ID)
					return
				}
				logger.Info("finish import slot", slot.ID)
			}
		}()
	}
}

func (cluster *Cluster) importSlot(slot *Slot) error {
	/* change slot host */
	node := cluster.pickNode(slot.ID)
	cluster.initSlot(slot.ID, slotStateImporting) // prepare host slot before send `set slot`
	cluster.setLocalSlotImporting(slot.ID, cluster.self)
	peerCli, err := cluster.clientFactory.GetPeerClient(node.Addr)
	if err != nil {
		return err
	}
	defer cluster.clientFactory.ReturnPeerClient(node.Addr, peerCli)
	ret := peerCli.Send(utils.ToCmdLine(
		"gcluster", "set-slot", strconv.Itoa(int(slot.ID)), cluster.self))
	if !protocol.IsOKReply(ret) {
		return fmt.Errorf("set slot %d error: %v", slot.ID, ret)
	}
	logger.Info(fmt.Sprintf("set slot %d to current node, start migrate", slot.ID))

	/* get migrate stream */
	migrateCmdLine := utils.ToCmdLine(
		"gcluster", "migrate", strconv.Itoa(int(slot.ID)))
	migrateStream, err := cluster.clientFactory.NewStream(node.Addr, migrateCmdLine)
	if err != nil {
		return err
	}
	defer migrateStream.Close()

	fakeConn := connection.NewFakeConn()
slotLoop:
	for proto := range migrateStream.Stream() {
		if proto.Err != nil {
			return fmt.Errorf("set slot %d error: %v", slot.ID, err)
		}
		switch reply := proto.Data.(type) {
		case *protocol.MultiBulkReply:
			// todo: handle exec error
			keys, _ := database.GetRelatedKeys(reply.Args)
			// assert len(keys) == 1
			key := keys[0]
			// key may be imported by Cluster.ensureKey or by former failed migrating try
			if !cluster.isImportedKey(key) {
				cluster.setImportedKey(key)
				_ = cluster.db.Exec(fakeConn, reply.Args)
			}
		case *protocol.StatusReply:
			if protocol.IsOKReply(reply) {
				break slotLoop
			} else {
				// todo: return slot to former host node
				msg := fmt.Sprintf("migrate slot %d error: %s", slot.ID, reply.Status)
				logger.Errorf(msg)
				return protocol.MakeErrReply(msg)
			}
		case protocol.ErrorReply:
			// todo: return slot to former host node
			msg := fmt.Sprintf("migrate slot %d error: %s", slot.ID, reply.Error())
			logger.Errorf(msg)
			return protocol.MakeErrReply(msg)
		}
	}
	cluster.finishSlotImport(slot.ID)
	peerCli.Send(utils.ToCmdLine("gcluster", "migrate-done", strconv.Itoa(int(slot.ID))))
	return nil
}
