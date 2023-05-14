package cluster

import (
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
	"net"
	"strconv"
	"time"
)

func (cluster *Cluster) startAsSeed() protocol.ErrorReply {
	err := cluster.topology.StartAsSeed(config.Properties.AnnounceAddress())
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
	fakeConn := connection.NewFakeConn()
	node := cluster.pickNode(slot.ID)
	conn, err := net.Dial("tcp", node.Addr)
	if err != nil {
		return fmt.Errorf("connect with %s(%s) error: %v", node.ID, node.Addr, err)
	}
	defer conn.Close()
	// we need to receive continuous stream like replication slave, so redis.Client cannot be used here
	nodeChan := parser.ParseStream(conn)
	send2node := func(cmdLine CmdLine) redis.Reply {
		req := protocol.MakeMultiBulkReply(cmdLine)
		_, err := conn.Write(req.ToBytes())
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		resp := <-nodeChan
		if resp.Err != nil {
			return protocol.MakeErrReply(resp.Err.Error())
		}
		return resp.Data
	}

	cluster.initSlot(slot.ID, slotStateImporting) // prepare host slot before send `set slot`
	cluster.setLocalSlotImporting(slot.ID, cluster.self)
	ret := send2node(utils.ToCmdLine(
		"gcluster", "set-slot", strconv.Itoa(int(slot.ID)), cluster.self))
	if !protocol.IsOKReply(ret) {
		return fmt.Errorf("set slot %d error: %v", slot.ID, ret)
	}
	logger.Info(fmt.Sprintf("set slot %d to current node, start migrate", slot.ID))

	req := protocol.MakeMultiBulkReply(utils.ToCmdLine(
		"gcluster", "migrate", strconv.Itoa(int(slot.ID)), cluster.self))
	_, err = conn.Write(req.ToBytes())
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
slotLoop:
	for proto := range nodeChan {
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
	send2node(utils.ToCmdLine("gcluster", "migrate-done", strconv.Itoa(int(slot.ID))))
	return nil
}
