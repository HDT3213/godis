package cluster

import (
	"bufio"
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/client"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
	"net"
	"os"
	"strconv"
	"time"
)

func (cluster *Cluster) startAsSeed() error {
	selfNodeId, err := cluster.topology.StartAsSeed(config.Properties.AnnounceAddress())
	if err != nil {
		return err
	}
	for i := 0; i < slotCount; i++ {
		cluster.initSlot(uint32(i), slotStateHost)
	}
	cluster.self = selfNodeId
	return nil
}

// Join send `gcluster join` to node in cluster to join
func (cluster *Cluster) Join(seed string) protocol.ErrorReply {
	/* STEP1: get leader from seed */
	seedCli, err := client.MakeClient(seed)
	if err != nil {
		return protocol.MakeErrReply("connect with seed failed: " + err.Error())
	}
	seedCli.Start()
	authCmdline := utils.ToCmdLine("AUTH", config.Properties.RequirePass)
	if config.Properties.RequirePass != "" {
		if ret := seedCli.Send(authCmdline); protocol.IsErrorReply(ret) {
			return ret.(protocol.ErrorReply)
		}
	}
	ret := seedCli.Send(utils.ToCmdLine("raft", "get-leader"))
	if protocol.IsErrorReply(ret) {
		return ret.(protocol.ErrorReply)
	}
	leaderInfo, ok := ret.(*protocol.MultiBulkReply)
	if !ok || len(leaderInfo.Args) != 2 {
		return protocol.MakeErrReply("ERR get-leader returns wrong reply")
	}
	leaderAddr := string(leaderInfo.Args[1])

	/* STEP2: join raft group */
	leaderCli, err := client.MakeClient(leaderAddr)
	if err != nil {
		return protocol.MakeErrReply("connect with seed failed: " + err.Error())
	}
	leaderCli.Start()
	if config.Properties.RequirePass != "" {
		if ret := leaderCli.Send(authCmdline); protocol.IsErrorReply(ret) {
			return ret.(protocol.ErrorReply)
		}
	}
	ret = leaderCli.Send(utils.ToCmdLine("raft", "join", config.Properties.AnnounceAddress()))
	if protocol.IsErrorReply(ret) {
		return ret.(protocol.ErrorReply)
	}
	snapshot, ok := ret.(*protocol.MultiBulkReply)
	if !ok || len(snapshot.Args) < 4 {
		return protocol.MakeErrReply("ERR gcluster join returns wrong reply")
	}
	cluster.topology.mu.Lock()
	defer cluster.topology.mu.Unlock()
	if errReply := cluster.topology.loadSnapshot(snapshot.Args); errReply != nil {
		return errReply
	}
	cluster.self = cluster.topology.selfNodeID
	cluster.topology.start(follower)

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
	f, err := os.Open(cluster.topology.persistFile)
	if err == os.ErrNotExist {
		return errConfigFileNotExist
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.Error("close cloud config file error: %v", err)
		}
	}()
	scanner := bufio.NewScanner(f)
	var snapshot [][]byte
	for scanner.Scan() {
		snapshot = append(snapshot, scanner.Bytes())
	}
	cluster.topology.mu.Lock()
	defer cluster.topology.mu.Unlock()
	if errReply := cluster.topology.loadSnapshot(snapshot); errReply != nil {
		return errReply
	}
	cluster.self = cluster.topology.selfNodeID
	cluster.topology.start(cluster.topology.state)
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
	node := cluster.topology.PickNode(slot.ID)
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
