package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	rdb "github.com/hdt3213/rdb/parser"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
)

const (
	masterRole = iota
	slaveRole
)

type slaveStatus struct {
	mutex  sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc

	// configVersion stands for the version of slaveStatus config. Any change of master host/port will cause configVersion increment
	// If configVersion change has been found during slaveStatus current slaveStatus procedure will stop.
	// It is designed to abort a running slaveStatus procedure
	configVersion int32

	masterHost string
	masterPort int

	masterConn   net.Conn
	masterChan   <-chan *parser.Payload
	replId       string
	replOffset   int64
	lastRecvTime time.Time
	running      sync.WaitGroup
}

var configChangedErr = errors.New("slaveStatus config changed")

func initReplSlaveStatus() *slaveStatus {
	return &slaveStatus{}
}

func (server *Server) execSlaveOf(c redis.Connection, args [][]byte) redis.Reply {
	if strings.ToLower(string(args[0])) == "no" &&
		strings.ToLower(string(args[1])) == "one" {
		server.slaveOfNone()
		return protocol.MakeOkReply()
	}
	host := string(args[0])
	port, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	server.slaveStatus.mutex.Lock()
	atomic.StoreInt32(&server.role, slaveRole)
	server.slaveStatus.masterHost = host
	server.slaveStatus.masterPort = port
	atomic.AddInt32(&server.slaveStatus.configVersion, 1)
	server.slaveStatus.mutex.Unlock()
	go server.setupMaster()
	return protocol.MakeOkReply()
}

func (server *Server) slaveOfNone() {
	server.slaveStatus.mutex.Lock()
	defer server.slaveStatus.mutex.Unlock()
	server.slaveStatus.masterHost = ""
	server.slaveStatus.masterPort = 0
	server.slaveStatus.replId = ""
	server.slaveStatus.replOffset = -1
	server.slaveStatus.stopSlaveWithMutex()
	server.role = masterRole
}

// stopSlaveWithMutex stops in-progress connectWithMaster/fullSync/receiveAOF
// invoker should have slaveStatus mutex
func (repl *slaveStatus) stopSlaveWithMutex() {
	// update configVersion to stop connectWithMaster and fullSync
	atomic.AddInt32(&repl.configVersion, 1)
	// send cancel to receiveAOF
	if repl.cancel != nil {
		repl.cancel()
		repl.running.Wait()
	}
	repl.ctx = context.Background()
	repl.cancel = nil
	if repl.masterConn != nil {
		_ = repl.masterConn.Close() // parser.ParseStream will close masterChan
	}
	repl.masterConn = nil
	repl.masterChan = nil
}

func (repl *slaveStatus) close() error {
	repl.mutex.Lock()
	defer repl.mutex.Unlock()
	repl.stopSlaveWithMutex()
	return nil
}

// setupMaster connects to master and starts full sync
func (server *Server) setupMaster() {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	var configVersion int32
	ctx, cancel := context.WithCancel(context.Background())
	server.slaveStatus.mutex.Lock()
	server.slaveStatus.ctx = ctx
	server.slaveStatus.cancel = cancel
	configVersion = server.slaveStatus.configVersion
	server.slaveStatus.mutex.Unlock()
	isFullReSync, err := server.connectWithMaster(configVersion)
	if err != nil {
		// connect failed, abort master
		logger.Error(err)
		server.slaveOfNone()
		return
	}
	if isFullReSync {
		err = server.loadMasterRDB(configVersion)
		if err != nil {
			// load failed, abort master
			logger.Error(err)
			server.slaveOfNone()
			return
		}
	}
	err = server.receiveAOF(ctx, configVersion)
	if err != nil {
		// full sync failed, abort
		logger.Error(err)
		return
	}
}

// connectWithMaster finishes handshake with master
// returns: isFullReSync, error
func (server *Server) connectWithMaster(configVersion int32) (bool, error) {
	addr := server.slaveStatus.masterHost + ":" + strconv.Itoa(server.slaveStatus.masterPort)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		server.slaveOfNone() // abort
		return false, errors.New("connect master failed " + err.Error())
	}
	masterChan := parser.ParseStream(conn)

	// ping
	pingCmdLine := utils.ToCmdLine("ping")
	pingReq := protocol.MakeMultiBulkReply(pingCmdLine)
	_, err = conn.Write(pingReq.ToBytes())
	if err != nil {
		return false, errors.New("send failed " + err.Error())
	}
	pingResp := <-masterChan
	if pingResp.Err != nil {
		return false, errors.New("read response failed: " + pingResp.Err.Error())
	}
	switch reply := pingResp.Data.(type) {
	case *protocol.StandardErrReply:
		if !strings.HasPrefix(reply.Error(), "NOAUTH") &&
			!strings.HasPrefix(reply.Error(), "NOPERM") &&
			!strings.HasPrefix(reply.Error(), "ERR operation not permitted") {
			logger.Error("Error reply to PING from master: " + string(reply.ToBytes()))
			server.slaveOfNone() // abort
			return false, nil
		}
	}

	// just to reduce duplication of code
	sendCmdToMaster := func(conn net.Conn, cmdLine CmdLine, masterChan <-chan *parser.Payload) error {
		req := protocol.MakeMultiBulkReply(cmdLine)
		_, err := conn.Write(req.ToBytes())
		if err != nil {
			server.slaveOfNone() // abort
			return errors.New("send failed " + err.Error())
		}
		resp := <-masterChan
		if resp.Err != nil {
			server.slaveOfNone() // abort
			return errors.New("read response failed: " + resp.Err.Error())
		}
		if !protocol.IsOKReply(resp.Data) {
			server.slaveOfNone() // abort
			return errors.New("unexpected auth response: " + string(resp.Data.ToBytes()))
		}
		return nil
	}

	// auth
	if config.Properties.MasterAuth != "" {
		authCmdLine := utils.ToCmdLine("auth", config.Properties.MasterAuth)
		err = sendCmdToMaster(conn, authCmdLine, masterChan)
		if err != nil {
			return false, err
		}
	}

	// announce port
	var port int
	if config.Properties.SlaveAnnouncePort != 0 {
		port = config.Properties.SlaveAnnouncePort
	} else {
		port = config.Properties.Port
	}
	portCmdLine := utils.ToCmdLine("REPLCONF", "listening-port", strconv.Itoa(port))
	err = sendCmdToMaster(conn, portCmdLine, masterChan)
	if err != nil {
		return false, err
	}

	// announce ip
	if config.Properties.SlaveAnnounceIP != "" {
		ipCmdLine := utils.ToCmdLine("REPLCONF", "ip-address", config.Properties.SlaveAnnounceIP)
		err = sendCmdToMaster(conn, ipCmdLine, masterChan)
		if err != nil {
			return false, err
		}
	}

	// announce capacity
	capaCmdLine := utils.ToCmdLine("REPLCONF", "capa", "psync2")
	err = sendCmdToMaster(conn, capaCmdLine, masterChan)
	if err != nil {
		return false, err
	}

	// update connection
	server.slaveStatus.mutex.Lock()
	defer server.slaveStatus.mutex.Unlock()
	if server.slaveStatus.configVersion != configVersion {
		// slaveStatus conf changed during connecting and waiting mutex
		return false, configChangedErr
	}
	server.slaveStatus.masterConn = conn
	server.slaveStatus.masterChan = masterChan
	server.slaveStatus.lastRecvTime = time.Now()
	return server.psyncHandshake()
}

// psyncHandshake send `psync` to master and sync repl-id/offset with master
// invoker should provide with slaveStatus.mutex
func (server *Server) psyncHandshake() (bool, error) {
	replId := "?"
	var replOffset int64 = -1
	if server.slaveStatus.replId != "" {
		replId = server.slaveStatus.replId
		replOffset = server.slaveStatus.replOffset
	}
	psyncCmdLine := utils.ToCmdLine("psync", replId, strconv.FormatInt(replOffset, 10))
	psyncReq := protocol.MakeMultiBulkReply(psyncCmdLine)
	_, err := server.slaveStatus.masterConn.Write(psyncReq.ToBytes())
	if err != nil {
		return false, errors.New("send failed " + err.Error())
	}
	return server.parsePsyncHandshake()
}

func (server *Server) parsePsyncHandshake() (bool, error) {
	var err error
	psyncPayload := <-server.slaveStatus.masterChan
	if psyncPayload.Err != nil {
		return false, errors.New("read response failed: " + psyncPayload.Err.Error())
	}
	psyncHeader, ok := psyncPayload.Data.(*protocol.StatusReply)
	if !ok {
		return false, errors.New("illegal payload header not a status reply: " + string(psyncPayload.Data.ToBytes()))
	}
	headers := strings.Split(psyncHeader.Status, " ")
	if len(headers) != 3 && len(headers) != 2 {
		return false, errors.New("illegal payload header: " + psyncHeader.Status)
	}

	logger.Info("receive psync header from master")
	var isFullReSync bool
	if headers[0] == "FULLRESYNC" {
		logger.Info("full re-sync with master")
		server.slaveStatus.replId = headers[1]
		server.slaveStatus.replOffset, err = strconv.ParseInt(headers[2], 10, 64)
		isFullReSync = true
	} else if headers[0] == "CONTINUE" {
		logger.Info("continue partial sync")
		server.slaveStatus.replId = headers[1]
		isFullReSync = false
	} else {
		return false, errors.New("illegal psync resp: " + psyncHeader.Status)
	}

	if err != nil {
		return false, errors.New("get illegal repl offset: " + headers[2])
	}
	logger.Info(fmt.Sprintf("repl id: %s, current offset: %d", server.slaveStatus.replId, server.slaveStatus.replOffset))
	return isFullReSync, nil
}

func makeRdbLoader(upgradeAof bool) (*Server, string, error) {
	rdbLoader := MakeAuxiliaryServer()
	if !upgradeAof {
		return rdbLoader, "", nil
	}
	// make aof handler to generate new aof file during loading rdb
	newAofFile, err := ioutil.TempFile("", "*.aof")
	if err != nil {
		return nil, "", fmt.Errorf("create temp rdb failed: %v", err)
	}
	newAofFilename := newAofFile.Name()
	aofHandler, err := NewPersister(rdbLoader, newAofFilename, false, aof.FsyncNo)
	if err != nil {
		return nil, "", err
	}
	rdbLoader.bindPersister(aofHandler)
	return rdbLoader, newAofFilename, nil
}

// loadMasterRDB downloads rdb after handshake has been done
func (server *Server) loadMasterRDB(configVersion int32) error {
	rdbPayload := <-server.slaveStatus.masterChan
	if rdbPayload.Err != nil {
		return errors.New("read response failed: " + rdbPayload.Err.Error())
	}
	rdbReply, ok := rdbPayload.Data.(*protocol.BulkReply)
	if !ok {
		return errors.New("illegal payload header: " + string(rdbPayload.Data.ToBytes()))
	}

	logger.Info(fmt.Sprintf("receive %d bytes of rdb from master", len(rdbReply.Arg)))
	rdbDec := rdb.NewDecoder(bytes.NewReader(rdbReply.Arg))

	rdbLoader, newAofFilename, err := makeRdbLoader(config.Properties.AppendOnly)
	if err != nil {
		return err
	}
	err = rdbLoader.LoadRDB(rdbDec)
	if err != nil {
		return errors.New("dump rdb failed: " + err.Error())
	}

	server.slaveStatus.mutex.Lock()
	defer server.slaveStatus.mutex.Unlock()
	if server.slaveStatus.configVersion != configVersion {
		// slaveStatus conf changed during connecting and waiting mutex
		return configChangedErr
	}
	for i, h := range rdbLoader.dbSet {
		newDB := h.Load().(*DB)
		server.loadDB(i, newDB)
	}

	if config.Properties.AppendOnly {
		// use new aof file
		server.persister.Close()
		err = os.Rename(newAofFilename, config.Properties.AppendFilename)
		if err != nil {
			return err
		}
		persister, err := NewPersister(server, config.Properties.AppendFilename, false, config.Properties.AppendFsync)
		if err != nil {
			return err
		}
		server.bindPersister(persister)
	}

	return nil
}

func (server *Server) receiveAOF(ctx context.Context, configVersion int32) error {
	conn := connection.NewConn(server.slaveStatus.masterConn)
	conn.SetMaster()
	server.slaveStatus.running.Add(1)
	defer server.slaveStatus.running.Done()
	for {
		select {
		case payload, open := <-server.slaveStatus.masterChan:
			if !open {
				return errors.New("master channel unexpected close")
			}
			if payload.Err != nil {
				return payload.Err
			}
			cmdLine, ok := payload.Data.(*protocol.MultiBulkReply)
			if !ok {
				return errors.New("unexpected payload: " + string(payload.Data.ToBytes()))
			}
			server.slaveStatus.mutex.Lock()
			if server.slaveStatus.configVersion != configVersion {
				// slaveStatus conf changed during connecting and waiting mutex
				return configChangedErr
			}
			server.Exec(conn, cmdLine.Args)
			n := len(cmdLine.ToBytes()) // todo: directly get size from socket
			server.slaveStatus.replOffset += int64(n)
			server.slaveStatus.lastRecvTime = time.Now()
			logger.Info(fmt.Sprintf("receive %d bytes from master, current offset %d, %s",
				n, server.slaveStatus.replOffset, strconv.Quote(string(cmdLine.ToBytes()))))
			server.slaveStatus.mutex.Unlock()
		case <-ctx.Done():
			_ = conn.Close()
			return nil
		}
	}
}

func (server *Server) slaveCron() {
	repl := server.slaveStatus
	if repl.masterConn == nil {
		return
	}

	// check master timeout
	replTimeout := 60 * time.Second
	if config.Properties.ReplTimeout != 0 {
		replTimeout = time.Duration(config.Properties.ReplTimeout) * time.Second
	}
	minLastRecvTime := time.Now().Add(-replTimeout)
	if repl.lastRecvTime.Before(minLastRecvTime) {
		// reconnect with master
		err := server.reconnectWithMaster()
		if err != nil {
			logger.Error("send failed " + err.Error())
		}
		return
	}
	// send ack to master
	err := repl.sendAck2Master()
	if err != nil {
		logger.Error("send failed " + err.Error())
	}
}

// Send a REPLCONF ACK command to the master to inform it about the current processed offset
func (repl *slaveStatus) sendAck2Master() error {
	psyncCmdLine := utils.ToCmdLine("REPLCONF", "ACK",
		strconv.FormatInt(repl.replOffset, 10))
	psyncReq := protocol.MakeMultiBulkReply(psyncCmdLine)
	_, err := repl.masterConn.Write(psyncReq.ToBytes())
	// logger.Info("send ack to master")
	return err
}

func (server *Server) reconnectWithMaster() error {
	logger.Info("reconnecting with master")
	server.slaveStatus.mutex.Lock()
	defer server.slaveStatus.mutex.Unlock()
	server.slaveStatus.stopSlaveWithMutex()
	go server.setupMaster()
	return nil
}
