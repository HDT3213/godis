package database

import (
	"container/list"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
	"strings"
	"sync"
	"time"
)

// GodisExecCommandStartUnixTime Record the start time of command execution
var GodisExecCommandStartUnixTime time.Time

type SlowLogEntry struct {
	ID        int64
	Timestamp time.Time
	Duration  time.Duration
	Command   [][]byte
	PeerId    string
}

// SlowLogger Slow query logger
type SlowLogger struct {
	mu         sync.RWMutex
	entries    *list.List // 日志链表
	maxEntries int        // 最大条目数

	threshold   int64 // 阈值(微秒)
	nextID      int64
	logCommands [][]byte
}

func NewSlowLogger(maxEntries int, threshold int64) *SlowLogger {
	return &SlowLogger{
		entries:     list.New(),
		maxEntries:  maxEntries,
		threshold:   threshold,
		nextID:      1,
		logCommands: [][]byte{},
	}
}

func (sl *SlowLogger) Record(start time.Time, args [][]byte, client string) {
	duration := time.Since(start)
	micros := duration.Microseconds()

	if micros < sl.threshold {
		return
	}

	sl.mu.Lock()
	defer sl.mu.Unlock()

	entry := &SlowLogEntry{
		ID:        sl.nextID,
		Timestamp: start,
		Duration:  duration,
		Command:   args,
		PeerId:    client,
	}

	sl.nextID++

	// Add to the header of the linked list
	sl.entries.PushFront(entry)

	// When the list exceeds the preset length, the last element pops up
	for sl.entries.Len() > sl.maxEntries {
		oldest := sl.entries.Back()
		sl.entries.Remove(oldest)
	}
}

func (sl *SlowLogger) GetEntries(count int) []*SlowLogEntry {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	result := make([]*SlowLogEntry, 0, count)
	for e := sl.entries.Front(); e != nil && count > 0; e = e.Next() {
		entry := e.Value.(*SlowLogEntry)
		result = append(result, entry)
		count--
	}

	return result
}

func (sl *SlowLogger) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.entries.Len()
}

func (sl *SlowLogger) Reset() {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	sl.entries.Init()
	sl.nextID = 1
}

// HandleSlowlogCommand Process SLOWLOG command
func (sl *SlowLogger) HandleSlowlogCommand(args [][]byte) redis.Reply {
	argsLen := len(args)
	if argsLen <= 1 || argsLen > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'SLOWLOG' command")
	}

	subCmd := strings.ToLower(string(args[1]))

	switch subCmd {
	case "get":
		count := 10
		if argsLen == 3 {
			n, err := strconv.Atoi(string(args[2]))
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			if n < 0 {
				return protocol.MakeEmptyMultiBulkReply()
			}
			count = n
		}
		entries := sl.GetEntries(count)
		return formatSlowlogEntries(entries)
	case "len":
		return protocol.MakeIntReply(int64(sl.Len()))
	case "reset":
		sl.Reset()
		return protocol.MakeOkReply()
	default:
		return protocol.MakeErrReply("ERR Unknown subcommand or wrong number of arguments for  '" + subCmd + "'")
	}
}

func formatSlowlogEntries(entries []*SlowLogEntry) redis.Reply {
	result := make([]redis.Reply, 0, len(entries))
	for _, log := range entries {
		logList := make([]redis.Reply, 0)
		logList = append(logList, protocol.MakeIntReply(log.ID),
			protocol.MakeIntReply(log.Timestamp.Unix()),
			protocol.MakeIntReply(int64(log.Duration)),
			protocol.MakeMultiBulkReply(log.Command),
			protocol.MakeBulkReply([]byte(log.PeerId)))
		result = append(result, protocol.MakeMultiRawReply(logList))
	}
	return protocol.MakeMultiRawReply(result)
}
