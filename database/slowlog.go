package database

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SlowLogEntry struct {
	ID        int64
	Timestamp time.Time
	Duration  int64
	Command   [][]byte
	PeerId    string
}

// SlowLogger Slow query logger
type SlowLogger struct {
	mu         sync.RWMutex
	entries    []*SlowLogEntry
	count      int
	nextIdx    int
	maxEntries int

	threshold   int64
	nextID      int64
	logCommands [][]byte
}

func NewSlowLogger(maxEntries int, threshold int64) *SlowLogger {
	entries := make([]*SlowLogEntry, maxEntries)
	return &SlowLogger{
		entries:     entries,
		maxEntries:  maxEntries,
		threshold:   threshold,
		nextID:      1,
		logCommands: [][]byte{},
	}
}

func (sl *SlowLogger) Record(start time.Time, args [][]byte, client string) {
	if sl == nil || len(sl.entries) == 0 {
		return
	}
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
		Duration:  micros,
		Command:   args,
		PeerId:    client,
	}

	sl.nextID++

	// 插入到环形数组
	sl.entries[sl.nextIdx] = entry
	sl.nextIdx = (sl.nextIdx + 1) % sl.maxEntries

	// 更新条目计数
	if sl.count < sl.maxEntries {
		sl.count++
	}
}

func (sl *SlowLogger) GetEntries(count int) []*SlowLogEntry {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	if count <= 0 || sl.count == 0 {
		return []*SlowLogEntry{}
	}

	// 获取实际返回数量
	if count > sl.count {
		count = sl.count
	}

	result := make([]*SlowLogEntry, 0, count)
	startIdx := (sl.nextIdx - 1 + sl.maxEntries) % sl.maxEntries

	for i := 0; i < count; i++ {
		idx := (startIdx - i + sl.maxEntries) % sl.maxEntries
		result = append(result, sl.entries[idx])
	}

	return result
}

func (sl *SlowLogger) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.count
}

func (sl *SlowLogger) Reset() {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// 重置环形数组状态
	sl.entries = make([]*SlowLogEntry, sl.maxEntries)
	sl.count = 0
	sl.nextIdx = 0
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
