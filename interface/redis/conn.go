package redis

// Connection represents a connection with redis client
type Connection interface {
	Write([]byte) error
	SetPassword(string)
	GetPassword() string

	// client should keep its subscribing channels
	Subscribe(channel string)
	UnSubscribe(channel string)
	SubsCount() int
	GetChannels() []string

	// used for `Multi` command
	InMultiState() bool
	SetMultiState(bool)
	GetQueuedCmdLine() [][][]byte
	EnqueueCmd([][]byte)
	ClearQueuedCmds()
	GetWatching() map[string]uint32

	// used for multi database
	GetDBIndex() int
	SelectDB(int)
}
