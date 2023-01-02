package redis

// Connection represents a connection with redis client
type Connection interface {
	Write([]byte) (int, error)
	Close() error
	RemoteAddr() string

	SetPassword(string)
	GetPassword() string

	// client should keep its subscribing channels
	Subscribe(channel string)
	UnSubscribe(channel string)
	SubsCount() int
	GetChannels() []string

	InMultiState() bool
	SetMultiState(bool)
	GetQueuedCmdLine() [][][]byte
	EnqueueCmd([][]byte)
	ClearQueuedCmds()
	GetWatching() map[string]uint32
	AddTxError(err error)
	GetTxErrors() []error

	GetDBIndex() int
	SelectDB(int)

	SetSlave()
	IsSlave() bool

	SetMaster()
	IsMaster() bool

	Name() string
}
