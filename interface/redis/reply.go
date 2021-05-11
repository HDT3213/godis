package redis

// Reply is the interface of redis serialization protocol message
type Reply interface {
	ToBytes() []byte
}
