package redis

type Reply interface {
	ToBytes() []byte
}
