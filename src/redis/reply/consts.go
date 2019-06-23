package reply

type PongReply struct {}

var PongBytes = []byte("+PONG\r\n")

func (r *PongReply)ToBytes()[]byte {
    return PongBytes
}

type OkReply struct {}

var OkBytes = []byte("+OK\r\n")

func (r *OkReply)ToBytes()[]byte {
    return OkBytes
}

var nullBulkBytes = []byte("$-1\r\n")

type NullBulkReply struct {}

func (r *NullBulkReply)ToBytes()[]byte {
    return nullBulkBytes
}