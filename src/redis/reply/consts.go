package reply

type PongReply struct {}

var PongBytes = []byte("+PONG\r\n")

func (r *PongReply)ToBytes()[]byte {
    return PongBytes
}

type OkReply struct {}

var okBytes = []byte("+OK\r\n")

func (r *OkReply)ToBytes()[]byte {
    return okBytes
}

var nullBulkBytes = []byte("$-1\r\n")

type NullBulkReply struct {}

func (r *NullBulkReply)ToBytes()[]byte {
    return nullBulkBytes
}

var emptyMultiBulkBytes = []byte("*0\r\n")

type EmptyMultiBulkReply struct {}

func (r *EmptyMultiBulkReply)ToBytes()[]byte {
    return emptyMultiBulkBytes
}

// reply nothing, for commands like subscribe
type NoReply struct {}

var NoBytes = []byte("")

func (r *NoReply)ToBytes()[]byte {
    return NoBytes
}