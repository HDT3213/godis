package reply

// UnknownErr
type UnknownErrReply struct {}

var unknownErrBytes = []byte("-Err unknown\r\n")

func (r *UnknownErrReply)ToBytes()[]byte {
    return unknownErrBytes
}

// ArgNumErr
type ArgNumErrReply struct {
    Cmd string
}

func (r *ArgNumErrReply)ToBytes()[]byte {
    return []byte("-ERR wrong number of arguments for '" + r.Cmd + "' command\r\n")
}

// SyntaxErr
type SyntaxErrReply struct {}

var syntaxErrBytes = []byte("-Err syntax error\r\n")

func (r *SyntaxErrReply)ToBytes()[]byte {
    return syntaxErrBytes
}

// WrongTypeErr
type WrongTypeErrReply struct {}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

func (r *WrongTypeErrReply)ToBytes()[]byte {
    return wrongTypeErrBytes
}
