package reply

// UnknownErr
type UnknownErrReply struct {}

var unknownErrBytes = []byte("-Err unknown\r\n")

func (r *UnknownErrReply)ToBytes()[]byte {
    return unknownErrBytes
}

func (r *UnknownErrReply) Error()string {
    return "Err unknown"
}

// ArgNumErr
type ArgNumErrReply struct {
    Cmd string
}

func (r *ArgNumErrReply)ToBytes()[]byte {
    return []byte("-ERR wrong number of arguments for '" + r.Cmd + "' command\r\n")
}

func (r *ArgNumErrReply) Error()string {
    return "ERR wrong number of arguments for '" + r.Cmd + "' command"
}

// SyntaxErr
type SyntaxErrReply struct {}

var syntaxErrBytes = []byte("-Err syntax error\r\n")

func (r *SyntaxErrReply)ToBytes()[]byte {
    return syntaxErrBytes
}

func (r *SyntaxErrReply)Error()string {
    return "Err syntax error"
}

// WrongTypeErr
type WrongTypeErrReply struct {}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

func (r *WrongTypeErrReply)ToBytes()[]byte {
    return wrongTypeErrBytes
}

func (r *WrongTypeErrReply)Error()string {
    return "WRONGTYPE Operation against a key holding the wrong kind of value"
}

// ProtocolErr

type ProtocolErrReply struct {
    Msg string
}

func (r *ProtocolErrReply)ToBytes()[]byte {
    return []byte("-ERR Protocol error: '" + r.Msg + "'\r\n")
}

func (r *ProtocolErrReply) Error()string {
    return "ERR Protocol error: '" + r.Msg
}