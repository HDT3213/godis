package reply

import (
    "strconv"
)

var (
    nullBulkReplyBytes = []byte("$-1")
    CRLF               = "\r\n"
)

type BulkReply struct {
    Arg []byte
}

func MakeBulkReply(arg []byte) *BulkReply {
    return &BulkReply{
        Arg: arg,
    }
}

func (r *BulkReply) ToBytes() []byte {
    if len(r.Arg) == 0 {
        return nullBulkReplyBytes
    }
    return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

type MultiBulkReply struct {
    Args [][]byte
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
    return &MultiBulkReply{
        Args: args,
    }
}

func (r *MultiBulkReply) ToBytes() []byte {
    argLen := len(r.Args)
    res := "*" + strconv.Itoa(argLen) + CRLF
    for _, arg := range r.Args {
        if arg == nil {
            res += "$-1" + CRLF
        } else {
            res += "$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF
        }
    }
    return []byte(res)
}

type StatusReply struct {
    Status string
}

func MakeStatusReply(status string) *StatusReply {
    return &StatusReply{
        Status: status,
    }
}

func (r *StatusReply) ToBytes() []byte {
    return []byte("+" + r.Status + "\r\n")
}

type ErrReply struct {
    Status string
}

func MakeErrReply(status string) *ErrReply {
    return &ErrReply{
        Status: status,
    }
}

func (r *ErrReply) ToBytes() []byte {
    return []byte("-" + r.Status + "\r\n")
}

type IntReply struct {
    Code int64
}

func MakeIntReply(code int64) *IntReply {
    return &IntReply{
        Code: code,
    }
}

func (r *IntReply) ToBytes() []byte {
    return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}
