package reply

import (
	"bytes"
	"github.com/hdt3213/godis/src/interface/redis"
	"strconv"
)

var (
	nullBulkReplyBytes = []byte("$-1")
	CRLF               = "\r\n"
)

/* ---- Bulk Reply ---- */

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

/* ---- Multi Bulk Reply ---- */

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
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

/* ---- Multi Raw Reply ---- */

type MultiRawReply struct {
	Args [][]byte
}

func MakeMultiRawReply(args [][]byte) *MultiRawReply {
	return &MultiRawReply{
		Args: args,
	}
}

func (r *MultiRawReply) ToBytes() []byte {
	argLen := len(r.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		buf.Write(arg)
	}
	return buf.Bytes()
}

/* ---- Status Reply ---- */

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

/* ---- Int Reply ---- */

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

/* ---- Error Reply ---- */

type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

type StandardErrReply struct {
	Status string
}

func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

func IsErrorReply(reply redis.Reply) bool {
	return reply.ToBytes()[0] == '-'
}

func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + "\r\n")
}

func (r *StandardErrReply) Error() string {
	return r.Status
}
