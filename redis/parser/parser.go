package parser

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/protocol"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

// Payload stores redis.Reply or error
type Payload struct {
	Data redis.Reply
	Err  error
}

// ParseStream reads data from io.Reader and send payloads through channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// ParseBytes reads data from []byte and return all replies
func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	var results []redis.Reply
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

// ParseOne reads data from []byte and return the first payload
func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	payload := <-ch // parse0 will close the channel
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Err
}

//parse0 RESP协议解析
func parse0(rawReader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()
	reader := bufio.NewReader(rawReader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Err: err}
			close(ch)
			return
		}
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			// there are some empty lines within replication traffic, ignore this error
			//protocolError(ch, "empty line")
			continue
		}
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		switch line[0] {
		// Simple Strings。以+为前缀的响应数据
		case '+':
			content := string(line[1:])
			ch <- &Payload{
				Data: protocol.MakeStatusReply(content),
			}
			if strings.HasPrefix(content, "FULLRESYNC") {
				err = parseRDBBulkString(reader, ch)
				if err != nil {
					ch <- &Payload{Err: err}
					close(ch)
					return
				}
			}
		// RESP 协议中错误响应
		// SETNX, DEL, EXISTS, INCR, INCRBY, DECR, DECRBY, DBSIZE, LASTSAVE, RENAMENX, MOVE, LLEN, SADD, SREM, SISMEMBER, SCARD.
		case '-':
			ch <- &Payload{
				Data: protocol.MakeErrReply(string(line[1:])),
			}
		// RESP 整型。表示响应的是整数，以 : 开头，比如 :0\r\n 和 :1000\r\n
		case ':':
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				protocolError(ch, "illegal number "+string(line[1:]))
				continue
			}
			ch <- &Payload{
				Data: protocol.MakeIntReply(value),
			}
		// RESP Bulk Strings.
		// 批量回复，是一个大小在 512 Mb 的二进制安全字符串
		// 以 $ 开头，紧跟一个整数代表回复字符串的大小，以 \r\n 结束
		// 随后是 实际的字符串数据
		// 最后以 “\r\n” 结尾
		case '$':
			err = parseBulkString(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		// RESP Arrays。数组，对于响应的集合元素，比如 LRANGE 命令，返回的是元素列表，也就是数组形式。
		// 以 * 开头表示，紧接着是一个整数，表示数组元素个数，并以 \r\n 结尾。
		// 数组的每个元素的都是 RESP 提供的类型。
		case '*':
			err = parseArray(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		default:
			args := bytes.Split(line, []byte{' '})
			ch <- &Payload{
				Data: protocol.MakeMultiBulkReply(args),
			}
		}
	}
}

func parseBulkString(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen < -1 {
		protocolError(ch, "illegal bulk string header: "+string(header))
		return nil
	} else if strLen == -1 {
		ch <- &Payload{
			Data: protocol.MakeNullBulkReply(),
		}
		return nil
	}
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)-2]),
	}
	return nil
}

// there is no CRLF between RDB and following AOF, therefore it needs to be treated differently
func parseRDBBulkString(reader *bufio.Reader, ch chan<- *Payload) error {
	header, err := reader.ReadBytes('\n')
	header = bytes.TrimSuffix(header, []byte{'\r', '\n'})
	if len(header) == 0 {
		return errors.New("empty header")
	}
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen <= 0 {
		return errors.New("illegal bulk header: " + string(header))
	}
	body := make([]byte, strLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)]),
	}
	return nil
}

func parseArray(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < 0 {
		protocolError(ch, "illegal array header "+string(header[1:]))
		return nil
	} else if nStrs == 0 {
		ch <- &Payload{
			Data: protocol.MakeEmptyMultiBulkReply(),
		}
		return nil
	}
	lines := make([][]byte, 0, nStrs)
	for i := int64(0); i < nStrs; i++ {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			protocolError(ch, "illegal bulk string header "+string(line))
			break
		}
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			protocolError(ch, "illegal bulk string length "+string(line))
			break
		} else if strLen == -1 {
			lines = append(lines, []byte{})
		} else {
			body := make([]byte, strLen+2)
			_, err := io.ReadFull(reader, body)
			if err != nil {
				return err
			}
			lines = append(lines, body[:len(body)-2])
		}
	}
	ch <- &Payload{
		Data: protocol.MakeMultiBulkReply(lines),
	}
	return nil
}

func protocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	ch <- &Payload{Err: err}
}
