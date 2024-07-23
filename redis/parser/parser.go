package parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/protocol"
)

// Payload 存储redis.Reply或错误，redis解析器里面解析完成存储的数据的数据结构。
type Payload struct {
	Data redis.Reply
	Err  error
}

/*
	有一个Reply接口，里面包含Tobytes函数，所有的不同类型的Reply结构体都实现了Tobytes函数
	从而我们实现多态。每个不同类型的Reply结构体中有相关的字段记录了仅仅包含的字符串
	如*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
	存入到可以通过MakeMultiBulkReply 放入到
	type MultiBulkReply struct {
	Args [][]byte
	}
	被存储为[set][key][value].
	如果我们调用每个不同Reply结构体类型的ToBytes函数，则会把他变成redis的resp格式。
*/
// ParseStream 从io.Reader读取数据并通过通道发送负载
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload) // 创建通道
	go parse0(reader, ch)     // 启动协程解析数据
	return ch                 // 返回通道
}

// ParseBytes 从[]byte读取数据并返回所有回复
func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)       // 创建通道
	reader := bytes.NewReader(data) // 创建bytes阅读器
	go parse0(reader, ch)           // 启动协程解析数据
	var results []redis.Reply       // 存储解析结果
	for payload := range ch {       // 循环读取通道中的数据
		if payload == nil { // 检查负载是否为空
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil { // 检查是否有错误
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data) // 添加数据到结果集
	}
	return results, nil
}

// ParseOne 从[]byte读取数据并返回第一个回复
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

// parse0 函数解析来自io.Reader的数据，并将结果通过Payload结构发送到通道
// rawReader io.Reader: 输入源，可以是网络连接、文件等
// ch chan<- *Payload: 结果发送通道，Payload包含解析结果或错误信息
func parse0(rawReader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			// 如果发生panic，则捕获异常并记录错误和堆栈信息
			logger.Error(err, string(debug.Stack()))
		}
	}()
	reader := bufio.NewReader(rawReader) //创建一个缓冲读取器
	for {
		line, err := reader.ReadBytes('\n') // 逐行读取数据
		/*
			客户端可能发来的是单行数据，例如+OK\r\n
			或者多行数据$3\r\nset\r\n  字符串是两行数据，第一行是$3\r\n   第二行是set\r\n
		*/
		if err != nil {
			ch <- &Payload{Err: err} // 读取错误处理，将错误发送到通道并关闭通道
			close(ch)
			return
		}
		// 处理每行数据
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			// 如果读取的行长度小于等于2或者行的倒数第二个字符不是回车符，则忽略这行数据
			continue
		}
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'}) // 移除行尾的回车换行符
		switch line[0] {                                  // 根据行的首个字符判断数据类型，并进行相应的处理
		case '+': // 状态回复
			content := string(line[1:])
			ch <- &Payload{
				Data: protocol.MakeStatusReply(content), // 创建状态回复并发送
			}
			if strings.HasPrefix(content, "FULLRESYNC") { // 特定命令的额外处理逻辑
				// 如果内容以"FULLRESYNC"开始，处理RDB批量字符串
				err = parseRDBBulkString(reader, ch)
				if err != nil {
					// 如果处理过程中发生错误，发送错误信息并关闭通道
					ch <- &Payload{Err: err}
					close(ch)
					return
				}
			}
		case '-': // 错误回复
			ch <- &Payload{
				Data: protocol.MakeErrReply(string(line[1:])), // 创建错误回复并发送
			}
		case ':':
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				// 如果整数解析失败，记录协议错误
				protocolError(ch, "illegal number "+string(line[1:]))
				continue
			}
			ch <- &Payload{
				Data: protocol.MakeIntReply(value), // 创建整数回复并发送
			}
		case '$':
			err = parseBulkString(line, reader, ch) //因为第一行数据已经取走，所以后面的reader部分不包含line的东西
			if err != nil {
				ch <- &Payload{Err: err}
				// 如果解析批量字符串失败，发送错误信息并关闭通道
				close(ch)
				return
			}
		case '*':
			err = parseArray(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		default: // 默认情况，处理多批量回复
			args := bytes.Split(line, []byte{' '})
			ch <- &Payload{
				Data: protocol.MakeMultiBulkReply(args),
			}
		}
	}
}

// parseBulkString 解析批量字符串回复。
// header []byte: 接收到的以 '$' 开头的行，表示批量字符串的长度。
// reader *bufio.Reader: 用于从连接中继续读取批量字符串的内容。
// ch chan<- *Payload: 用于发送解析后的结果或错误信息的通道。
func parseBulkString(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 解析批量字符串长度 此时的header $3
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen < -1 {
		// 如果解析错误或字符串长度非法，则发送协议错误信息到通道
		protocolError(ch, "illegal bulk string header: "+string(header))
		return nil
	} else if strLen == -1 {
		// 如果字符串长度为-1，表示这是一个空的批量字符串（即 "$-1\r\n"）
		ch <- &Payload{
			Data: protocol.MakeNullBulkReply(), // 发送空批量字符串回复
		}
		return nil
	}
	// 分配足够的空间来存储字符串内容和结尾的CRLF
	body := make([]byte, strLen+2)
	//从reader中读取指定长度的数据放到缓冲区中
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err // 如果读取过程中出现错误，直接返回这个错误
	}
	// 将读取到的内容（去除最后的CRLF）发送到通道，
	//所以最终存储在我们的Payload中的DATA的redis.Reply结构体中的Arg []byte。

	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)-2]),
	}
	return nil
}

// parseRDBBulkString 处理RDB文件数据流中的批量字符串，因为RDB和AOF之间没有CRLF。
func parseRDBBulkString(reader *bufio.Reader, ch chan<- *Payload) error {
	header, err := reader.ReadBytes('\n')
	if err != nil {
		return errors.New("failed to read bytes")
	}
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

// parseArray 用于解析Redis协议中的数组数据。
// header []byte: 包含数组元素数量的头部数据，如*3表示数组有三个元素。
func parseArray(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < 0 {
		// 如果解析出错或数组长度小于0，发送协议错误信息
		protocolError(ch, "illegal array header "+string(header[1:]))
		return nil
	} else if nStrs == 0 {
		ch <- &Payload{
			Data: protocol.MakeEmptyMultiBulkReply(),
		}
		return nil
	}
	// 初始化一个切片来存储数组中的元素，预分配nStrs长度的空间
	lines := make([][]byte, 0, nStrs)
	for i := int64(0); i < nStrs; i++ {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		length := len(line)
		// 检查读取的行是否合法，长度至少为4，并且以'$'开头
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			// 如果不符合批量字符串的要求，发送协议错误
			protocolError(ch, "illegal bulk string header "+string(line))
			break
		}
		// 解析批量字符串的长度
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			// 如果长度解析失败，发送协议错误
			protocolError(ch, "illegal bulk string length "+string(line))
			break
		} else if strLen == -1 {
			// 如果长度为-1，表示空的批量字符串
			lines = append(lines, []byte{})
		} else {
			// 分配足够的空间来存储字符串内容和结尾的CRLF
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
