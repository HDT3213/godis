package server

import (
	"bufio"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/tcp"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
)

func startServe(t *testing.T, ch chan struct{}) string {
	var err error
	listener, err := net.Listen("tcp", "127.0.0.1:9999")
	if err != nil {
		t.Error(err)
		return ""
	}
	addr := listener.Addr().String()
	go tcp.ListenAndServe(listener, MakeHandler(), ch)
	return addr
}

func TestListenAndServe(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := listener.Addr().String()
	go tcp.ListenAndServe(listener, MakeHandler(), closeChan)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = conn.Write([]byte("PING\r\n"))
	if err != nil {
		t.Error(err)
		return
	}
	bufReader := bufio.NewReader(conn)
	line, _, err := bufReader.ReadLine()
	if err != nil {
		t.Error(err)
		return
	}
	if string(line) != "+PONG" {
		t.Error("get wrong response")
		return
	}
	closeChan <- struct{}{}
	time.Sleep(time.Second * 2)
}

func TestAuthButNoPasswordSet(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	addr := startServe(t, closeChan)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = conn.Write([]byte("Auth root\r\n"))
	if err != nil {
		t.Error(err)
		return
	}
	bufReader := bufio.NewReader(conn)
	line, _, err := bufReader.ReadLine()
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t,"-ERR Client sent AUTH, but no password is set", string(line))

	closeChan <- struct{}{}
	time.Sleep(time.Second)
}

func TestAuthWrongArgs(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	addr := startServe(t, closeChan)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = conn.Write([]byte("Auth root 123 \r\n"))
	if err != nil {
		t.Error(err)
		return
	}
	bufReader := bufio.NewReader(conn)
	line, _, err := bufReader.ReadLine()
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t,"-ERR wrong number of arguments for 'auth' command", string(line))

	closeChan <- struct{}{}
	time.Sleep(time.Second)
}

func TestAuthRequired(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	addr := startServe(t, closeChan)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}

	config.Properties.RequirePass = "root"
	_, err = conn.Write([]byte("PING\r\n"))
	if err != nil {
		t.Error(err)
		return
	}
	bufReader := bufio.NewReader(conn)
	line, _, err := bufReader.ReadLine()
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t,"-NOAUTH Authentication required", string(line))

	_, err = conn.Write([]byte("Auth 123\r\n"))
	if err != nil {
		t.Error(err)
		return
	}
	bufReader = bufio.NewReader(conn)
	line, _, err = bufReader.ReadLine()
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t,"-ERR invalid password", string(line))

	_, err = conn.Write([]byte("Auth root\r\n"))
	if err != nil {
		t.Error(err)
		return
	}
	bufReader = bufio.NewReader(conn)
	line, _, err = bufReader.ReadLine()
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t,"+OK", string(line))

	_, err = conn.Write([]byte("PING\r\n"))
	if err != nil {
		t.Error(err)
		return
	}
	bufReader = bufio.NewReader(conn)
	line, _, err = bufReader.ReadLine()
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t,"+PONG", string(line))

	closeChan <- struct{}{}
	time.Sleep(time.Second)
}