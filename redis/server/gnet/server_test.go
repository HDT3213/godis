package gnet

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/hdt3213/godis/database"
)

func TestListenAndServe(t *testing.T) {
	var err error
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := listener.Addr().String()
	db := database.NewStandaloneServer()
	server := NewGnetServer(db)
	go server.Run(addr)
	time.Sleep(2*time.Second)
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
	conn.Close()
	server.Close()
}
