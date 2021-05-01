package tcp

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"testing"
)

func TestListenAndServe(t *testing.T) {
	var err error
	var addr string
	closeChan := make(chan struct{})
	port := rand.Intn(60000) + 1024
	addr = fmt.Sprintf("localhost:%d", port)
	go func() {
		err = ListenAndServe(&Config{
			Address: addr,
		}, MakeEchoHandler(), closeChan)
	}()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 10; i++ {
		val := strconv.Itoa(rand.Int())
		_, err = conn.Write([]byte(val + "\n"))
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
		if string(line) != val {
			t.Error("get wrong response")
			return
		}
	}
	closeChan <- struct{}{}
}
