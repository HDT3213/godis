package tcp

import (
	"bufio"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestListenAndServe(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := listener.Addr().String()
	go ListenAndServe(listener, MakeEchoHandler(), closeChan)

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
	_ = conn.Close()
	for i := 0; i < 5; i++ {
		// create idle connection
		_, _ = net.Dial("tcp", addr)
	}
	closeChan <- struct{}{}
	time.Sleep(time.Second)
}

func TestEcho(t *testing.T) {
	addr := "127.0.0.1:9999"
	go ListenAndServeWithSignal(&Config{
		Address:   addr,
		ReusePort: true,
	}, MakeEchoHandler())
	time.Sleep(time.Second)

	counter := int32(100000)
	initCounter := counter
	concurrent := 64
	wg := &sync.WaitGroup{}
	beg := time.Now()
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Error(err)
				return
			}
			for atomic.AddInt32(&counter, -1) > 0 {
				_, err = conn.Write([]byte("a\n"))
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
				if string(line) != "a" {
					t.Error("wrong response: " + string(line))
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	elapsed := time.Now().Sub(beg)
	count := initCounter - counter
	qps := float64(count) / elapsed.Seconds()
	t.Logf("qps: %f", qps)
}
