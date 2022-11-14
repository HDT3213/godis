package pool

import (
	"errors"
	"testing"
	"time"
)

type mockConn struct {
	open bool
}

func TestPool(t *testing.T) {
	connNum := 0
	factory := func() (interface{}, error) {
		connNum++
		return &mockConn{
			open: true,
		}, nil
	}
	finalizer := func(x interface{}) {
		connNum--
		c := x.(*mockConn)
		c.open = false
	}
	cfg := Config{
		MaxIdle:   20,
		MaxActive: 40,
	}
	pool := New(factory, finalizer, cfg)
	var borrowed []*mockConn
	for i := 0; i < int(cfg.MaxActive); i++ {
		x, err := pool.Get()
		if err != nil {
			t.Error(err)
			return
		}
		c := x.(*mockConn)
		if !c.open {
			t.Error("conn is not open")
			return
		}
		borrowed = append(borrowed, c)
	}
	for _, c := range borrowed {
		pool.Put(c)
	}
	borrowed = nil
	// borrow returned
	for i := 0; i < int(cfg.MaxActive); i++ {
		x, err := pool.Get()
		if err != nil {
			t.Error(err)
			return
		}
		c := x.(*mockConn)
		if !c.open {
			t.Error("conn is not open")
			return
		}
		borrowed = append(borrowed, c)
	}
	for i, c := range borrowed {
		if i < len(borrowed)-1 {
			pool.Put(c)
		}
	}
	pool.Close()
	pool.Close() // test close twice
	pool.Put(borrowed[len(borrowed)-1])
	if connNum != 0 {
		t.Errorf("%d connections has not closed", connNum)
	}
	_, err := pool.Get()
	if err != ErrClosed {
		t.Error("expect err closed")
	}
}

func TestPool_Waiting(t *testing.T) {
	factory := func() (interface{}, error) {
		return &mockConn{
			open: true,
		}, nil
	}
	finalizer := func(x interface{}) {
		c := x.(*mockConn)
		c.open = false
	}
	cfg := Config{
		MaxIdle:   2,
		MaxActive: 4,
	}
	pool := New(factory, finalizer, cfg)
	var borrowed []*mockConn
	for i := 0; i < int(cfg.MaxActive); i++ {
		x, err := pool.Get()
		if err != nil {
			t.Error(err)
			return
		}
		c := x.(*mockConn)
		if !c.open {
			t.Error("conn is not open")
			return
		}
		borrowed = append(borrowed, c)
	}
	getResult := make(chan bool, 0)
	go func() {
		x, err := pool.Get()
		if err != nil {
			t.Error(err)
			getResult <- false
			return
		}
		c := x.(*mockConn)
		if !c.open {
			t.Error("conn is not open")
			getResult <- false
			return
		}
		getResult <- true
	}()
	time.Sleep(time.Second)
	pool.Put(borrowed[0])
	if ret := <-getResult; !ret {
		t.Error("get and waiting returned failed")
	}
}

func TestPool_CreateErr(t *testing.T) {
	makeErr := true
	factory := func() (interface{}, error) {
		if makeErr {
			makeErr = false
			return nil, errors.New("mock err")
		}
		return &mockConn{
			open: true,
		}, nil
	}
	finalizer := func(x interface{}) {
		c := x.(*mockConn)
		c.open = false
	}
	cfg := Config{
		MaxIdle:   2,
		MaxActive: 4,
	}
	pool := New(factory, finalizer, cfg)
	_, err := pool.Get()
	if err == nil {
		t.Error("expecting err")
		return
	}
	x, err := pool.Get()
	if err != nil {
		t.Error("get err")
		return
	}
	pool.Put(x)
	_, err = pool.Get()
	if err != nil {
		t.Error("get err")
		return
	}

}
