package pool

import (
	"errors"
	"sync/atomic"
)

var (
	ErrClosed = errors.New("pool closed")
	ErrMax    = errors.New("reach max connection limit")
)

type Config struct {
	MaxIdle   int32
	MaxActive int32
}

// Pool stores object for reusing, such as redis connection
type Pool struct {
	Config
	factory     func() (interface{}, error)
	finalizer   func(x interface{})
	idles       chan interface{}
	activeCount int32 // increases during creating connection, decrease during destroying connection
	closed      atomic.Bool
}

func New(factory func() (interface{}, error), finalizer func(x interface{}), cfg Config) *Pool {
	return &Pool{
		factory:   factory,
		finalizer: finalizer,
		idles:     make(chan interface{}, cfg.MaxIdle),
		Config:    cfg,
	}
}

// getOnNoIdle try to create a new connection or waiting for connection being returned
// invoker should have pool.mu
func (pool *Pool) getOnNoIdle() (interface{}, error) {
	if atomic.LoadInt32(&pool.activeCount) >= pool.MaxActive {
		// waiting for connection being returned
		x, ok := <-pool.idles
		if !ok {
			return nil, ErrMax
		}
		return x, nil
	}
	// create a new connection
	atomic.AddInt32(&pool.activeCount, 1) // hold a place for new connection
	x, err := pool.factory()
	if err != nil {
		// create failed return token
		atomic.AddInt32(&pool.activeCount, -1) // release the holding place
		return nil, err
	}
	return x, nil
}

func (pool *Pool) Get() (interface{}, error) {
	if pool.closed.Load() {
		return nil, ErrClosed
	}

	select {
	case item := <-pool.idles:
		return item, nil
	default:
		// no pooled item, create one
		return pool.getOnNoIdle()
	}
}

func (pool *Pool) Put(x interface{}) {
	if pool.closed.Load() {
		pool.finalizer(x)
		return
	}

	select {
	case pool.idles <- x:
		return
	default:
		// reach max idle, destroy redundant item
		atomic.AddInt32(&pool.activeCount, -1)
		pool.finalizer(x)
	}
}

func (pool *Pool) Close() {
	if pool.closed.Load() {
		return
	}
	pool.closed.Store(true)
	close(pool.idles)

	for x := range pool.idles {
		pool.finalizer(x)
	}
}
