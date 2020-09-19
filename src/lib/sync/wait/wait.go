package wait

import (
    "sync"
    "time"
)

type Wait struct {
    wg sync.WaitGroup
}

func (w *Wait)Add(delta int) {
    w.wg.Add(delta)
}

func (w *Wait)Done() {
    w.wg.Done()
}

func (w *Wait)Wait() {
    w.wg.Wait()
}

// return isTimeout
func (w *Wait)WaitWithTimeout(timeout time.Duration)bool {
    c := make(chan bool)
    go func() {
        defer close(c)
        w.wg.Wait()
        c <- true
    }()
    select {
    case <-c:
        return false // completed normally
    case <-time.After(timeout):
        return true // timed out
    }
}