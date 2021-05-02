package timewheel

import "time"

var tw = New(time.Second, 3600)

func init() {
	tw.Start()
}

func Delay(duration time.Duration, key string, job func()) {
	tw.AddTimer(duration, key, job)
}

func At(at time.Time, key string, job func()) {
	tw.AddTimer(at.Sub(time.Now()), key, job)
}

func Cancel(key string) {
	tw.RemoveTimer(key)
}
