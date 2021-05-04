package cluster

import (
	"github.com/hdt3213/godis/config"
	"math/rand"
)

var testCluster = MakeTestCluster(nil)

func MakeTestCluster(peers []string) *Cluster {
	if config.Properties == nil {
		config.Properties = &config.PropertyHolder{}
	}
	config.Properties.Self = "127.0.0.1:6399"
	config.Properties.Peers = peers
	return MakeCluster()
}

func toArgs(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = []byte(s)
	}
	return args
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
