package config

import (
	"strings"
	"testing"
)

func init() {
	Properties = &ServerProperties{
		AppendOnly:        true,
		AppendFilename:    "appendonly.aof",
		AofUseRdbPreamble: false,
		MaxClients:        128,
	}
}

func TestParse(t *testing.T) {
	src := "bind 0.0.0.0\n" +
		"port 6399\n" +
		"appendonly yes\n" +
		"peers a,b"
	p := parse(strings.NewReader(src))
	if p == nil {
		t.Error("cannot get result")
		return
	}
	if p.Bind != "0.0.0.0" {
		t.Error("string parse failed")
	}
	if p.Port != 6399 {
		t.Error("int parse failed")
	}
	if !p.AppendOnly {
		t.Error("bool parse failed")
	}
	if len(p.Peers) != 2 || p.Peers[0] != "a" || p.Peers[1] != "b" {
		t.Error("list parse failed")
	}
}

func TestIsMutableConfig(t *testing.T) {
	if IsMutableConfig("databases") {
		t.Error("save is an immutable config")
	}
	if !IsMutableConfig("maxclients") {
		t.Error("maxclients is a mutable config")
	}
}

func TestCopyProperties(t *testing.T) {
	Properties.MaxClients = 127
	p := CopyProperties()
	if p.MaxClients != Properties.MaxClients {
		t.Error("no copy")
	}
}
