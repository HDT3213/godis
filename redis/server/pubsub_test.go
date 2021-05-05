package server

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/pubsub"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/reply/asserts"
	"testing"
)

func TestPublish(t *testing.T) {
	hub := pubsub.MakeHub()
	channel := utils.RandString(5)
	msg := utils.RandString(5)
	conn := &connection.FakeConn{}
	pubsub.Subscribe(hub, conn, utils.ToBytesList(channel))
	conn.Clean() // clean subscribe success
	pubsub.Publish(hub, utils.ToBytesList(channel, msg))
	data := conn.Bytes()
	ret, err := parser.ParseOne(data)
	if err != nil {
		t.Error(err)
		return
	}
	asserts.AssertMultiBulkReply(t, ret, []string{
		"message",
		channel,
		msg,
	})

	// unsubscribe
	pubsub.UnSubscribe(hub, conn, utils.ToBytesList(channel))
	conn.Clean()
	pubsub.Publish(hub, utils.ToBytesList(channel, msg))
	data = conn.Bytes()
	if len(data) > 0 {
		t.Error("expect no msg")
	}

	// unsubscribe all
	pubsub.Subscribe(hub, conn, utils.ToBytesList(channel))
	pubsub.UnSubscribe(hub, conn, utils.ToBytesList())
	conn.Clean()
	pubsub.Publish(hub, utils.ToBytesList(channel, msg))
	data = conn.Bytes()
	if len(data) > 0 {
		t.Error("expect no msg")
	}
}
