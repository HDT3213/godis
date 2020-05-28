package db

import (
    "github.com/HDT3213/godis/src/datastruct/list"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/redis/reply"
    "strconv"
)

var (
    _subscribe = "subscribe"
    _unsubscribe = "unsubscribe"
    messageBytes = []byte("message")
    unSubscribeNothing = []byte("*3\r\n$11\r\nunsubscribe\r\n$-1\n:0\r\n")
)

func makeMsg(t string, channel string, code int64)[]byte {
    return []byte("*3\r\n$" + strconv.FormatInt(int64(len(t)), 10) + reply.CRLF + t + reply.CRLF +
        "$" + strconv.FormatInt(int64(len(channel)), 10) + reply.CRLF + channel + reply.CRLF +
        ":" + strconv.FormatInt(code, 10) + reply.CRLF)
}

/*
 * invoker should lock channel
 * return: is new subscribed
 */
func subscribe0(db *DB, channel string, client redis.Client)bool {
    client.SubsChannel(channel)

    // add into db.subs
    raw, ok := db.subs.Get(channel)
    var subscribers *list.LinkedList
    if ok {
        subscribers, _ = raw.(*list.LinkedList)
    } else {
        subscribers = list.Make()
        db.subs.Put(channel, subscribers)
    }
    if subscribers.Contains(client) {
        return false
    }
    subscribers.Add(client)
    return true
}

/*
 * invoker should lock channel
 * return: is actually un-subscribe
 */
func unsubscribe0(db *DB, channel string, client redis.Client)bool {
    client.UnSubsChannel(channel)

    // remove from db.subs
    raw, ok := db.subs.Get(channel)
    if ok {
        subscribers, _ := raw.(*list.LinkedList)
        subscribers.RemoveAllByVal(client)

        if subscribers.Len() == 0 {
            // clean
            db.subs.Remove(channel)
        }
        return true
    }
    return false
}

func Subscribe(db *DB, c redis.Client, args [][]byte)redis.Reply {
    channels := make([]string, len(args))
    for i, b := range args {
        channels[i] = string(b)
    }

    db.subsLocker.Locks(channels...)
    defer db.subsLocker.UnLocks(channels...)

    for _, channel := range channels {
        if subscribe0(db, channel, c) {
            _ = c.Write(makeMsg(_subscribe, channel, int64(c.SubsCount())))
        }
    }
    return &reply.NoReply{}
}

func unsubscribeAll(db *DB, c redis.Client) {
    channels := c.GetChannels()

    db.subsLocker.Locks(channels...)
    defer db.subsLocker.UnLocks(channels...)

    for _, channel := range channels {
        unsubscribe0(db, channel, c)
    }

}

func UnSubscribe(db *DB, c redis.Client, args [][]byte)redis.Reply {
    var channels []string
    if len(args) > 0 {
        channels = make([]string, len(args))
        for i, b := range args {
            channels[i] = string(b)
        }
    } else {
        channels = c.GetChannels()
    }

    db.subsLocker.Locks(channels...)
    defer db.subsLocker.UnLocks(channels...)

    if len(channels) == 0 {
        _ = c.Write(unSubscribeNothing)
        return &reply.NoReply{}
    }

    for _, channel := range channels {
        if unsubscribe0(db, channel, c) {
            _ = c.Write(makeMsg(_unsubscribe, channel, int64(c.SubsCount())))
        }
    }
    return &reply.NoReply{}
}

func Publish(db *DB, args [][]byte) redis.Reply {
    if len(args) != 2 {
        return &reply.ArgNumErrReply{Cmd: "publish"}
    }
    channel := string(args[0])
    message := args[1]

    db.subsLocker.Lock(channel)
    defer db.subsLocker.UnLock(channel)

    raw, ok := db.subs.Get(channel)
    if !ok {
        return reply.MakeIntReply(0)
    }
    subscribers, _ := raw.(*list.LinkedList)
    subscribers.ForEach(func(i int, c interface{}) bool {
        client, _ := c.(redis.Client)
        replyArgs := make([][]byte, 3)
        replyArgs[0] = messageBytes
        replyArgs[1] = []byte(channel)
        replyArgs[2] = message
        _ = client.Write(reply.MakeMultiBulkReply(replyArgs).ToBytes())
        return true
    })
    return reply.MakeIntReply(int64(subscribers.Len()))
}

