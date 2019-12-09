package redis

type Client interface {
    Write([]byte) error

    // client should keep its subscribing channels
    SubsChannel(channel string)
    UnSubsChannel(channel string)
    SubsCount()int
    GetChannels()[]string
}
