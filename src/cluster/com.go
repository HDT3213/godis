// communicate with peers within cluster
package cluster

import (
	"context"
	"errors"
	"github.com/HDT3213/godis/src/interface/redis"
	"github.com/HDT3213/godis/src/redis/client"
	"github.com/HDT3213/godis/src/redis/reply"
)

func (cluster *Cluster) getPeerClient(peer string) (*client.Client, error) {
	connectionFactory, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection factory not found")
	}
	raw, err := connectionFactory.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection factory make wrong type")
	}
	return conn, nil
}

func (cluster *Cluster) returnPeerClient(peer string, peerClient *client.Client) error {
	connectionFactory, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection factory not found")
	}
	return connectionFactory.ReturnObject(context.Background(), peerClient)
}

// relay command to peer
// cannot call Prepare, Commit, Rollback of self node
func (cluster *Cluster) Relay(peer string, c redis.Connection, args [][]byte) redis.Reply {
	if peer == cluster.self {
		// to self db
		return cluster.db.Exec(c, args)
	} else {
		peerClient, err := cluster.getPeerClient(peer)
		if err != nil {
			return reply.MakeErrReply(err.Error())
		}
		defer func() {
			_ = cluster.returnPeerClient(peer, peerClient)
		}()
		return peerClient.Send(args)
	}
}
