package wsclient

import (
	"github.com/mcudby/mwat/components/replayengine/common"
)

type UnregisterRequest struct {
	Sender *WSClient
}

type RegisterRequest struct {
	Sender *WSClient
}

type SubRequest struct {
	Sender  *WSClient
	Streams []common.Stream
}

type UnsubRequest struct {
	Sender  *WSClient
	Streams []common.Stream
}
