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

// todo: possibly move to own file which defines external interface

type SubscriptionRequest struct {
	Action string `json:"action"` // "sub" or "unsub"
	Params string `json:"params"` // e.g. "AM.AAPL, A.AAPL"
}

type SubRequest struct {
	Sender  *WSClient
	Streams []common.Stream
}

type UnsubRequest struct {
	Sender  *WSClient
	Streams []common.Stream
}
