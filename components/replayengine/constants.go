package replayengine

import "github.com/mcudby/mwat/components/replayengine/common"

type SubscriptionRequest struct {
	Action  string   `json:"action"`  // "sub" or "unsub"
	Symbols []string `json:"symbols"` // e.g., ["QQQ", "SPY"]
}

type BroadcastMessage struct {
	Symbol common.Symbol
	Data   any
}

type LifeCycle interface {
	// 1. Starts the component's main loop
	Start()

	// 2. Signals to the component to stop and BLOCKS until finished.
	Shutdown()
}
