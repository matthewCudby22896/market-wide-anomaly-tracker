package replayengine

import "github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"

const replayEnginerServerSocket = ":8080"

type Message struct {
	Action  string   `json:"action"`
	Symbols []string `json:"symbols"`
}

type SubscriptionRequest struct {
	Client  *client
	Symbols []common.Symbol
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
