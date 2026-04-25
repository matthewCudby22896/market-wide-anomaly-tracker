package replayengine

import "github.com/mcudby/mwat/components/replayengine/common"


type Message struct {
	Action  string   `json:"action"`
	Symbols []string `json:"symbols"`
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
