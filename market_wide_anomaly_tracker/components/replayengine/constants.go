package replayengine

const replayEnginerServerSocket = ":8080"

type Symbol string

type Message struct {
	Action  string   `json:"action"`
	Tickers []string `json:"tickers"`
}

type SubscriptionRequest struct {
	Client  *client
	Tickers []Symbol
}

type BroadcastMessage struct {
	Ticker Symbol
	Data   any
}

type LifeCycle interface {
	// 1. Starts the component's main loop
	Start()

	// 2. Signals to the component to stop and BLOCKS until finished.
	Shutdown()
}
