package replayengine

const replayEnginerServerSocket = ":8080"

type Ticker string

type Message struct {
	Action  string   `json:"action"`
	Tickers []string `json:"tickers"`
}

type SubscriptionRequest struct {
	Client  *client
	Tickers []Ticker
}

type BroadcastMessage struct {
	Ticker Ticker
	Data   any
}

type LifeCycle interface {
	// 1. Starts the component's main loop
	Start()

	// 2. Signals to the component to stop and BLOCKS until finished.
	Shutdown()
}
