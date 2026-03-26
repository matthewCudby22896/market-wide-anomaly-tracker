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

type OHLC struct {
	symbol string  // e.g. "AAPL"
	vw     float64 // volume weighted average price
	c      float64 // close price
	h      float64 // highest price
	l      float64 // lowest price
	n      float64 // no. transactions
	o      float64 // open
	t      float64 // timestamp
	v      float64 // volume
}

func DummyOHLCBar(symbol string) OHLC {
	return OHLC{
		symbol: symbol,
	}
}
