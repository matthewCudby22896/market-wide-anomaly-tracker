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

type AggregateBar struct {
	Event   string  `json:"ev"` // Event Type (e.g., "AM")
	Symbol  string  `json:"sym"`
	Volume  int     `json:"v"`
	Open    float64 `json:"o"`
	Close   float64 `json:"c"`
	High    float64 `json:"h"`
	Low     float64 `json:"l"`
	VWAP    float64 `json:"a"` // Volume Weighted Average Price
	StartMS int64   `json:"s"` // Starting Unix Epoch (milliseconds)
	EndMS   int64   `json:"e"` // Ending Unix Epoch (milliseconds)
}

func DummyAggregateBar(ticker string) AggregateBar {
	bar := AggregateBar{
		Event:   "AM",
		Symbol:  ticker,
		Volume:  12345,
		Open:    150.85,
		High:    153.17,
		Low:     150.50,
		Close:   152.90,
		VWAP:    151.87,
		StartMS: 1611082800000,
		EndMS:   1611082860000,
	}

	return bar
}
