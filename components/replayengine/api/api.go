package api

type SubscriptionRequest struct {
	Action string `json:"action"` // "sub" or "unsub"
	Params string `json:"params"` // e.g. "AM.AAPL, A.AAPL"
}

type FailedHydrationMsg struct {
	Msg    string `json:"msg"`
	Symbol string `json:"symbol"`
	Date   string `json:"date"`
}

type Bar struct {
	Symbol string  `json:"symbol"` // e.g. "AAPL"
	T      int64   `json:"t"`      // timestamp
	O      float64 `json:"o"`      // open
	H      float64 `json:"h"`      // highest price
	L      float64 `json:"l"`      // lowest price
	C      float64 `json:"c"`      // close price
	N      int64   `json:"n"`      // no. transactions
	V      float64 `json:"v"`      // volume
	VW     float64 `json:"vw"`     // volume weighted average price
}

type Tick struct {
	T string `json:"t"`
}

var StreamTypes = []string{
	"A",  // -> 1s
	"AM", // -> 1m
}

