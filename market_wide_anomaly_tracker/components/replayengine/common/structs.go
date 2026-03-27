package common

type OHLC struct {
	Symbol string  // e.g. "AAPL"
	VW     float64 // volume weighted average price
	C      float64 // close price
	H      float64 // highest price
	L      float64 // lowest price
	N      float64 // no. transactions
	O      float64 // open
	T      float64 // timestamp
	V      float64 // volume
}

func DummyOHLCBar(symbol string) OHLC {
	return OHLC{
		Symbol: symbol,
	}
}
