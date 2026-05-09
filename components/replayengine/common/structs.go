package common

type Bar struct {
	Symbol string  // e.g. "AAPL"
	T      int64   // timestamp
	O      float64 // open
	H      float64 // highest price
	L      float64 // lowest price
	C      float64 // close price
	N      int64   // no. transactions
	V      float64 // volume
	VW     float64 // volume weighted average price
}

type HydrationStatusRow struct {
	Symbol string
	Date   string
}
