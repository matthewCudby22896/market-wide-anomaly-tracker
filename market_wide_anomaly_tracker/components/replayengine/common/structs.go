package common

// CREATE TABLE ohlc_bars (
//     symbol TEXT,
//     t      BIGINT,
//     o      REAL,
//     h      REAL,
//     l      REAL,
//     c      REAL,
//     v      REAL,
//     vw     REAL,
//     PRIMARY KEY (ticker, t)
// );

type OHLC struct {
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

func DummyOHLCBar(symbol string) OHLC {
	return OHLC{
		Symbol: symbol,
	}
}

type AggBars []OHLC

func (a AggBars) ToRows() [][]any {
	matrix := make([][]any, len(a))

	for i := range matrix {
		matrix[i] = []any{
			a[i].Symbol,
			a[i].T,
			a[i].O,
			a[i].H,
			a[i].L,
			a[i].C,
			a[i].N,
			a[i].V,
			a[i].VW,
		}
	}

	return matrix
}

func (a AggBars) ColNames() []string {
	return []string{
		"symbol",
		"t",
		"o",
		"h",
		"l",
		"c",
		"n",
		"v",
		"vw",
	}
}
