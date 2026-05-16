package common

import (
	"fmt"
	"strings"

	"cloud.google.com/go/civil"
)

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

type SimulationConfig struct {
	Timescale float32
	Date      civil.Date
}

type BroadcastMessage struct {
	StreamID string
	Payload  any
}


type Stream struct {
	Type   string // A or AM
	Symbol string // e.g. AAPL
}

func (s Stream) ID() string {
	return fmt.Sprintf("%s.%s", s.Type, s.Symbol)
}

// todo: validation
func StreamFromID(id string) Stream {
	X := strings.Split(id, ".")
	return Stream{X[0], X[1]}
}

var StreamTypes = []string{
	"A",  // -> 1s
	"AM", // -> 1m
}
