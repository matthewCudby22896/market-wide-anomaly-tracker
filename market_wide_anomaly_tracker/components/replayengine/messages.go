package replayengine

import "cloud.google.com/go/civil"

type DataReadyMsg struct {
	ticker Ticker
	date  civil.Date
}