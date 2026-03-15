package replayengine

import "cloud.google.com/go/civil"

type dataReadyMsg struct {
	ticker Ticker
	date   civil.Date
}
