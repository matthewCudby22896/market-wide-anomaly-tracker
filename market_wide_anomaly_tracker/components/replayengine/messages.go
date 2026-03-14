package replayengine

import "cloud.google.com/go/civil"

type DataReadyMsg {
	ticker Ticker
	date  civil.Data
}