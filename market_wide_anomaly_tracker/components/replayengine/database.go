package replayengine

type Database interface {
	IsReady(dateStr string, ticker Ticker) bool
}
