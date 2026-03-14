package replayengine

import (
	"time"
)

type TickerThread struct {
	ingester BroadcastIngester
	Shutdown chan struct{}
	Ticker   Ticker
}

func (t *TickerThread) RunThread() {
	// 1. Initialize the ticker for 1-second intervals
	ticker := time.NewTicker(1 * time.Second)

	// 2. Always clean up the ticker when the function exits
	defer ticker.Stop()

	for {
		select {
		case <-t.Shutdown:
			// Exit immediately when signaled
			return

		case <-ticker.C:
			// 3. This block only triggers once every second
			dummyMsg := BroadcastMessage{
				Ticker: t.Ticker,
				Data:   DummyAggregateBar(string(t.Ticker)),
			}

			// Send to the broadcast channel
			// (Consider using a non-blocking send here if you have many listeners)
			t.ingester.BroadcastMessagePipe() <- dummyMsg
		}
	}
}
