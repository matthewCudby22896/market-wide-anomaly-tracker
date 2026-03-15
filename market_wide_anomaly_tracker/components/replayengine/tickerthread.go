package replayengine

import (
	"context"
	"time"

	"cloud.google.com/go/civil"
)

type TickerThread interface {
	LifeCycle
	AsynShutdown()
}

type tickerThread struct {
	Ctx context.Context
	CancelCtx context.CancelFunc
	Ingester BroadcastIngester
	Ticker Ticker
	Date civil.Date
}

func NewTickerThread(owner Hub, ticker Ticker, date civil.Date) *tickerThread {
	ctx, cancel := context.WithCancel(context.Background())	

	return &tickerThread{
		Ctx: ctx,
		CancelCtx: cancel,
		Ingester: owner,
		Ticker: ticker,
		Date: date,
	}
}

// TODO: Do I need both
func (t *tickerThread) Shutdown() {
	// Shutdown child components

	// Shutdown self
	t.CancelCtx()
}

func (t *tickerThread) AsynShutdown() {
	// Trigger async shutdown of child components

	// Trigger shutdown of self
	t.CancelCtx()
}

func (t *tickerThread) Start() {
	ticker := time.NewTicker(1 * time.Second)

	defer ticker.Stop()

	for {
		select {
		case <-t.Ctx.Done():
			return

		case <-ticker.C:
			dummyMsg := BroadcastMessage,{
				Ticker: t.Ticker,
				Data:   DummyAggregateBar(string(t.Ticker)),
			}

			t.Ingester.BroadcastMessagePipe() <- dummyMsg
		}
	}
}
