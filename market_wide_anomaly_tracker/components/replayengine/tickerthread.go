package replayengine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/civil"
)

type TickerThread interface {
	LifeCycle
	AsynShutdown()
}

type tickerThread struct {
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup
	Ingester  BroadcastIngester
	Ticker    Ticker
	Date      civil.Date
	logger    ComponentLogger
}

func NewTickerThread(owner Hub, ticker Ticker, date civil.Date) *tickerThread {
	ctx, cancel := context.WithCancel(context.Background())

	return &tickerThread{
		Ctx:       ctx,
		CancelCtx: cancel,
		wg:        sync.WaitGroup{},
		Ingester:  owner,
		Ticker:    ticker,
		Date:      date,
		logger:    NewLogger(fmt.Sprintf("%s TickerThread", ticker)),
	}
}

// TODO: Do I need both
func (t *tickerThread) Shutdown() {
	// Shutdown child components

	// Shutdown self
	t.CancelCtx()

	t.wg.Wait()
	t.logger.Info("shutdown")
}

func (t *tickerThread) AsynShutdown() {
	go t.Shutdown()
}

func (t *tickerThread) Start() {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		ticker := time.NewTicker(1 * time.Second)

		defer ticker.Stop()

		for {
			select {
			case <-t.Ctx.Done():
				return

			case <-ticker.C:
				dummyMsg := BroadcastMessage{
					Ticker: t.Ticker,
					Data:   DummyAggregateBar(string(t.Ticker)),
				}

				t.Ingester.BroadcastMessagePipe() <- dummyMsg
			}
		}
	}()
}
