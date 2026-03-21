package replayengine

import (
	"context"
	"fmt"
	"os"
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
	Ticker    Ticker
	Date      civil.Date
	logger    ComponentLogger
	outbox    chan<- BroadcastMessage
}

func NewTickerThread(owner Hub, ticker Ticker, date civil.Date) *tickerThread {
	ctx, cancel := context.WithCancel(context.Background())

	return &tickerThread{
		Ctx:       ctx,
		CancelCtx: cancel,
		wg:        sync.WaitGroup{},
		logger:    NewLogger(fmt.Sprintf("%s TickerThread", ticker)),
		Ticker:    ticker,
		Date:      date,
		outbox:    nil, // Initialised by parent
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
	if t.outbox == nil {
		t.logger.Info("fatal : t.outbox was nil")
		os.Exit(1)
	}

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

				t.outbox <- dummyMsg
			}
		}
	}()
}
