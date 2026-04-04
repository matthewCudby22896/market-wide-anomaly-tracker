package replayengine

import (
	"context"
	"fmt"
	"os"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"
)

type TickerThread interface {
	LifeCycle
	AsynShutdown()
	GetTickPipe() chan<- int64
	SetOutbox(outbox chan<- BroadcastMessage)
}

type tickerThread struct {
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup
	Ticker    Symbol
	Date      civil.Date
	logger    ComponentLogger
	outbox    chan<- BroadcastMessage
	ticks     chan int64
}

func NewTickerThread(owner Hub, ticker Symbol, date civil.Date) *tickerThread {
	ctx, cancel := context.WithCancel(context.Background())

	return &tickerThread{
		Ctx:       ctx,
		CancelCtx: cancel,
		wg:        sync.WaitGroup{},
		logger:    NewLogger(fmt.Sprintf("%s TickerThread", ticker)),
		Ticker:    ticker,
		Date:      date,
		ticks:     make(chan int64),
		outbox:    nil, // Initialised by parent
	}
}

// TODO: Do I need both
func (t *tickerThread) Shutdown() {
	// Shutdown child components

	// Shutdown self
	t.CancelCtx()

	t.wg.Wait()
	t.logger.LogShutdown()
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

		for {
			select {
			case <-t.Ctx.Done():
				return

			case <-t.ticks:
				dummyMsg := BroadcastMessage{
					Ticker: t.Ticker,
					Data:   common.DummyOHLCBar(string(t.Ticker)),
				}

				t.outbox <- dummyMsg
			}
		}
	}()
}

func (t *tickerThread) GetTickPipe() chan<- int64 {
	return t.ticks
}

func (t *tickerThread) SetOutbox(outbox chan<- BroadcastMessage) {
	t.outbox = outbox
}
