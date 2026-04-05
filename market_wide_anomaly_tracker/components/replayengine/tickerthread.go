package replayengine

import (
	"context"
	"fmt"
	"os"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/db"
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
	db        db.Database
}

func NewTickerThread(owner Hub, ticker Symbol, date civil.Date, db db.Database) *tickerThread {
	ctx, cancel := context.WithCancel(context.Background())

	return &tickerThread{
		Ctx:       ctx,
		CancelCtx: cancel,
		wg:        sync.WaitGroup{},
		logger:    NewLogger(fmt.Sprintf("%s TickerThread", ticker)),
		Ticker:    ticker,
		Date:      date,
		ticks:     make(chan int64),
		db:        db,
		outbox:    nil, // Initialised by parent
	}
}

// TODO: Do I need both
func (t *tickerThread) Shutdown() {
	// Shutdown child components

	// Shutdown self
	t.CancelCtx()

	t.logger.Info("before wg")
	t.wg.Wait()
	t.logger.Info("after wg")
	t.logger.LogShutdown()
}

func (t *tickerThread) AsynShutdown() {
	go t.Shutdown()
}

func (t *tickerThread) Start() {
	if t.outbox == nil {
		t.logger.Errorf("fatal : t.outbox was nil")
		os.Exit(1)
	}

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

		// Currently returns in DESC order
		series, err := t.db.GetCompleteTradingDay(t.Ctx, t.Date, string(t.Ticker))
		if err != nil {
			t.logger.Errorf("ticker failed to fetch data for symbol '%s': %s", t.Ticker, err)
			t.Shutdown()
			return
		}

		// Wait for a tick
		tick := <-t.ticks

		// Trim out-of-date bars
		for i := len(series) - 1; i >= 0; i-- {
			if series[i].T >= tick {
				series = series[:i+1]
				break
			}
		}

		for {
			select {
			case <-t.Ctx.Done():
				return

			case tick = <-t.ticks:
				// DEBUGGING:
				timestamp := common.UnixMilliToTimestampNYC(tick)
				t.logger.Info(timestamp)

				// Send all bars with T <= tick
				for len(series) > 0 && series[len(series)-1].T <= tick {
					msg := BroadcastMessage{
						t.Ticker,
						series[len(series)-1],
					}

					series = series[:len(series)-1]

					t.outbox <- msg
				}
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
