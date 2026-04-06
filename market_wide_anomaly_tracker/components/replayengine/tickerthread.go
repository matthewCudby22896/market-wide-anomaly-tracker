package replayengine

import (
	"context"
	"fmt"
	"os"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/db"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"
)

type SymbolThread interface {
	LifeCycle
	AsynShutdown()
	GetTickPipe() chan<- int64
	SetOutbox(outbox chan<- BroadcastMessage)
}

type symbolThread struct {
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup
	symbol    common.Symbol
	Date      civil.Date
	logger    ComponentLogger
	outbox    chan<- BroadcastMessage
	ticks     chan int64
	db        db.Database
}

func NewSymbolThread(owner Hub, symbol common.Symbol, date civil.Date, db db.Database) *symbolThread {
	ctx, cancel := context.WithCancel(context.Background())

	return &symbolThread{
		Ctx:       ctx,
		CancelCtx: cancel,
		wg:        sync.WaitGroup{},
		logger:    NewLogger(fmt.Sprintf("%s symbolThread", symbol)),
		symbol:    symbol,
		Date:      date,
		ticks:     make(chan int64),
		db:        db,
		outbox:    nil, // Initialised by parent
	}
}

// TODO: Do I need both
func (t *symbolThread) Shutdown() {
	// Shutdown child components

	// Shutdown self
	t.CancelCtx()

	t.logger.Info("before wg")
	t.wg.Wait()
	t.logger.Info("after wg")
	t.logger.LogShutdown()
}

func (t *symbolThread) AsynShutdown() {
	go t.Shutdown()
}

func (t *symbolThread) Start() {
	if t.outbox == nil {
		t.logger.Errorf("fatal : t.outbox was nil")
		os.Exit(1)
	}

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

		// Currently returns in DESC order
		series, err := t.db.GetCompleteTradingDay(t.Ctx, t.Date, string(t.symbol))
		if err != nil {
			t.logger.Errorf("symbol failed to fetch data for symbol '%s': %s", t.symbol, err)
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
				// Send all bars that occured before the tick
				for len(series) > 0 && series[len(series)-1].T <= tick {
					msg := BroadcastMessage{
						t.symbol,
						series[len(series)-1],
					}

					series = series[:len(series)-1]

					t.outbox <- msg
				}
			}
		}
	}()
}

func (t *symbolThread) GetTickPipe() chan<- int64 {
	return t.ticks
}

func (t *symbolThread) SetOutbox(outbox chan<- BroadcastMessage) {
	t.outbox = outbox
}
