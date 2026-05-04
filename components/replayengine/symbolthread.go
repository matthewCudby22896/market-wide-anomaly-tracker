package replayengine

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/db"
)

const (
	bufferSize = 500
)

type SymbolThread interface {
	LifeCycle
	AsyncShutdown()
	SetOutbox(outbox chan<- BroadcastMessage)
	Restart()
}

type symbolThread struct {
	ID        string
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup
	symbol    common.Symbol
	Date      civil.Date
	logger    *Logger
	outbox    chan<- BroadcastMessage
	tickInbox <-chan int64
	db        *db.ReplayEngineDB
	getTime   func() int64
}

func NewSymbolThread(
	symbol common.Symbol,
	date civil.Date,
	db *db.ReplayEngineDB,
	outbox chan<- BroadcastMessage,
	tickInbox <-chan int64,

) *symbolThread {
	ctx, cancel := context.WithCancel(context.Background())

	id := fmt.Sprintf("%s-%s", symbol, date.String())

	return &symbolThread{
		ID:        id,
		Ctx:       ctx,
		CancelCtx: cancel,
		wg:        sync.WaitGroup{},
		logger:    NewComponentLogger(id),
		symbol:    symbol,
		Date:      date,
		tickInbox: tickInbox,
		db:        db,
		outbox:    outbox,
	}
}

func (t *symbolThread) Shutdown() {
	t.CancelCtx()
	t.wg.Wait()
	t.logger.LogShutdown()
}

func (t *symbolThread) AsyncShutdown() {
	go t.Shutdown()
}

func (t *symbolThread) Start() {
	if t.outbox == nil {
		t.logger.Fatal("failed to start symbol thread: t.outbox was nil")
	}

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

		t.logger.Info("in main loop")

		// Currently returns in DESC order
		series, err := t.db.GetSeries(t.Ctx, t.symbol, t.Date)
		if err != nil {
			t.logger.Error("failed to fetch data for symbol", "symbol", t.symbol, "error", err)
			t.Shutdown()
			return
		}

		t.logger.Info("before tick")

		// Wait for a tick
		tick := <-t.tickInbox

		t.logger.Info("after tick")

		// Trim out-of-date bars
		for i := len(series) - 1; i >= 0; i-- {
			// t.logger.Info("\n", "series[i].T", series[i].T, "tick", tick, "i", i)
			if series[i].T >= tick {
				series = series[:i+1]
				break
			}
		}

		for {
			select {
			case <-t.Ctx.Done():
				t.logger.Info("done")
				return

			case tick = <-t.tickInbox:
				t.logger.Info("received tick", "tick", tick)

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

func (t *symbolThread) Start2() {
	if t.outbox == nil {
		t.logger.Fatal("failed to start symbol thread: t.outbox was nil")
	}

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

		// buffer1 := make([]common.Bar, bufferSize)
		// buffer2 := make([]common.Bar, bufferSize)

		// Need to somehow get current time
	}()
}

func (t *symbolThread) SetOutbox(outbox chan<- BroadcastMessage) {
	t.outbox = outbox
}

func (t *symbolThread) Restart() {
	t.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())
	t.Ctx = ctx
	t.CancelCtx = cancel
	t.Start()
}
