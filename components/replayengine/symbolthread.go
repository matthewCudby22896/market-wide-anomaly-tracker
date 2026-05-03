package replayengine

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/db"
)

type SymbolThread interface {
	LifeCycle
	AsyncShutdown()
	GetTickPipe() chan<- int64
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
	ticks     chan int64
	db        db.ReplayEngineDB
}

func NewSymbolThread(owner Hub, symbol common.Symbol, date civil.Date, db db.ReplayEngineDB) *symbolThread {
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
		ticks:     make(chan int64),
		db:        db,
		outbox:    nil, // Initialised post-hoc, by parent
	}
}

func (t *symbolThread) Shutdown() {
	// Shutdown self
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

		// Currently returns in DESC order
		series, err := t.db.GetSeries(t.Ctx, t.symbol, t.Date)
		if err != nil {
			t.logger.Error("failed to fetch data for symbol", "symbol", t.symbol, "error", err)
			t.Shutdown()
			return
		}

		// Wait for a tick
		tick := <-t.ticks

		// todo: check logic / fix
		// Trim out-of-date bars
		// for i := len(series) - 1; i >= 0; i-- {
		// 	if series[i].T >= tick {
		// 		series = series[:i+1]
		// 		break
		// 	}
		// }

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

func (t *symbolThread) Restart() {
	t.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())
	t.Ctx = ctx
	t.CancelCtx = cancel
	t.Start()
}
