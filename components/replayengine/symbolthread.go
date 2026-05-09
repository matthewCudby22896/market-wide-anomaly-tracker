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
	id        string
	ctx       context.Context
	cancelCtx context.CancelFunc
	wg        sync.WaitGroup
	logger    *Logger

	symbol string
	date   civil.Date

	tickInbox <-chan int64
	outbox    chan<- BroadcastMessage

	db                *db.ReplayEngineDB
	getSimulationTime func() int64
}

func NewSymbolThread(
	symbol string,
	date civil.Date,
	db *db.ReplayEngineDB,
	outbox chan<- BroadcastMessage,
	tickInbox <-chan int64,
	getSimulationTime func() int64,
) *symbolThread {
	ctx, cancel := context.WithCancel(context.Background())

	id := fmt.Sprintf("%s-%s", symbol, date.String())

	return &symbolThread{
		id:        id,
		ctx:       ctx,
		cancelCtx: cancel,
		wg:        sync.WaitGroup{},
		logger:    NewComponentLogger(id),

		symbol: symbol,
		date:   date,

		tickInbox: tickInbox,
		outbox:    outbox,

		db:                db,
		getSimulationTime: getSimulationTime,
	}
}

func (t *symbolThread) Shutdown() {
	t.cancelCtx()
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

		marketClose := common.NYSECloseUnixMilli(t.date)

		buffer1 := make([]common.Bar, bufferSize)
		buffer2 := make([]common.Bar, bufferSize)

		// bufferA points to the buffer we are currently reading from
		bufferA := &buffer1

		// bufferB points to the buffer we are currently writing to
		bufferB := &buffer2

		t1 := t.getSimulationTime()
		t2 := t1 + 1000*bufferSize

		// populate bufferA before beginning
		n, err := t.db.GetSeries(
			t.ctx,
			t.symbol,
			t.date.String(),
			t1,
			t2,
			*bufferA,
		)
		if err != nil {
			t.logger.Error("failed to populate initial buffer", "err", err)
		}
		if n == 0 {
			t.logger.Error("failed to populate initial buffer", "num_bars", n)
		}

		// begin async populating bufferB
		t1 = t2
		t2 = t1 + 1000*bufferSize
		bufferBReady := t.asyncPopulateBuffer(bufferB, t1, t2)

		i := 0
		for {
			select {
			case <-t.ctx.Done():
				return

			case tick := <-t.tickInbox:
				// whilst bar occured before current tick, send it
				for i < len(*bufferA) && (*bufferA)[i].T <= tick {
					t.outbox <- BroadcastMessage{
						(*bufferA)[i].Symbol,
						(*bufferA)[i],
					}
					i += 1
				}
			}

			if i == len(*bufferA) { // now at end of buffer
				err := <-bufferBReady
				if err != nil {
					t.logger.Error("error occured whilst populating bufferB", "err", err)
				}

				// swap
				tmp := bufferA
				bufferA = bufferB
				bufferB := tmp

				t1 = t2
				t2 = t1 + 1000*bufferSize

				if t1 > marketClose {
					t.logger.Error("reached market close", "t1", common.UnixMilliToTimestampNYC(t1))
				}

				// begin async populating the new bufferB
				bufferBReady = t.asyncPopulateBuffer(bufferB, t1, t2)

				// reset i to point to begginning of new bufferA
				i = 0
			}
		}
	}()
	t.logger.LogStart()
}

// asyncPopulateBuffer fetches bars in the range [t1, t2) and loads them into the provided buffer.
// It returns a receive-only channel that transmits a single nil (or error) upon completion
// Note: The caller must not access 'buffer' until the channel signals completion to avoid data races.
func (t *symbolThread) asyncPopulateBuffer(buffer *[]common.Bar, t1, t2 int64) chan error {
	done := make(chan error, 1)
	go func() {
		// note: you can still receive from a close chan
		defer close(done)

		*buffer = (*buffer)[:cap(*buffer)]
		n, err := t.db.GetSeries(t.ctx, t.symbol, t.date.String(), t1, t2, *buffer)

		// update the len to indicate no. actual, non-stale bars in the buffer
		fmt.Println("before!")
		*buffer = (*buffer)[:n]
		fmt.Println("after!")

		done <- err
	}()

	return done
}

func (t *symbolThread) Restart() {
	t.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())
	t.ctx = ctx
	t.cancelCtx = cancel

tag:
	for {
		select{
		case <-t.tickInbox:
		default:
			break tag
		}
	}


	t.Start()
}
