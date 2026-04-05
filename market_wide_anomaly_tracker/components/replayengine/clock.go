package replayengine

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/civil"
)

type Clock interface {
	LifeCycle
	RegisterPipe(chan<- int64)
}

type clock struct {
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup
	logger    ComponentLogger
	clockSettings
	isPaused bool

	subscribersMu sync.Mutex
	subscribers   map[chan<- int64]struct{}
}

type clockSettings struct {
	startTime time.Time
	speedup   float32
}

func defaultStartTime(day civil.Date) time.Time {
	location, _ := time.LoadLocation("America/New_York")

	return time.Date(
		day.Year,
		day.Month,
		day.Day,
		9, 30, 0, 0, // 9:30:00.000000
		location,
	)
}

func NewClock(day civil.Date, speedup float32) *clock {
	ctx, cancel := context.WithCancel(context.Background())

	settings := clockSettings{
		startTime: defaultStartTime(day),
		speedup:   speedup,
	}

	return &clock{
		Ctx:           ctx,
		CancelCtx:     cancel,
		wg:            sync.WaitGroup{},
		logger:        NewLogger("Clock"),
		clockSettings: settings,
		isPaused:      false, // Init as un-paused for now
		subscribers:   make(map[chan<- int64]struct{}),
	}
}

func (c *clock) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		globalTime := c.clockSettings.startTime.UnixMilli()

		// Init Ticker
		interval := time.Duration(float64(time.Second) / float64(c.speedup)) // int64
		t := time.NewTicker(time.Duration(interval))

		for {
			select {
			case <-c.Ctx.Done():
				return
			case <-t.C:
				if c.isPaused {
					continue
				}

				// 1 Sec (1000 Millisecond)
				globalTime += 1000

				c.subscribersMu.Lock()
				for pipe := range c.subscribers {
					// Non-blocking send
					select {
					case pipe <- globalTime:
					default:
						// Do nothing
					}
				}
				c.subscribersMu.Unlock()
			}
		}
	}()
	c.logger.Info("started.")
}

func (c *clock) Shutdown() {
	c.CancelCtx()
	c.wg.Wait()
	c.logger.LogShutdown()
}

func (c *clock) RegisterPipe(pipe chan<- int64) {
	c.subscribersMu.Lock()
	c.subscribers[pipe] = struct{}{}
	c.subscribersMu.Unlock()
}
