package replayengine

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"
)

type Clock interface {
	LifeCycle
	RegisterPipe(chan<- int64)
	Pause()
	Resume()
	IsPaused() bool
	ResetState()
	UpdateClockSettings(speedup float32, date civil.Date)
}

// TODO: think more carefully about the use of sync.Mutex here
// Can likely be simplified
type clock struct {
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup
	logger    ComponentLogger
	clockSettings

	isPausedMu sync.Mutex
	isPaused   bool

	globalTime int64 // Unix Milli

	subscribersMu sync.Mutex
	subscribers   map[chan<- int64]struct{}
}

type clockSettings struct {
	startTime int64 // Unix Milli
	speedup   float32
}

func NewClock(day civil.Date, speedup float32) *clock {
	ctx, cancel := context.WithCancel(context.Background())

	settings := clockSettings{
		startTime: common.NYSEOpenUnixMilli(day),
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

		c.globalTime = c.clockSettings.startTime

		// Init Ticker
		interval := time.Duration(float64(time.Second) / float64(c.speedup)) // int64
		t := time.NewTicker(time.Duration(interval))

		for {
			select {
			case <-c.Ctx.Done():
				return
			case <-t.C:
				c.isPausedMu.Lock()
				if c.isPaused {
					c.isPausedMu.Unlock()
					continue
				}
				c.globalTime += 1000
				c.isPausedMu.Unlock()

				// TODO: Remove
				c.logger.Info(common.UnixMilliToTimestampNYC(c.globalTime))

				c.subscribersMu.Lock()
				for pipe := range c.subscribers {
					// Non-blocking send
					select {
					case pipe <- c.globalTime:
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

func (c *clock) ResetState() {
	// Attaining this lock essentially pauses the clock
	c.isPausedMu.Lock()
	defer c.isPausedMu.Unlock()

	c.globalTime = c.clockSettings.startTime
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

func (c *clock) Pause() {
	c.isPausedMu.Lock()
	defer c.isPausedMu.Unlock()
	c.isPaused = true
}

func (c *clock) Resume() {
	c.isPausedMu.Lock()
	defer c.isPausedMu.Unlock()
	c.isPaused = false
}

func (c *clock) IsPaused() bool {
	c.isPausedMu.Lock()
	defer c.isPausedMu.Unlock()
	return c.isPaused
}

func (c *clock) UpdateClockSettings(speedup float32, date civil.Date) {
	c.clockSettings = clockSettings{
		startTime: common.NYSEOpenUnixMilli(date),
		speedup:   speedup,
	}
}
