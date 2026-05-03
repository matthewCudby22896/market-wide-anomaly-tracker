package replayengine

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"github.com/mcudby/mwat/components/replayengine/common"
)

const clockID = "clock"

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
	logger    *Logger
	clockSettings

	isPausedMu sync.Mutex
	isPaused   bool

	globalTime int64 // Unix Milli
	interval   time.Duration
	t          *time.Ticker

	subscribersMu sync.Mutex
	subscribers   map[chan<- int64]struct{}

	timestreamOutbox chan<- Tick
}

type clockSettings struct {
	startTime int64 // Unix Milli
	timescale float32
}

type Tick struct {
	Tick string `json:"tick"`
}

func NewClock(day civil.Date, speedup float32) *clock {
	ctx, cancel := context.WithCancel(context.Background())

	settings := clockSettings{
		startTime: common.NYSEOpenUnixMilli(day),
		timescale: speedup,
	}

	return &clock{
		Ctx:              ctx,
		CancelCtx:        cancel,
		wg:               sync.WaitGroup{},
		logger:           NewComponentLogger(clockID),
		clockSettings:    settings,
		isPaused:         true, // Init in paused state
		subscribers:      make(map[chan<- int64]struct{}),
		timestreamOutbox: nil, // Initialised post-hox, by parent
	}
}

func (c *clock) SetTicker() {
	c.interval = time.Duration(float64(time.Second) / float64(c.timescale)) // int64
	c.t = time.NewTicker(time.Duration(c.interval))
}

func (c *clock) Start() {
	if c.timestreamOutbox == nil {
		c.logger.Fatal("c.timeStreamOutbox was nil")
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		c.globalTime = c.clockSettings.startTime

		// Init Ticker
		c.SetTicker()

		for {
			select {
			case <-c.Ctx.Done():
				return
			case <-c.t.C:
				ts := common.UnixMilliToTimestampNYC(c.globalTime)
				select { // Non-blocking send
				case c.timestreamOutbox <- Tick{ts}:
				default:
				}

				c.isPausedMu.Lock()
				if c.isPaused {
					c.isPausedMu.Unlock()
					continue
				}
				c.globalTime += 1000
				c.isPausedMu.Unlock()

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
	c.logger.LogStart()
}

func (c *clock) ResetState() {
	// Attaining this lock essentially pauses the clock
	c.isPausedMu.Lock()
	defer c.isPausedMu.Unlock()

	c.SetTicker()
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

func (c *clock) UpdateClockSettings(timescale float32, date civil.Date) {
	c.clockSettings = clockSettings{
		startTime: common.NYSEOpenUnixMilli(date),
		timescale: timescale,
	}
}
