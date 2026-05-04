package replayengine

import (
	"context"
	"sync"
	"time"

	"github.com/mcudby/mwat/components/replayengine/common"
)

const clockID = "clock"

// todo: think more carefully about the use of sync.Mutex here
// Can likely be simplified
type clock struct {
	ctx       context.Context
	cancelCtx context.CancelFunc
	wg        sync.WaitGroup
	logger    *Logger

	isPausedMu sync.Mutex
	isPaused   bool

	startTime      int64 // Unix Milli
	simulationTime int64 // Unix Milli
	t              *time.Ticker

	subscribersMu sync.Mutex
	subscribers   map[chan<- int64]struct{}

	// i.o.
	timestreamOutbox    chan<- Tick
	getSimulationConfig func() SimulationConfig
}

type Tick struct {
	Tick string `json:"tick"`
}

func NewClock(
	timestreamOutbox chan<- Tick,
	getSimulationConfig func() SimulationConfig,
) *clock {
	ctx, cancel := context.WithCancel(context.Background())

	config := getSimulationConfig()

	startTime := common.NYSECloseUnixMilli(config.Date)
	interval := time.Duration(float64(time.Second) / float64(config.Timescale)) // int64
	ticker := time.NewTicker(time.Duration(interval))

	return &clock{
		ctx:       ctx,
		cancelCtx: cancel,
		wg:        sync.WaitGroup{},
		logger:    NewComponentLogger(clockID),

		isPausedMu: sync.Mutex{},
		isPaused:   true, // Init in paused state

		startTime:      startTime,
		simulationTime: startTime,
		t:              ticker,

		subscribersMu: sync.Mutex{},
		subscribers:   make(map[chan<- int64]struct{}),

		timestreamOutbox:    timestreamOutbox,
		getSimulationConfig: getSimulationConfig,
	}
}

func (c *clock) Start() {
	if c.timestreamOutbox == nil {
		c.logger.Fatal("c.timeStreamOutbox was nil")
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-c.t.C:
				ts := common.UnixMilliToTimestampNYC(c.simulationTime)
				select { // Non-blocking send
				case c.timestreamOutbox <- Tick{ts}:
				default:
				}

				c.isPausedMu.Lock()
				if c.isPaused {
					c.isPausedMu.Unlock()
					continue
				}
				c.isPausedMu.Unlock()

				c.subscribersMu.Lock()
				for pipe := range c.subscribers {
					// Non-blocking send
					select {
					case pipe <- c.simulationTime:
					default:
						// Do nothing
					}
				}
				c.subscribersMu.Unlock()

				c.simulationTime += 1000
			}
		}
	}()
	c.logger.LogStart()
}

func (c *clock) PullSettingsAndReset() {
	// Attaining this lock essentially pauses the clock
	c.isPausedMu.Lock()
	defer c.isPausedMu.Unlock()

	config := c.getSimulationConfig()
	c.startTime = common.NYSECloseUnixMilli(config.Date)
	interval := time.Duration(float64(time.Second) / float64(config.Timescale)) // int64
	c.t = time.NewTicker(time.Duration(interval))
}

func (c *clock) Shutdown() {
	c.cancelCtx()
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
