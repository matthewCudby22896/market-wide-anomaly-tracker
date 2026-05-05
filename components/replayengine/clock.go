package replayengine

import (
	"context"
	"sync"
	"sync/atomic"
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

	isPaused       *atomic.Bool
	startTime      int64 // Unix Milli
	simulationTime int64 // Unix Milli
	t              *time.Ticker

	isPausedChan chan bool
	subChan      chan chan<- int64
	unsubChan    chan chan<- int64
	subscribers  map[chan<- int64]struct{}

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

	startTime := common.NYSEOpenUnixMilli(config.Date)
	interval := time.Duration(float64(time.Second) / float64(config.Timescale)) // int64
	ticker := time.NewTicker(time.Duration(interval))

	isPaused := &atomic.Bool{}
	isPaused.Store(true)

	return &clock{
		ctx:       ctx,
		cancelCtx: cancel,
		wg:        sync.WaitGroup{},
		logger:    NewComponentLogger(clockID),

		isPaused:       isPaused,
		startTime:      startTime,
		simulationTime: startTime,
		t:              ticker,

		isPausedChan: make(chan bool, 1024),
		subChan:      make(chan chan<- int64, 1024),
		unsubChan:    make(chan chan<- int64, 1024),

		subscribers: make(map[chan<- int64]struct{}),

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
			// Main loop: This is the only thread where c.subscribers ought to
			// be modified.

			// Prioritise sub/unsub of subscribers
		SubLoop:
			for {
				select {
				case pipe := <-c.subChan:
					c.subscribers[pipe] = struct{}{}
				case pipe := <-c.unsubChan:
					delete(c.subscribers, pipe)
				default:
					break SubLoop
				}
			}

			// Then pause / unpause
		PauseLoop:
			for {
				select {
				case x := <-c.isPausedChan:
					if x == true {
						c.isPaused.Store(true)
					} else {
						c.isPaused.Store(false)
					}

				default:
					break PauseLoop
				}
			}

			select {
			case <-c.ctx.Done():
				return
			case <-c.t.C:
				ts := common.UnixMilliToTimestampNYC(c.simulationTime)
				select { // Non-blocking send
				case c.timestreamOutbox <- Tick{ts}:
				default:
				}

				// c.logger.Info(ts)

				if c.isPaused.Load() == true {
					continue
				}

				for pipe := range c.subscribers {
					// Non-blocking send
					select {
					case pipe <- c.simulationTime:
					default:
						// Do nothing
					}
				}

				c.simulationTime += 1000
			}
		}
	}()
	c.logger.LogStart()
}

func (c *clock) PullSettingsAndReset() {
	// Attaining this lock essentially pauses the clock
	config := c.getSimulationConfig()
	c.startTime = common.NYSEOpenUnixMilli(config.Date)
	interval := time.Duration(float64(time.Second) / float64(config.Timescale)) // int64
	c.t.Reset(time.Duration(interval))
}

func (c *clock) Shutdown() {
	c.cancelCtx()
	c.wg.Wait()
	c.logger.LogShutdown()
}

func (c *clock) RegisterPipe(pipe chan<- int64) {
	c.subChan <- pipe
}

func (c *clock) UnregisterPipe(pipe chan<- int64) {
	c.unsubChan <- pipe
}

func (c *clock) Pause() {
	c.isPausedChan <- true
}

func (c *clock) Resume() {
	c.isPausedChan <- false
}

func (c *clock) IsPaused() bool {
	c.logger.Info("IsPaused()")
	v := c.isPaused.Load()
	c.logger.Info("", "v", v)
	return v
}

func (c *clock) GetSimulationTime() int64 {
	// Need to be able to grab current simulation time
	// in thread-safe manner. This will be called by external
	// threads

	return 0
}
