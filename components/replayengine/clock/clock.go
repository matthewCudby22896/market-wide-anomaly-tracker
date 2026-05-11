package clock

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/logging"
)

const clockID = "clock"

type clock struct {
	ctx       context.Context
	cancelCtx context.CancelFunc
	wg        sync.WaitGroup
	logger    *logging.Logger

	isPaused       *atomic.Bool
	startTime      int64         // Unix Milli
	simulationTime *atomic.Int64 // Unix Milli
	t              *time.Ticker

	triggerRestartChan chan any
	restartDone        chan any
	isPausedChan       chan bool
	subChan            chan chan<- int64
	unsubChan          chan chan<- int64
	subscribers        map[chan<- int64]struct{}

	// i.o.
	timestreamOutbox    chan<- Tick
	getSimulationConfig func() common.SimulationConfig
}

func NewClock(
	timestreamOutbox chan<- Tick,
	getSimulationConfig func() common.SimulationConfig,
) *clock {
	ctx, cancel := context.WithCancel(context.Background())

	config := getSimulationConfig()

	startTime := common.NYSEOpenUnixMilli(config.Date)
	interval := time.Duration(float64(time.Second) / float64(config.Timescale)) // int64
	ticker := time.NewTicker(time.Duration(interval))

	isPaused := &atomic.Bool{}
	isPaused.Store(true)

	simulationTime := &atomic.Int64{}
	simulationTime.Store(startTime)

	return &clock{
		ctx:       ctx,
		cancelCtx: cancel,
		wg:        sync.WaitGroup{},
		logger:    logging.NewComponentLogger(clockID),

		isPaused:       isPaused,
		startTime:      startTime,
		simulationTime: simulationTime,
		t:              ticker,

		triggerRestartChan: make(chan any, 1024),
		restartDone:        make(chan any, 1024),
		isPausedChan:       make(chan bool, 1024),
		subChan:            make(chan chan<- int64, 1024),
		unsubChan:          make(chan chan<- int64, 1024),

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
				case ch := <-c.subChan:
					c.subscribers[ch] = struct{}{}
				case ch := <-c.unsubChan:
					delete(c.subscribers, ch)
					close(ch)
				default:
					break SubLoop
				}
			}

			// Then pause / unpause / restart
		PauseLoop:
			for {
				select {
				case <-c.triggerRestartChan:
					c.isPaused.Store(true)
					c.pullSettingsAndReset()
					c.restartDone <- nil

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
				simulationTime := c.simulationTime.Load()

				ts := common.UnixMilliToTimestampNYC(simulationTime)
				select { // Non-blocking send
				case c.timestreamOutbox <- Tick{ts}:
				default:
				}

				if c.isPaused.Load() == true {
					continue
				}

				for pipe := range c.subscribers {
					// Non-blocking send
					select {
					case pipe <- simulationTime:
					default:
						// Do nothing
					}
				}

				c.simulationTime.Store(simulationTime + 1000)
			}
		}
	}()
	c.logger.LogStart()
}

func (c *clock) pullSettingsAndReset() {
	config := c.getSimulationConfig()
	c.startTime = common.NYSEOpenUnixMilli(config.Date)
	interval := time.Duration(float64(time.Second) / float64(config.Timescale)) // int64
	c.simulationTime.Store(c.startTime)
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

func (c *clock) Restart() {
	c.triggerRestartChan <- nil
	<-c.restartDone
}

func (c *clock) Resume() {
	c.isPausedChan <- false
}

func (c *clock) IsPaused() bool {
	return c.isPaused.Load()
}

func (c *clock) GetSimulationTime() int64 {
	return c.simulationTime.Load()
}
