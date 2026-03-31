package replayengine

import (
	"context"
	"os"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/db"

)

type DataAvailability int

const (
	READY DataAvailability = iota
	HYDRATING
	NONE
)

type DataCoordinator interface {
	LifeCycle
	IsReady(t Ticker, d civil.Date) bool
	SetOutbox(chan any)
}

// dataCoordinator implements the DataCoordinater interface
type dataCoordinator struct {
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup
	logger    ComponentLogger

	database      db.Database
	massiveClient *massiveClient

	// Internal state
	statusMapMu sync.Mutex
	statusMap   map[string]map[Ticker]DataAvailability

	// Internal channels
	dataQueryChan chan dataQuery
	outbox        chan any
}

// Structs for internal use:
type dataQuery struct {
	ticker Ticker
	date   civil.Date
}

// Structs for external messaging
type hydrationSuccess struct {
	Ticker Ticker
	Date   civil.Date
}

type hydrationFailure struct {
	Ticker Ticker
	Date   civil.Date
}

func NewDataCoordinator(database db.Database) *dataCoordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &dataCoordinator{
		Ctx:           ctx,
		CancelCtx:     cancel,
		wg:            sync.WaitGroup{},
		logger:        NewLogger("DataCoordinator"),
		database:      database,
		massiveClient: NewMassiveClient(),
		statusMapMu:   sync.Mutex{},
		statusMap:     make(map[string]map[Ticker]DataAvailability),
		dataQueryChan: make(chan dataQuery, 1024),
		outbox:        nil, // Assigned post-hoc (by parent)
	}
}

func (c *dataCoordinator) Start() {
	c.logger.LogStartChild("MassiveClient")
	c.massiveClient.Start()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case query := <-c.dataQueryChan:
				state := c.getState(query.ticker, query.date.String())
				if !(state == READY || state == HYDRATING) {
					// Launch Hydration Task
					c.wg.Add(1)
					go c.HydrationTask(query.date, query.ticker)

					// Update status -> HYDRATING
					c.updateStatus(query.ticker, query.date.String(), HYDRATING)
				}

			case <-c.Ctx.Done():
				return
			default:
				continue
			}
		}
	}()
	c.logger.Info("started.")
}

func (c *dataCoordinator) Shutdown() {
	c.logger.LogShutdownChild("MassiveClient")
	c.massiveClient.Shutdown()

	c.CancelCtx()
	c.wg.Wait()
	c.logger.LogShutdown()
}

func (c *dataCoordinator) SetOutbox(outbox chan any) {
	if c.outbox != nil {
		c.logger.Info("SetOutbox() has been called twice")
	}
	c.outbox = outbox
}

func (c *dataCoordinator) HydrationTask(date civil.Date, ticker Ticker) {
	defer c.wg.Done()

	ctx := context.WithoutCancel(c.Ctx)

	bars, err := c.massiveClient.FetchDayData(c.Ctx, date, ticker)

	if err != nil {
		c.logger.Info("hydration task failed for %s", ticker)

		// Signal back to the Hub that the task failed
		c.outbox <- hydrationFailure{ticker, date}
	}

	err = c.database.BatchStoreBars(ctx, bars)
	if err != nil {
		c.logger.Errorf("failed to store fetch ohlc data for ticker: `%s`", ticker)
		os.Exit(1)
	}

	c.logger.Info("%d ohlc bars succesfully fetched", len(bars))

	// Update status
	c.updateStatus(ticker, date.String(), READY)

	// Signal to Hub that data is Ready
	c.outbox <- hydrationSuccess{ticker, date}

}

func (c *dataCoordinator) getState(ticker Ticker, date string) DataAvailability {
	c.statusMapMu.Lock()
	defer c.statusMapMu.Unlock()
	if _, ok := c.statusMap[date]; !ok {
		c.statusMap[date] = make(map[Ticker]DataAvailability)
	}
	state, ok := c.statusMap[date][ticker]
	if !ok {
		return NONE
	}
	return state
}

func (c *dataCoordinator) updateStatus(ticker Ticker, date string, status DataAvailability) {
	c.statusMapMu.Lock()
	defer c.statusMapMu.Unlock()
	if _, ok := c.statusMap[date]; !ok {
		c.statusMap[date] = make(map[Ticker]DataAvailability)
	}
	c.statusMap[date][ticker] = status
}

func (c *dataCoordinator) IsReady(t Ticker, d civil.Date) bool {
	state := c.getState(t, d.String())

	if state == READY {
		return true
	}

	c.dataQueryChan <- dataQuery{t, d}

	return false
}
