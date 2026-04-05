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
	IsReady(t Symbol, d civil.Date) bool
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
	statusMap   map[string]map[Symbol]DataAvailability

	// Internal channels
	dataQueryChan chan dataQuery
	outbox        chan any
}


// Structs for internal use:
type dataQuery struct {
	Symbol Symbol
	date   civil.Date
}

// Structs for external messaging
type hydrationSuccess struct {
	Symbol Symbol
	Date   civil.Date
}

type hydrationFailure struct {
	Symbol Symbol
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
		statusMap:     make(map[string]map[Symbol]DataAvailability),
		dataQueryChan: make(chan dataQuery, 1024),
		outbox:        nil, // Assigned post-hoc (by parent)
	}
}

func (c *dataCoordinator) Start() {
	// Init c.statusMap based of db state


	c.logger.LogStartChild("MassiveClient")
	c.massiveClient.Start()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case query := <-c.dataQueryChan:
				state := c.getState(query.Symbol, query.date.String())
				if !(state == READY || state == HYDRATING) {
					// Launch Hydration Task
					c.wg.Add(1)
					go c.HydrationTask(query.date, query.Symbol)

					// Update status -> HYDRATING
					c.updateStatus(query.Symbol, query.date.String(), HYDRATING)
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

func (c *dataCoordinator) HydrationTask(date civil.Date, symbol Symbol) {
	defer c.wg.Done()

	ctx := context.WithoutCancel(c.Ctx)

	bars, err := c.massiveClient.FetchDayData(c.Ctx, date, symbol)

	if err != nil {
		c.logger.Info("hydration task failed for %s: %v", symbol, err)

		// Signal back to the Hub that the task failed
		c.outbox <- hydrationFailure{symbol, date}
	}

	err = c.database.BatchStoreBars(ctx, bars)
	if err != nil {
		c.logger.Errorf("failed to store fetch ohlc data for ticker: `%s`: %v", symbol, err)
		os.Exit(1)
	}

	c.logger.Info("%d ohlc bars succesfully fetched", len(bars))

	// Update status
	c.updateStatus(symbol, date.String(), READY)

	// Signal to Hub that data is Ready
	c.outbox <- hydrationSuccess{symbol, date}
}

func (c *dataCoordinator) getState(ticker Symbol, date string) DataAvailability {
	c.statusMapMu.Lock()
	defer c.statusMapMu.Unlock()
	if _, ok := c.statusMap[date]; !ok {
		c.statusMap[date] = make(map[Symbol]DataAvailability)
	}
	state, ok := c.statusMap[date][ticker]
	if !ok {
		return NONE
	}
	return state
}

func (c *dataCoordinator) updateStatus(ticker Symbol, date string, status DataAvailability) {
	c.statusMapMu.Lock()
	defer c.statusMapMu.Unlock()
	if _, ok := c.statusMap[date]; !ok {
		c.statusMap[date] = make(map[Symbol]DataAvailability)
	}
	c.statusMap[date][ticker] = status
}

func (c *dataCoordinator) IsReady(t Symbol, d civil.Date) bool {
	state := c.getState(t, d.String())

	if state == READY {
		return true
	}

	c.dataQueryChan <- dataQuery{t, d}

	return false
}

/*
THOUGHTS

- Want to avoid refetching data and attempting to insert already present data

- Could add new hydration status table to the database

Process would be:
	On boot:
		- Load all fetched statuses from db and use it to init the status map

*/