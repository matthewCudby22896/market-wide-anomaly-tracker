package replayengine

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/mcudby/mwat/components/replayengine/db"
)

const dataCoordinatorID = "data-coordinator"

type DataAvailability int

const (
	NONE DataAvailability = iota // state not previously tracked
	READY
	HYDRATING
	FAILED // indicates that hydration was attempted but failed
)

type DataCoordinator interface {
	LifeCycle
	GetID() string
	IsReady(t string, d civil.Date) bool
	SetOutbox(chan any)
	HydrateSymbol(ctx context.Context, symbol string, date civil.Date) error
}

// dataCoordinator implements the DataCoordinater interface
type dataCoordinator struct {
	ID        string
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup
	logger    *Logger

	database      *db.ReplayEngineDB
	massiveClient *massiveClient

	// Internal state
	statusMapMu sync.Mutex
	statusMap   map[string]map[string]DataAvailability

	// Internal channels
	dataQueryChan chan dataQuery
	outbox        chan<- any
}

// Structs for internal use:
type dataQuery struct {
	Symbol string
	date   civil.Date
}

// Structs for external messaging
type hydrationSuccess struct {
	Symbol string
	Date   civil.Date
}

type hydrationFailure struct {
	Symbol string
	Date   civil.Date
}

func NewDataCoordinator(database *db.ReplayEngineDB, outbox chan<- any) *dataCoordinator {
	ctx, cancel := context.WithCancel(context.Background())

	return &dataCoordinator{
		ID:            dataCoordinatorID,
		Ctx:           ctx,
		CancelCtx:     cancel,
		wg:            sync.WaitGroup{},
		logger:        NewComponentLogger(dataCoordinatorID),
		database:      database,
		massiveClient: NewMassiveClient(),
		statusMapMu:   sync.Mutex{},
		statusMap:     make(map[string]map[string]DataAvailability),
		dataQueryChan: make(chan dataQuery, 1024),
		outbox:        outbox,
	}
}

func (c *dataCoordinator) GetID() string { return c.ID }

func (c *dataCoordinator) Start() {
	c.InitStatusMap()

	c.logger.LogStartChild(c.massiveClient.ID)
	c.massiveClient.Start()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case query := <-c.dataQueryChan:
				state := c.getStateWithLock(query.Symbol, query.date.String())
				if !(state == READY || state == HYDRATING) {
					// Launch Hydration Task
					c.wg.Add(1)
					go c.HydrationTask(query.Symbol, query.date)

					// Update status -> HYDRATING
					c.setState(query.Symbol, query.date.String(), HYDRATING)
				}

			case <-c.Ctx.Done():
				return
			default:
				continue
			}
		}
	}()
	c.logger.LogStart()
}

func (c *dataCoordinator) InitStatusMap() {
	state, err := c.database.GetHydrationStatus(c.Ctx)
	if err != nil {
		c.logger.Error("failed to load hydration state", "error", err)
	}
	for _, x := range state {
		c.setState(x.Symbol, x.Date, READY)
	}
}

func (c *dataCoordinator) Shutdown() {
	c.logger.LogStopChild(c.massiveClient.ID)
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

var AlreadyHydratedErr error = errors.New("symbol is already hydrated")

var AlreadyHydratingErr error = errors.New("symbol is currently hydrating")

// HydrateSymbol fetches and stores the data series for a given symbol-date combination
func (c *dataCoordinator) HydrateSymbol(ctx context.Context, symbol string, date civil.Date) (err error) {
	dateStr := date.String()

	err = func() error {
		c.statusMapMu.Lock()
		defer c.statusMapMu.Unlock()
		state := c.getStateWOLock(symbol, dateStr)
		switch state {
		case READY:
			return AlreadyHydratedErr
		case HYDRATING:
			return AlreadyHydratingErr
		}
		c.statusMap[dateStr][symbol] = HYDRATING
		return nil
	}()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			c.setState(symbol, dateStr, FAILED)
		}
	}()

	bars, err := c.massiveClient.FetchDayData(c.Ctx, date, symbol)

	if err != nil {
		return fmt.Errorf("symbol hydration failed for `%s-%s`: %w", symbol, date, err)
	}

	err = c.database.InsertFullSession(ctx, bars, symbol, date.String())
	if err != nil {
		return fmt.Errorf("failed to store series for `%s-%s`: %w", symbol, date, err)
	}

	c.setState(symbol, dateStr, READY)
	return
}

func (c *dataCoordinator) HydrationTask(symbol string, date civil.Date) {
	defer c.wg.Done()

	ctx := context.WithoutCancel(c.Ctx)

	bars, err := c.massiveClient.FetchDayData(c.Ctx, date, symbol)

	if err != nil {
		c.logger.Info(
			"hydration task failed",
			"symbol", symbol,
			"date", date.String(),
			"error", err,
		)

		c.setState(symbol, date.String(), FAILED)

		// Signal back to the Hub that the task failed
		c.outbox <- hydrationFailure{symbol, date}
	}

	err = c.database.InsertFullSession(ctx, bars, symbol, date.String())
	if err != nil {
		c.logger.Fatal(
			"failed to store fetched ohlc bars",
			"symbol", symbol,
			"date", date.String(),
			"error", err,
		)
	}

	c.logger.Info("ohlc bars succesfully fetched", "num-bars", len(bars))

	// Update status
	c.setState(symbol, date.String(), READY)

	// Signal to Hub that data is Ready
	c.outbox <- hydrationSuccess{symbol, date}
}

func (c *dataCoordinator) getStateWithLock(symbol string, date string) DataAvailability {
	c.statusMapMu.Lock()
	defer c.statusMapMu.Unlock()
	if _, ok := c.statusMap[date]; !ok {
		c.statusMap[date] = make(map[string]DataAvailability)
	}
	state, ok := c.statusMap[date][symbol]
	if !ok {
		return NONE
	}
	return state
}

func (c *dataCoordinator) getStateWOLock(symbol string, date string) DataAvailability {
	if _, ok := c.statusMap[date]; !ok {
		c.statusMap[date] = make(map[string]DataAvailability)
	}
	state, ok := c.statusMap[date][symbol]
	if !ok {
		return NONE
	}
	return state
}

func (c *dataCoordinator) setState(ticker string, date string, status DataAvailability) {
	c.statusMapMu.Lock()
	defer c.statusMapMu.Unlock()
	if _, ok := c.statusMap[date]; !ok {
		c.statusMap[date] = make(map[string]DataAvailability)
	}
	c.statusMap[date][ticker] = status
}

func (c *dataCoordinator) IsReady(t string, d civil.Date) bool {
	state := c.getStateWithLock(t, d.String())

	if state == READY {
		return true
	}

	c.dataQueryChan <- dataQuery{t, d}

	return false
}
