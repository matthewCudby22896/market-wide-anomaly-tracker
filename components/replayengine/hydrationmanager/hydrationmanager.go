package hydrationmanager

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/db"
	massive "github.com/mcudby/mwat/components/replayengine/hydrationmanager/massiveclient"
	"github.com/mcudby/mwat/components/replayengine/logging"
)

const dataCoordinatorID = "data-coordinator"

type DataFetcher interface {
	FetchTradingSession(ctx context.Context, day civil.Date, symbol string) ([]common.Bar, error)
	GetID() string
	Start()
	Shutdown()
}

type hydrationMgr struct {
	ID        string
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup
	logger    *logging.Logger

	database *db.ReplayEngineDB
	fetcher  DataFetcher

	// Internal state
	statusMapMu sync.Mutex
	statusMap   map[string]map[string]hydrationStatus

	// Channels
	dataQueryChan chan dataQuery
	outbox        chan<- any
}

func NewHydrationMgr(database *db.ReplayEngineDB, outbox chan<- any) *hydrationMgr {
	ctx, cancel := context.WithCancel(context.Background())

	return &hydrationMgr{
		ID:            dataCoordinatorID,
		Ctx:           ctx,
		CancelCtx:     cancel,
		wg:            sync.WaitGroup{},
		logger:        logging.NewComponentLogger(dataCoordinatorID),
		database:      database,
		fetcher:       massive.NewMassiveClient(),
		statusMapMu:   sync.Mutex{},
		statusMap:     make(map[string]map[string]hydrationStatus),
		dataQueryChan: make(chan dataQuery, 1024),
		outbox:        outbox,
	}
}

func (c *hydrationMgr) GetID() string { return c.ID }

func (c *hydrationMgr) Start() {
	c.initStatusMap()

	c.logger.LogStartChild(c.fetcher.GetID())
	c.fetcher.Start()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case query := <-c.dataQueryChan:
				state := c.getStateWithLock(query.Symbol, query.date.String())
				if !(state == Ready || state == Hydrating) {
					// Launch Hydration Task
					c.wg.Add(1)
					go c.HydrationTask(query.Symbol, query.date)

					// Update status -> HYDRATING
					c.setStatusWithLock(query.Symbol, query.date.String(), Hydrating)
				}

			case <-c.Ctx.Done():
				return
			default:
				continue
			}}
		}
	}()
	c.logger.LogStart()
}

func (c *hydrationMgr) initStatusMap() {
	state, err := c.database.GetHydrationStatus(c.Ctx)
	if err != nil {
		c.logger.Error("failed to load hydration state", "error", err)
	}
	for _, x := range state {
		c.setStatusWithLock(x.Symbol, x.Date, Ready)
	}
}

func (c *hydrationMgr) Shutdown() {
	c.logger.LogStopChild(c.fetcher.GetID())
	c.fetcher.Shutdown()

	c.CancelCtx()
	c.wg.Wait()
	c.logger.LogShutdown()
}

// HydrateSymbol fetches and stores the data series for a given symbol-date combination
func (c *hydrationMgr) HydrateSymbol(ctx context.Context, symbol string, date civil.Date) (err error) {
	dateStr := date.String()

	err = func() error {
		c.statusMapMu.Lock()
		defer c.statusMapMu.Unlock()
		state := c.getStateWOLock(symbol, dateStr)
		switch state {
		case Ready:
			return AlreadyHydratedErr
		case Hydrating:
			return AlreadyHydratingErr
		}
		c.statusMap[dateStr][symbol] = Hydrating
		return nil
	}()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			c.setStatusWithLock(symbol, dateStr, Failed)
		}
	}()

	bars, err := c.fetcher.FetchTradingSession(c.Ctx, date, symbol)

	if err != nil {
		return fmt.Errorf("symbol hydration failed for `%s-%s`: %w", symbol, date, err)
	}

	err = c.database.InsertFullSession(ctx, bars, symbol, date.String())
	if err != nil {
		return fmt.Errorf("failed to store series for `%s-%s`: %w", symbol, date, err)
	}

	c.setStatusWithLock(symbol, dateStr, Ready)
	return
}

func (c *hydrationMgr) HydrationTask(symbol string, date civil.Date) {
	defer c.wg.Done()

	ctx := context.WithoutCancel(c.Ctx)

	bars, err := c.fetcher.FetchTradingSession(c.Ctx, date, symbol)

	if err != nil {
		c.logger.Info(
			"hydration task failed",
			"symbol", symbol,
			"date", date.String(),
			"error", err,
		)

		c.setStatusWithLock(symbol, date.String(), Failed)

		// Signal back to the Hub that the task failed
		c.outbox <- HydrationFailure{symbol, date}
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
	c.setStatusWithLock(symbol, date.String(), Ready)

	// Signal hydration success
	c.outbox <- HydrationSuccess{symbol, date}
}

func (c *hydrationMgr) getStateWithLock(symbol string, date string) hydrationStatus {
	c.statusMapMu.Lock()
	defer c.statusMapMu.Unlock()
	if _, ok := c.statusMap[date]; !ok {
		c.statusMap[date] = make(map[string]hydrationStatus)
	}
	state, ok := c.statusMap[date][symbol]
	if !ok {
		return None
	}
	return state
}

func (c *hydrationMgr) getStateWOLock(symbol string, date string) hydrationStatus {
	if _, ok := c.statusMap[date]; !ok {
		c.statusMap[date] = make(map[string]hydrationStatus)
	}
	state, ok := c.statusMap[date][symbol]
	if !ok {
		return None
	}
	return state
}

func (c *hydrationMgr) setStatusWithLock(ticker string, date string, status hydrationStatus) {
	c.statusMapMu.Lock()
	defer c.statusMapMu.Unlock()
	if _, ok := c.statusMap[date]; !ok {
		c.statusMap[date] = make(map[string]hydrationStatus)
	}
	c.statusMap[date][ticker] = status
}

func (c *hydrationMgr) IsReady(t string, d civil.Date) bool {
	state := c.getStateWithLock(t, d.String())

	if state == Ready {
		return true
	}

	c.dataQueryChan <- dataQuery{t, d}

	return false
}
