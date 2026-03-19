package replayengine

import (
	"context"
	"sync"

	"cloud.google.com/go/civil"
	"golang.org/x/text/date"
)

type DataAvailability int

const (
	READY DataAvailability = iota
	HYDRATING
	NONE
)

type dataQuery struct {
	ticker Ticker
	date   civil.Date
}

type stateNotification struct {
}

type DataAvailabilityProvider interface {
	IsReady(t Ticker, d civil.Date) bool
}

// Functionality exposed to the DataFetcher
type DataStateStatusConsumer interface {
	// For the DataFetcher to inform the DataCoordinater
	SignalDataReady(ticker, date civil.Date)
}

type DataCoordinater interface {
	LifeCycle
	DataAvailabilityProvider
}

// dataCoordinater implements the DataCoordinater interface
type dataCoordinater struct {
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup

	statusMapMu sync.Mutex
	statusMap   map[string]map[Ticker]DataAvailability

	logger        ComponentLogger
	db            Database
	massiveClient *massiveClient

	dataQueryChan chan dataQuery

	DataAvailabilityConsumer
}

// TODO:
func NewDataCoordinater(dataEventChan) *dataCoordinater {
	ctx, cancel := context.WithCancel(context.Background())
	return &dataCoordinater{
		Ctx:       ctx,
		CancelCtx: cancel,
		wg:        sync.WaitGroup{},

		statusMapMu: sync.Mutex{},
		statusMap:   make(map[string]map[Ticker]DataAvailability),

		// TODO: db
		massiveClient: NewMassiveClient(),

		logger:        NewLogger("DataCoordinator"),
		dataQueryChan: make(chan dataQuery, 1024),

		// initialised post-hoc:
		DataAvailabilityConsumer: nil, 
	}
}

func (c *dataCoordinater) Start() {
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

func (c *dataCoordinater) Shutdown() {
	c.massiveClient.Shutdown()

	c.CancelCtx()
	c.wg.Wait()
	c.logger.Info("shutdown.")
}

func (c *dataCoordinater) HydrationTask(date civil.Date, ticker Ticker) {
	defer c.wg.Done()

	aggregateData, err := c.massiveClient.FetchDayData(c.Ctx, date, ticker)

	if err != nil {
		c.logger.Info("hydration task failed for %s", ticker)

		// Signal back to the Hub that the task failed
	}

	// TODO: Store the data in the Time Series DB

	// Update status
	c.updateStatus(ticker, date.String(), READY)

	// Signal to Hub that data is Ready
}

func (c *dataCoordinater) getState(ticker Ticker, date string) DataAvailability {
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

func (c *dataCoordinater) updateStatus(ticker Ticker, date string, status DataAvailability) {
	c.statusMapMu.Lock()
	defer c.statusMapMu.Unlock()
	if _, ok := c.statusMap[date]; !ok {
		c.statusMap[date] = make(map[Ticker]DataAvailability)
	}
	c.statusMap[date][ticker] = status
}

// DataAvailabilityProvider interface implementation
func (c *dataCoordinater) IsReady(t Ticker, d civil.Date) bool {
	state := c.getState(t, d.String())

	if state == READY {
		return true
	}

	c.dataQueryChan <- dataQuery{t, d}

	return false
}

// DataStateStatusConsumer
func (c *dataCoordinater) SignalDataReady(ticker, date civil.Date) {

}
