package replayengine

import (
	"context"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/massive-com/client-go/v3/rest"
)

type DataAvailability int

const (
	READY DataAvailability = iota
	HYDRATING
)

type dataQuery struct {
	ticker  Ticker
	date civil.Date
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
	DataStateStatusConsumer
}

// dataCoordinater implements the DataCoordinater interface
type dataCoordinater struct {
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup

	statusMapMu sync.Mutex
	statusMap   map[string]map[Ticker]DataAvailability

	logger ComponentLogger
	db     Database
	massiveClient *massiveClient

	dataQueryChan chan dataQuery
}

// TODO:
func NewDataCoordinater() *dataCoordinater {
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
	}
}

func (c *dataCoordinater) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case query := <-c.dataQueryChan:
				c.statusMapMu.Lock()
				dateStr := query.date.String()
				state := c.statusMap[dateStr][query.ticker]
				if !(state == READY || state == HYDRATING) {
					c.wg.Add(1)
					go c.HydrationTask(query.date, query.ticker)

					c.statusMap[dateStr][query.ticker] = HYDRATING
				}
				c.statusMapMu.Unlock()

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

	c.massiveClient.FetchDayData(c.Ctx, date, ticker)
}

// DataAvailabilityProvider interface implementation
func (c *dataCoordinater) IsReady(t Ticker, d civil.Date) bool {
	dateStr := d.String()
	if _, ok := c.statusMap[dateStr]; !ok {
		c.statusMap[dateStr] = make(map[Ticker]DataAvailability)
	}

	// If READY immediately return
	if c.statusMap[dateStr][t] == READY {
		return true
	}

	// Otherwise 
	c.dataQueryChan <- dataQuery{t, d}

	return false
}

// DataStateStatusConsumer
func (c *dataCoordinater) SignalDataReady(ticker, date civil.Date) {

}
