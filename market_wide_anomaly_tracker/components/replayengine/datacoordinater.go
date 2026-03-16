package replayengine

import (
	"context"
	"sync"

	"cloud.google.com/go/civil"
)

/*
PLAN: Data Coordinater

Purpose:
- Hub needs to be able ask if data for a certain ticker for a certain day is ready [METHOD], a tickers state
will be maintained in a map[Ticker]AvailabilityStatus
  - IF READY: return True
  - IF NOT READY: update status to HYDRATING and send a command [PIPE] to the Fetcher via
  -	IF not read: return False and issue a command [PIPE] to the DataFetcher to fetch & save the data from MASSIVE
    - WHEN DataFetcher sends a `data_ready` event back, forward it to the Hub s.t. the Hub can then start the
	associated `TickerThread`
*/

type DataAvailability int

const (
	READY DataAvailability = iota
	HYDRATING
)

type dataQuery struct {
	ticker  Ticker
	dateStr string
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

	statusMapMu sync.Mutex
	statusMap   map[string]map[Ticker]DataAvailability

	logger ComponentLogger
	db     Database

	dataQueryChan chan dataQuery
}

// TODO:
func NewDataCoordinater() *dataCoordinater {
	return &dataCoordinater{
		logger: NewLogger("DataCoordinator"),
	}
}

func (c *dataCoordinater) Start() {
	go func() {
		for {
			select {
			case query := <-c.dataQueryChan:
				c.statusMapMu.Lock()
				state := c.statusMap[query.dateStr][query.ticker]
				if !(state == READY || state == HYDRATING) {
					go c.HydrationTask(query.dateStr, query.ticker)
					c.statusMap[query.dateStr][query.ticker] = HYDRATING
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
	c.logger.Info("shutdown.")
}

func (c *dataCoordinater) HydrationTask(dateStr string, ticker Ticker) {

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

	// IF not ready, do something that triggers:

	// query the DB to see if we already have the required data

	// IF yes, then signal that the data is ready

	// IF no, spawn a data fetcher thread to go and fetch & store the data from the external API

}

// DataStateStatusConsumer
func (c *dataCoordinater) SignalDataReady(ticker, date civil.Date) {

}
