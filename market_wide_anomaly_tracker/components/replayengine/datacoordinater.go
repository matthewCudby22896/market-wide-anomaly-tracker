package replayengine

import "cloud.google.com/go/civil"

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

type DataAvailabilityProvider interface {
	IsReady(t Ticker, d civil.Date) DataAvailability
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
	shutdownChan chan struct{}

	statusMap map[string]map[Ticker]DataAvailability

	// Internal ref to the dataFetcher
	dataFetcher DataFetcher
}

// INIT METHOD
func NewDataCoordinater() *dataCoordinater {
	// TODO
	return &dataCoordinater{}
}

// CORE LOOP
func (c *dataCoordinater) Start() {
	for {
		select {
		default:
			continue
		}
	}
}

// SHUTDOWN
func (c *dataCoordinater) Shutdown() {
	// TODO
}

// DataAvailabilityProvider interface implementation
func (c *dataCoordinater) IsReady(t Ticker, d civil.Date) DataAvailability {

	//statusMap map[string]map[Ticker]DataAvailability
	dateStr := d.String()
	var dayMap map[Ticker]DataAvailability
	var ok bool
	if dayMap, ok = c.statusMap[dateStr]; !ok {
		c.statusMap[dateStr] = make(map[Ticker]DataAvailability)
		dayMap = c.statusMap[dateStr]
	}

	if dayMap[t] == READY {
		return READY
	}
	c.dataFetcher.RequestHydation(t, d)

	dayMap[t] = HYDRATING

}

// DataStateStatusConsumer
func (c *dataCoordinater) SignalDataReady(ticker, date civil.Date) {

}
