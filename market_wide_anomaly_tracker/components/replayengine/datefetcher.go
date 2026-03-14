package replayengine

import "cloud.google.com/go/civil"

type DataFetcher interface {
	RequestHydation(ticker Ticker, date civil.Date)
}

type HydrationRequest struct {
	Ticker Ticker
	Date   civil.Date
}

// dataFetcher implements the DataFetcher inteface
type dataFetcher struct {
	shutdownChan         chan struct{}
	reqHydrationChan chan HydrationRequest
}

// INIT METHOD
func NewDataFetcher() *dataFetcher {
	return &dataFetcher{}
}

// CORE LOOP
func (f *dataFetcher) Run() {
	for {
		select {
		case <-f.shutdownChan:
			f.shutdown()
			return

		// TODO: Handling of hydration requests
		case _ = <-f.reqHydrationChan:

			continue
		}
	}
}

// SHUTDOWN METHOD
func (f *dataFetcher) shutdown() {
	// TODO: Implement shutdown of all child processes (TBD)
}

func (f *dataFetcher) RequestHydation(ticker Ticker, date civil.Date) {
	f.reqHydrationChan <- HydrationRequest{ticker, date}
}


