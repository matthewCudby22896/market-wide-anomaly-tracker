package replayengine

import (
	"context"
	"sync"

	"cloud.google.com/go/civil"
)

type DataFetcher interface {
	LifeCycle
	RequestHydation(ticker Ticker, date civil.Date)
}

type HydrationRequest struct {
	Ticker Ticker
	Date   civil.Date
}

// dataFetcher implements the DataFetcher inteface
type dataFetcher struct {
	Ctx       context.Context
	CancelCtx context.CancelFunc
	wg        sync.WaitGroup

	reqHydrationChan chan HydrationRequest

	logger ComponentLogger
}

func NewDataFetcher() *dataFetcher {
	return &dataFetcher{}
}

func (f *dataFetcher) Start() {
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			select {
			case <-f.Ctx.Done():
				return

			// TODO: Handling of hydration requests
			case _ = <-f.reqHydrationChan:

				continue
			}
		}
	}()
	f.logger.Info("Started.")
}

func (f *dataFetcher) Shutdown() {
	f.CancelCtx()
	f.wg.Wait()
	f.logger.Info("Shutdown.")
}

func (f *dataFetcher) RequestHydation(ticker Ticker, date civil.Date) {
	f.reqHydrationChan <- HydrationRequest{ticker, date}
}
