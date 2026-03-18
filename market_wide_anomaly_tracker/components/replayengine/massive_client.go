package replayengine

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"github.com/massive-com/client-go/v3/rest"
	"github.com/massive-com/client-go/v3/rest/gen"
)

/*
	Requirements

	Must stay under 100 requests per second
*/

type MassiveClient interface {
	LifeCycle
	FetchDayData(day civil.Date, ticker Ticker)
}

type massiveClient struct {
	Ctx              context.Context
	CancelCtx        context.CancelFunc
	wg               sync.WaitGroup
	RequestTokenChan chan struct{}
	logger           ComponentLogger
	client           *rest.Client
}

func NewMassiveClient() *massiveClient {
	ctx, cancel := context.WithCancel(context.Background())

	API_KEY := os.Getenv("MASSIVE_API_KEY")

	client := rest.NewWithOptions(
		API_KEY,
		rest.WithTrace(false),
		rest.WithPagination(true),
	)

	return &massiveClient{
		Ctx:              ctx,
		CancelCtx:        cancel,
		wg:               sync.WaitGroup{},
		RequestTokenChan: make(chan struct{}, 5),
		logger:           NewLogger("MassiveClient"),
		client:           client,
	}
}

func (c *massiveClient) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker((1000 / 60) * time.Millisecond)

		for {
			select {
			case <-c.Ctx.Done():
				return
			case <-ticker.C:
				c.RequestTokenChan <- struct{}{}
			}
		}
	}()
	c.logger.Info("started.")
}

func (c *massiveClient) Shutdown() {
	c.CancelCtx()
	c.wg.Wait()
	c.logger.Info("shutdown.")
}

// Intended to be called by a DataCoordinator Job go thread
func (c *massiveClient) FetchDayData(ctx context.Context, day civil.Date, ticker Ticker) {

	params := &gen.GetStocksAggregatesParams{
		Adjusted: rest.Ptr(true),
		Sort:     "asc",
		Limit:    rest.Ptr(10000),
	}

	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatal(err)
	}

	marketOpenTime := time.Date(
		day.Year,
		day.Month,
		day.Day,
		9, 30, 0, 0, // 9:30:00.000000
		location,
	)

	marketCloseTime := time.Date(
		day.Year,
		day.Month,
		day.Day,
		16, 0, 0, 0,
		location,
	)

	marketOpenTS := marketOpenTime.Format("2006-01-02Y15:04:05.000Z07:00")
	marketCloseTS := marketCloseTime.Format("2006-01-02Y15:04:05.000Z07:00")

	tickerStr := string(ticker)

	resp, err := c.client.GetStocksAggregatesWithResponse(
		ctx,
		tickerStr,
		5, gen.Second,
		marketOpenTS,
		marketCloseTS,
		params,
	)

	if err != nil {
		log.Fatal(err)
	}

	if err := rest.CheckResponse(resp); err != nil {
		log.Fatal(err)
	}
	iter := rest.NewIteratorFromResponse(c, resp)
	for iter.Next() {
		item := iter.Item()
		fmt.Printf("%+v\n", item)
	}
}
