package replayengine

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
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
				// Non-blocking send
				select {
				case c.RequestTokenChan <- struct{}{}:
				default:
				}
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

type ohlcBar struct {
	vw float64 // volume weighted average price
	c  float64 // close price
	h  float64 // highest price
	l  float64 // lowest price
	n  float64 // no. transactions
	o  float64 // open
	t  float64 // timestamp
	v  float64 // volume
}

func (c *massiveClient) FetchDayData(ctx context.Context, day civil.Date, ticker Ticker) ([]ohlcBar, error) {
	params := &gen.GetStocksAggregatesParams{
		Adjusted: rest.Ptr(true),
		Sort:     "asc",
		Limit:    rest.Ptr(50000),
	}

	resp, err := c.client.GetStocksAggregatesWithResponse(
		ctx,
		string(ticker),
		5,
		gen.Second,
		c.getMarketOpenUnixMilli(day),
		c.getMarketCloseUnixMilli(day),
		params,
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := rest.CheckResponse(resp); err != nil {
		log.Fatal(err)
	}

	aggregateData := make([]ohlcBar, 0, 4680)
	iter := rest.NewIteratorFromResponse(c.client, resp)

	// TODO: Remove
	i := 0
	for iter.Next() {
		item := iter.Item()

		vw, _ := item["vw"].(float64)
		c, _ := item["c"].(float64)
		h, _ := item["h"].(float64)
		l, _ := item["l"].(float64)
		o, _ := item["o"].(float64)
		t, _ := item["t"].(float64)
		v, _ := item["v"].(float64)

		bar := ohlcBar{
			vw: vw,
			c:  c,
			h:  h,
			l:  l,
			o:  o,
			t:  t,
			v:  v,
		}
		aggregateData = append(aggregateData, bar)
		i++
		// TODO: Remove
		fmt.Printf("[%d] %#v \n", i, bar)
	}
	// TODO: Remove
	fmt.Printf("len arr: %d\n", len(aggregateData))

	return aggregateData, nil
}

func (c *massiveClient) getMarketOpenUnixMilli(day civil.Date) string {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatal(err)
	}
	t := time.Date(
		day.Year,
		day.Month,
		day.Day,
		9, 30, 0, 0, // 9:30:00.000000
		location,
	).UnixMicro()
	return strconv.Itoa(int(t))
}

func (c *massiveClient) getMarketCloseUnixMilli(day civil.Date) string {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatal(err)
	}
	t := time.Date(
		day.Year,
		day.Month,
		day.Day,
		16, 0, 0, 0,
		location,
	).UnixMicro()
	return strconv.Itoa(int(t))
}
