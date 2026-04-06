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
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"
)

/*
	Requirements

	Must stay under 100 requests per second
*/

type MassiveClient interface {
	LifeCycle
	FetchDayData(day civil.Date, ticker common.Symbol)
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
	c.logger.LogShutdown()
}

func (c *massiveClient) FetchDayData(ctx context.Context, day civil.Date, symbol common.Symbol) ([]common.Bar, error) {
	params := &gen.GetStocksAggregatesParams{
		Adjusted: rest.Ptr(true),
		Sort:     "asc",
		Limit:    rest.Ptr(50000),
	}

	open := strconv.FormatInt(common.GetMarketOpenUnixMilli(day), 10)
	close := strconv.FormatInt(common.GetMarketCloseUnixMilli(day), 10)
	resp, err := c.client.GetStocksAggregatesWithResponse(
		ctx,
		string(symbol),
		1,
		gen.Second,
		open,
		close,
		params,
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := rest.CheckResponse(resp); err != nil {
		log.Fatal(err)
	}

	aggregateData := make([]common.Bar, 0, 4680)
	iter := rest.NewIteratorFromResponse(c.client, resp)

	// TODO: Remove
	i := 0
	for iter.Next() {
		item := iter.Item()

		t, _ := item["t"].(float64)
		o, _ := item["o"].(float64)
		h, _ := item["h"].(float64)
		l, _ := item["l"].(float64)
		c, _ := item["c"].(float64)
		n, _ := item["n"].(float64)
		v, _ := item["v"].(float64)
		vw, _ := item["vw"].(float64)

		bar := common.Bar{
			Symbol: string(symbol),
			T:      int64(t), // TODO: Check this is okay
			O:      o,
			H:      h,
			L:      l,
			C:      c,
			N:      int64(n),
			V:      v,
			VW:     vw,
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
