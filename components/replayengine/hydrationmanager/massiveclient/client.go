package massiveclient

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"github.com/massive-com/client-go/v3/rest"
	"github.com/massive-com/client-go/v3/rest/gen"
	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/logging"
)

const clientID = "massive-client"

type client struct {
	ID               string
	Ctx              context.Context
	CancelCtx        context.CancelFunc
	wg               sync.WaitGroup
	RequestTokenChan chan struct{}
	logger           *logging.Logger
	client           *rest.Client
}

func NewMassiveClient() *client {
	ctx, cancel := context.WithCancel(context.Background())

	ApiKey := os.Getenv("MASSIVE_API_KEY")

	if ApiKey == "" {
		log.Fatal("MASSIVE_API_KEY environment variable not set")
	}

	restClient := rest.NewWithOptions(
		ApiKey,
		rest.WithTrace(false),
		rest.WithPagination(true),
	)

	return &client{
		ID:               clientID,
		Ctx:              ctx,
		CancelCtx:        cancel,
		wg:               sync.WaitGroup{},
		RequestTokenChan: make(chan struct{}, 5),
		logger:           logging.NewComponentLogger(clientID),
		client:           restClient,
	}
}

func (c *client) GetID() string {
	return c.ID
}

func (c *client) Start() {
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
	c.logger.LogStart()
}

func (c *client) Shutdown() {
	c.CancelCtx()
	c.wg.Wait()
	c.logger.LogShutdown()
}

func (c *client) FetchTradingSession(ctx context.Context, day civil.Date, symbol string) ([]common.Bar, error) {
	params := &gen.GetStocksAggregatesParams{
		Adjusted: rest.Ptr(true),
		Sort:     "asc",
		Limit:    rest.Ptr(50000),
	}

	open := common.NYSEOpenUnixMilli(day)
	close := common.NYSECloseUnixMilli(day)
	resp, err := c.client.GetStocksAggregatesWithResponse(
		ctx,
		string(symbol),
		1,
		gen.Second,
		strconv.FormatInt(open, 10),
		strconv.FormatInt(close, 10),
		params,
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := rest.CheckResponse(resp); err != nil {
		log.Fatal(err)
	}

	aggregateData := make([]common.Bar, 0, 23400)
	iter := rest.NewIteratorFromResponse(c.client, resp)

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

		// enforce the time range [open, close)
		tInt64 := int64(t)
		nInt64 := int64(n)

		if tInt64 < open {
			break
		}
		if tInt64 >= close {
			break
		}

		bar := common.Bar{
			Symbol: symbol,
			T:      tInt64,
			O:      o,
			H:      h,
			L:      l,
			C:      c,
			N:      nInt64,
			V:      v,
			VW:     vw,
		}
		aggregateData = append(aggregateData, bar)
		i++
	}

	return aggregateData, nil
}
