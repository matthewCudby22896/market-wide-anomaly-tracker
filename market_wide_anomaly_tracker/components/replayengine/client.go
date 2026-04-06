package replayengine

import (
	"context"
	"fmt"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"
)

var clientID int = 0

type Client interface {
	LifeCycle
	Outbox() <-chan any
}

type client struct {
	Connection *websocket.Conn

	outbox chan any

	context       context.Context
	cancelContext context.CancelFunc

	wg sync.WaitGroup

	logger ComponentLogger

	// For sending unsub, sub, unregister requests to hub
	// Set upon registration of the client to the hub
	hubRequestOutbox chan<- ClientRequest
}

func NewClient(c *websocket.Conn, hub Hub) *client {
	defer func() {
		clientID += 1
	}()

	ctx, cancel := context.WithCancel(context.Background())

	return &client{
		Connection:       c,
		outbox:           make(chan any, 1024),
		context:          ctx,
		cancelContext:    cancel,
		wg:               sync.WaitGroup{},
		logger:           NewLogger(fmt.Sprintf("Client %d", clientID)),
		hubRequestOutbox: nil, // Set by hub upon registration
	}

}

func (c *client) Shutdown() {
	c.cancelContext()
	c.wg.Wait()
	c.logger.LogShutdown()
}

func (c *client) Start() {
	c.wg.Add(1)
	go c.ListenerThread()
	c.wg.Add(1)
	go c.SenderThread()

	// This will ensure that the client is always unregistered from the Hub
	// when the client disconnects
	c.wg.Add(1)
	go func(c *client) {
		defer c.wg.Done()
		<-c.context.Done()
		c.logger.Info("requesting deregistration")
		c.hubRequestOutbox <- unregisterRequest{BaseRequest{c}}
	}(c)
	c.logger.Info("started.")
}

func (c *client) ListenerThread() {
	defer c.wg.Done()
	for {
		var v Message
		err := wsjson.Read(c.context, c.Connection, &v)

		if err != nil {
			status := websocket.CloseStatus(err)
			if status == -1 {
				c.logger.Info("client gracefully disconnected")
			} else {
				c.logger.Errorf("client read err: %v", err)
			}
			go c.Shutdown()
			return
		}

		switch v.Action {
		case "subscribe":
			c.hubRequestOutbox <- subRequest{BaseRequest{c}, toTypedTicker(v.Symbols)}
		case "unsubscribe":
			c.hubRequestOutbox <- unsubRequest{BaseRequest{c}, toTypedTicker(v.Symbols)}
		default:
			c.logger.Info("unrecognised `action` field : ", v.Action)
		}
	}
}

func toTypedTicker(arr []string) []common.Symbol {
	typedTickers := make([]common.Symbol, len(arr))
	for i, ticker := range arr {
		typedTickers[i] = common.Symbol(ticker)
	}
	return typedTickers
}

func (c *client) SenderThread() {
	defer c.wg.Done()
	for {
		select {
		case <-c.context.Done():
			return
		case msg := <-c.outbox:
			err := wsjson.Write(c.context, c.Connection, msg)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func (c *client) Outbox() chan<- any {
	return c.outbox
}
