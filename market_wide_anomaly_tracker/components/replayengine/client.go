package replayengine

import (
	"context"
	"fmt"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type Client interface {
	LifeCycle
	Outbox() <-chan any
}

type client struct {
	Connection *websocket.Conn

	HubClientInterface

	outbox chan any

	context       context.Context
	cancelContext context.CancelFunc

	wg sync.WaitGroup
}

func NewClient(c *websocket.Conn, hub Hub) *client {
	ctx, cancel := context.WithCancel(context.Background())

	return &client{
		Connection:          c,
		HubClientInterface:  hub,
		outbox:              make(chan any, 1024),
		context:             ctx,
		cancelContext:       cancel,
		wg:                  sync.WaitGroup{},
	}
}

func (c *client) Shutdown() {
	c.cancelContext()
	c.wg.Wait()
}

func (c *client) Start() {
	c.wg.Add(3)
	go c.ListenerThread()
	go c.SenderThread()

	// This will ensure that the client is always unregistered from the Hub
	// when the client disconnects
	go func(c *client) {
		defer c.wg.Done()
		<-c.context.Done()
		c.HubClientInterface.UnregisterClient(c) // Unregister self
	}(c)
}

func (c *client) ListenerThread() {
	defer c.wg.Done()
	for {
		var v Message
		err := wsjson.Read(c.context, c.Connection, &v)
		if err != nil {
			fmt.Println("Reader error/disconnect:", err)
			c.Shutdown()
			return
		}

		fmt.Printf("Received: %#v\n", v)

		switch v.Action {
		case "subscribe":
			c.HubClientInterface.RequestSub(c, toTypedTicker(v.Tickers))

		case "unsubscribe":
			c.HubClientInterface.RequestUnsub(c, toTypedTicker(v.Tickers))

		default:
			fmt.Println("Unrecognised `action` field : ", v.Action)
		}
	}
}

func toTypedTicker(arr []string) []Ticker{
	typedTickers := make([]Ticker, len(arr))
	for i, ticker := range arr {
		typedTickers[i] = Ticker(ticker)
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