package replayengine

import (
	"context"
	"fmt"
	"os"
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

	ClientSignallingAPI 
	outbox   chan any 

	context context.Context
	cancelContext context.CancelFunc

	wg sync.WaitGroup
}

func NewClient(c *websocket.Conn, hub Hub) *client{
	ctx, cancel := context.WithCancel(context.Background())
	
	return &client{
		Connection: c,	
		ClientSignallingAPI: hub,
		Outbox: make(chan any, 1024),	
		context: ctx,
		cancelContext: cancel,
		wg: sync.WaitGroup{},
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
	go func() {
		defer c.wg.Done()
		<-c.context.Done()
		// TODO: Unregister itself from the Hub
		return	
	}()
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
			c.HandleSub(v.Tickers)

		case "unsubscribe":
			c.HandleUnsub(v.Tickers)

		default:
			fmt.Println("Unrecognised `action` field : ", v.Action)
		}
	}
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

func (c *client) HandleSub(tickers []string) {
	c.ClientSignallingAPI.SubscribePipe() <- *c.createSubRequest(tickers)
}

func (c *client) HandleUnsub(tickers []string) {
	c.ClientSignallingAPI.UnsubPipe() <- *c.createSubRequest(tickers)
}

func (c *client) createSubRequest(tickers []string) *SubscriptionRequest {
	typedTickers := make([]Ticker, 0, len(tickers))
	for _, t := range tickers {
		typedTickers = append(typedTickers, Ticker(t))
	}
	return &SubscriptionRequest{
		Client:  c,
		Tickers: typedTickers,
	}
}
