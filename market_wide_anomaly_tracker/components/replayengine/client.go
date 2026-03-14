package replayengine

import (
	"context"
	"fmt"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type Client struct {
	Ctx        context.Context
	CancelCtx  context.CancelFunc
	Connection *websocket.Conn

	// TODO: Remove and replace with needed pipes
	Hub      *hub
	Outbox   chan any      // For sending
	Shutdown chan struct{} // Triggered by Hub when shutting down the server

	// For sending (un)subscription requests to the Hub
	subscribe   chan<- SubscriptionRequest
	unsubscribe chan<- SubscriptionRequest
}

func (c *Client) StartClient() {
	// Send the Hub a reference of itself to register
	c.Hub.register <- c

	// Unregister the client when the connection closes
	defer c.UnregisterFromHub()

	// Start go routine to listen for shutdown (shutdown on context closure)
	go c.ListenForShutdown()

	// Start SENDER go routine
	go c.SenderThread()

	// Start LISTENER go routine
	go c.ListenerThread()

	<-c.Ctx.Done()
}

// Exits on either:
//   - Cancellation of ctx
func (c *Client) ListenerThread() {
	for {
		var v Message
		err := wsjson.Read(c.Ctx, c.Connection, &v)
		if err != nil {
			fmt.Println("Reader error/disconnect:", err)
			c.CancelCtx()
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

// Exits on either:
//   - Cancellation of ctx
func (c *Client) SenderThread() {
	for {
		select {
		case <-c.Ctx.Done():
			return
		case msg := <-c.Outbox:
			err := wsjson.Write(c.Ctx, c.Connection, msg)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

// Exits on either:
//   - Cancellation of ctx from client closing connection
//   - Shutdown signal sent from parent Hub -> triggering ctx cancellation
func (c *Client) ListenForShutdown() {
	select {
	case <-c.Shutdown:
		// The hub has signalled for the client connection to be shutdown
		c.CancelCtx()

	case <-c.Ctx.Done():
		// The user disconnected normally
		return
	}
}

func (c *Client) HandleSub(tickers []string) {
	c.subscribe <- *c.createSubRequest(tickers)
}

func (c *Client) HandleUnsub(tickers []string) {
	c.unsubscribe <- *c.createSubRequest(tickers)
}

func (c *Client) UnregisterFromHub() {
	select {
	// If Hub is listening send reference of self to unregister
	case c.Hub.unregister <- c:
	case <-c.Shutdown:
	}
}

func (c *Client) createSubRequest(tickers []string) *SubscriptionRequest {
	typedTickers := make([]Ticker, 0, len(tickers))
	for _, t := range tickers {
		typedTickers = append(typedTickers, Ticker(t))
	}
	return &SubscriptionRequest{
		Client:  c,
		Tickers: typedTickers,
	}
}
